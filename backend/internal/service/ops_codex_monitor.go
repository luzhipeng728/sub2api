package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// OpsCodexOverview is the top-level summary for the Codex monitoring panel:
// total traffic in, how much succeeded, how many distinct accounts took
// traffic, RPM, and why requests were blocked.
type OpsCodexOverview struct {
	Since time.Time `json:"since"`
	Until time.Time `json:"until"`

	TotalTraffic  int64   `json:"total_traffic"`
	SuccessCount  int64   `json:"success_count"`
	Error429Count int64   `json:"error_429_count"`
	BlockedCount  int64   `json:"blocked_count"`
	AccountsUsed  int     `json:"accounts_used"`
	RPM1h         float64 `json:"rpm_1h"`

	BlockedReasons []*OpsCodexBlockedReason `json:"blocked_reasons"`
	Vistara        *OpsVistaraCostSummary   `json:"vistara"`
}

// OpsCodexAccountStatus merges per-account traffic counts with the account's
// live scheduling/quota status (concurrency, proxy, weekly/5h usage%,
// schedulable) — same fields the old codex_monitor.py script displayed.
type OpsCodexAccountStatus struct {
	AccountID     int64   `json:"account_id"`
	Name          string  `json:"name"`
	Concurrency   int     `json:"concurrency"`
	ProxyID       *int64  `json:"proxy_id"`
	Schedulable   bool    `json:"schedulable"`
	Weekly7dPct   float64 `json:"weekly_7d_pct"`
	Hourly5hPct   float64 `json:"hourly_5h_pct"`
	Weekly7dReset string  `json:"weekly_7d_reset"`

	SuccessCount    int64 `json:"success_count"`
	Error429Count   int64 `json:"error_429_count"`
	ErrorOtherCount int64 `json:"error_other_count"`
}

func (s *OpsService) GetCodexOverview(ctx context.Context, since, until time.Time) (*OpsCodexOverview, error) {
	if s == nil || s.opsRepo == nil {
		return nil, fmt.Errorf("ops service not available")
	}

	traffic, err := s.opsRepo.GetCodexAccountTraffic(ctx, since, until)
	if err != nil {
		return nil, err
	}
	blocked, err := s.opsRepo.GetCodexBlockedReasonBreakdown(ctx, since, until)
	if err != nil {
		return nil, err
	}
	vistara, err := s.opsRepo.GetVistaraCostSummary(ctx, until)
	if err != nil {
		return nil, err
	}

	out := &OpsCodexOverview{
		Since:          since,
		Until:          until,
		BlockedReasons: blocked,
		Vistara:        vistara,
	}
	for _, t := range traffic {
		out.SuccessCount += t.SuccessCount
		out.Error429Count += t.Error429Count
		out.BlockedCount += t.Error429Count + t.ErrorOtherCount
		if t.SuccessCount > 0 || t.Error429Count > 0 || t.ErrorOtherCount > 0 {
			out.AccountsUsed++
		}
	}
	out.TotalTraffic = out.SuccessCount + out.BlockedCount

	// RPM1h is the success rate extrapolated to a standard 1-hour window
	// (i.e. "how many requests would land in an hour at this rate"), not a
	// literal per-minute rate — a window shorter/longer than 1h is scaled
	// accordingly (windowMinutes=60 => RPM1h == SuccessCount).
	windowMinutes := until.Sub(since).Minutes()
	if windowMinutes > 0 {
		out.RPM1h = float64(out.SuccessCount) / windowMinutes * 60
	}

	return out, nil
}

func (s *OpsService) GetCodexAccounts(ctx context.Context, since, until time.Time) ([]*OpsCodexAccountStatus, error) {
	if s == nil || s.opsRepo == nil {
		return nil, fmt.Errorf("ops service not available")
	}
	if s.db == nil {
		return nil, fmt.Errorf("ops service has no db handle")
	}

	traffic, err := s.opsRepo.GetCodexAccountTraffic(ctx, since, until)
	if err != nil {
		return nil, err
	}
	byAccount := make(map[int64]*OpsCodexAccountTraffic, len(traffic))
	for _, t := range traffic {
		byAccount[t.AccountID] = t
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT
  id, name, concurrency, proxy_id, schedulable,
  COALESCE((extra->>'codex_7d_used_percent')::float, -1),
  COALESCE((extra->>'codex_5h_used_percent')::float, -1),
  COALESCE(extra->>'codex_7d_reset_at', '')
FROM accounts
WHERE deleted_at IS NULL AND platform = 'openai'
ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]*OpsCodexAccountStatus, 0, 32)
	for rows.Next() {
		var item OpsCodexAccountStatus
		var proxyID sql.NullInt64
		if err := rows.Scan(
			&item.AccountID, &item.Name, &item.Concurrency, &proxyID, &item.Schedulable,
			&item.Weekly7dPct, &item.Hourly5hPct, &item.Weekly7dReset,
		); err != nil {
			return nil, err
		}
		if proxyID.Valid {
			v := proxyID.Int64
			item.ProxyID = &v
		}
		if t, ok := byAccount[item.AccountID]; ok {
			item.SuccessCount = t.SuccessCount
			item.Error429Count = t.Error429Count
			item.ErrorOtherCount = t.ErrorOtherCount
		}
		out = append(out, &item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *OpsService) GetCodexRuntimeSeries(ctx context.Context, since time.Time) ([]*OpsSystemMetricsSnapshot, error) {
	if s == nil || s.opsRepo == nil {
		return nil, fmt.Errorf("ops service not available")
	}
	return s.opsRepo.ListSystemMetricsSince(ctx, since)
}
