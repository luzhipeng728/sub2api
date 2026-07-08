package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// GetCodexAccountTraffic aggregates success/429/other-error request counts
// by account_id for the OpenAI/Codex platform over [since, until). usage_logs
// has no platform column of its own, so its half of the union resolves
// platform via a join to accounts; ops_error_logs carries platform directly.
func (r *opsRepository) GetCodexAccountTraffic(ctx context.Context, since, until time.Time) ([]*service.OpsCodexAccountTraffic, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("nil ops repository")
	}

	q := `
WITH combined AS (
  SELECT
    ul.account_id AS account_id,
    'success'::TEXT AS bucket
  FROM usage_logs ul
  JOIN accounts a ON a.id = ul.account_id
  WHERE ul.created_at >= $1 AND ul.created_at < $2
    AND a.platform = 'openai'

  UNION ALL

  SELECT
    o.account_id AS account_id,
    CASE WHEN o.status_code = 429 THEN '429' ELSE 'other' END AS bucket
  FROM ops_error_logs o
  WHERE o.created_at >= $1 AND o.created_at < $2
    AND o.platform = 'openai'
    AND (COALESCE(o.status_code, 0) >= 400 OR o.error_type = 'cyber_policy')
    AND o.account_id IS NOT NULL
)
SELECT
  account_id,
  COUNT(*) FILTER (WHERE bucket = 'success') AS success_count,
  COUNT(*) FILTER (WHERE bucket = '429') AS error_429_count,
  COUNT(*) FILTER (WHERE bucket = 'other') AS error_other_count
FROM combined
WHERE account_id IS NOT NULL
GROUP BY account_id
ORDER BY success_count DESC`

	rows, err := r.db.QueryContext(ctx, q, since.UTC(), until.UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]*service.OpsCodexAccountTraffic, 0, 32)
	for rows.Next() {
		var item service.OpsCodexAccountTraffic
		if err := rows.Scan(&item.AccountID, &item.SuccessCount, &item.Error429Count, &item.ErrorOtherCount); err != nil {
			return nil, err
		}
		out = append(out, &item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// GetCodexBlockedReasonBreakdown counts ops_error_logs.error_type for the
// OpenAI/Codex platform over [since, until), scoped to actual errors
// (status_code >= 400) so it doesn't double-count anything below the error
// threshold.
func (r *opsRepository) GetCodexBlockedReasonBreakdown(ctx context.Context, since, until time.Time) ([]*service.OpsCodexBlockedReason, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("nil ops repository")
	}

	q := `
SELECT error_type, COUNT(*) AS cnt
FROM ops_error_logs
WHERE created_at >= $1 AND created_at < $2
  AND platform = 'openai'
  AND (COALESCE(status_code, 0) >= 400 OR error_type = 'cyber_policy')
GROUP BY error_type
ORDER BY cnt DESC`

	rows, err := r.db.QueryContext(ctx, q, since.UTC(), until.UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]*service.OpsCodexBlockedReason, 0, 16)
	for rows.Next() {
		var item service.OpsCodexBlockedReason
		if err := rows.Scan(&item.Reason, &item.Count); err != nil {
			return nil, err
		}
		out = append(out, &item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
