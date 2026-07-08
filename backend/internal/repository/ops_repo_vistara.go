package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// quotaPerUSD follows new-api's default conversion: 1 USD = 500000 quota.
const quotaPerUSD = 500000.0

func (r *opsRepository) InsertVistaraQuotaSample(ctx context.Context, usedQuota int64) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("nil ops repository")
	}
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO ops_vistara_quota_samples (created_at, used_quota) VALUES (NOW(), $1)`,
		usedQuota,
	)
	return err
}

func (r *opsRepository) GetVistaraCostSummary(ctx context.Context, now time.Time) (*service.OpsVistaraCostSummary, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("nil ops repository")
	}

	out := &service.OpsVistaraCostSummary{}

	var firstAt time.Time
	var firstQuota int64
	err := r.db.QueryRowContext(ctx,
		`SELECT created_at, used_quota FROM ops_vistara_quota_samples ORDER BY created_at ASC LIMIT 1`,
	).Scan(&firstAt, &firstQuota)
	haveFirst := err == nil
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT created_at, used_quota FROM ops_vistara_quota_samples ORDER BY created_at DESC LIMIT 2`,
	)
	if err != nil {
		return nil, err
	}
	type sample struct {
		at    time.Time
		quota int64
	}
	last := make([]sample, 0, 2)
	for rows.Next() {
		var s sample
		if err := rows.Scan(&s.at, &s.quota); err != nil {
			_ = rows.Close()
			return nil, err
		}
		last = append(last, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_ = rows.Close()

	if len(last) > 0 {
		total := float64(last[0].quota) / quotaPerUSD
		out.TotalUSD = &total
		if haveFirst {
			since := float64(last[0].quota-firstQuota) / quotaPerUSD
			out.SinceStartUSD = &since
			out.RecordFrom = &firstAt
		}
	}
	if len(last) == 2 {
		dq := last[0].quota - last[1].quota
		dt := last[0].at.Sub(last[1].at).Minutes()
		if dt > 0 {
			perMin := float64(dq) / quotaPerUSD / dt
			out.USDPerMinute = &perMin
		}
	}

	var mn, mx sql.NullInt64
	var mnt, mxt sql.NullTime
	err = r.db.QueryRowContext(ctx,
		`SELECT MIN(used_quota), MAX(used_quota), MIN(created_at), MAX(created_at) FROM ops_vistara_quota_samples WHERE created_at >= $1`,
		now.Add(-time.Hour),
	).Scan(&mn, &mx, &mnt, &mxt)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if mn.Valid && mx.Valid && mnt.Valid && mxt.Valid {
		spanHours := mxt.Time.Sub(mnt.Time).Hours()
		if spanHours > 0 {
			perHour := float64(mx.Int64-mn.Int64) / quotaPerUSD / spanHours
			perDay := perHour * 24
			out.USDPerHour = &perHour
			out.USDPerDay = &perDay
		}
	}

	return out, nil
}
