package repository

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestOpsRepositoryGetCodexAccountTraffic(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := &opsRepository{db: db}
	since := time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)

	mock.ExpectQuery(`(?s)WITH combined AS.*GROUP BY account_id`).
		WithArgs(since, until).
		WillReturnRows(sqlmock.NewRows([]string{"account_id", "success_count", "error_429_count", "error_other_count"}).
			AddRow(int64(64), int64(120), int64(3), int64(1)).
			AddRow(int64(70), int64(80), int64(0), int64(0)))

	out, err := repo.GetCodexAccountTraffic(context.Background(), since, until)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, int64(64), out[0].AccountID)
	require.Equal(t, int64(120), out[0].SuccessCount)
	require.Equal(t, int64(3), out[0].Error429Count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpsRepositoryGetCodexBlockedReasonBreakdown(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := &opsRepository{db: db}
	since := time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)

	mock.ExpectQuery(`(?s)SELECT error_type, COUNT\(\*\) AS cnt FROM ops_error_logs`).
		WithArgs(since, until).
		WillReturnRows(sqlmock.NewRows([]string{"error_type", "cnt"}).
			AddRow("rate_limit", int64(9)).
			AddRow("no_available_account", int64(2)))

	out, err := repo.GetCodexBlockedReasonBreakdown(context.Background(), since, until)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, "rate_limit", out[0].Reason)
	require.Equal(t, int64(9), out[0].Count)
	require.NoError(t, mock.ExpectationsWereMet())
}
