package repository

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestOpsRepositoryInsertVistaraQuotaSample(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := &opsRepository{db: db}

	mock.ExpectExec(`INSERT INTO ops_vistara_quota_samples`).
		WithArgs(int64(123456)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = repo.InsertVistaraQuotaSample(context.Background(), 123456)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpsRepositoryGetVistaraCostSummary(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := &opsRepository{db: db}
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

	// First/last-two-samples query.
	mock.ExpectQuery(`SELECT created_at, used_quota FROM ops_vistara_quota_samples ORDER BY created_at ASC LIMIT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"created_at", "used_quota"}).
			AddRow(now.Add(-3*time.Hour), int64(1_000_000)))

	mock.ExpectQuery(`SELECT created_at, used_quota FROM ops_vistara_quota_samples ORDER BY created_at DESC LIMIT 2`).
		WillReturnRows(sqlmock.NewRows([]string{"created_at", "used_quota"}).
			AddRow(now, int64(1_050_000)).
			AddRow(now.Add(-time.Minute), int64(1_049_500)))

	mock.ExpectQuery(`SELECT MIN\(used_quota\), MAX\(used_quota\), MIN\(created_at\), MAX\(created_at\) FROM ops_vistara_quota_samples WHERE created_at >= \$1`).
		WithArgs(now.Add(-time.Hour)).
		WillReturnRows(sqlmock.NewRows([]string{"mn", "mx", "mnt", "mxt"}).
			AddRow(int64(1_040_000), int64(1_050_000), now.Add(-time.Hour), now))

	summary, err := repo.GetVistaraCostSummary(context.Background(), now)
	require.NoError(t, err)
	require.NotNil(t, summary.TotalUSD)
	require.NotNil(t, summary.SinceStartUSD)
	require.NotNil(t, summary.USDPerMinute)
	require.NotNil(t, summary.USDPerHour)
	require.NoError(t, mock.ExpectationsWereMet())
}
