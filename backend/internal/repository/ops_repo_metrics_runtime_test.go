package repository

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpsRepositoryInsertSystemMetricsIncludesRuntimeWSFields(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := &opsRepository{db: db}

	heapAlloc := int64(512)
	heapSys := int64(768)
	gcCount := 42
	wsActive := 17
	wsHandshakeTotal := int64(9001)

	mock.ExpectExec(`INSERT INTO ops_system_metrics`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = repo.InsertSystemMetrics(context.Background(), &service.OpsInsertSystemMetricsInput{
		CreatedAt:        time.Now(),
		WindowMinutes:    1,
		HeapAllocMB:      &heapAlloc,
		HeapSysMB:        &heapSys,
		GCCount:          &gcCount,
		WSActiveConns:    &wsActive,
		WSHandshakeTotal: &wsHandshakeTotal,
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpsRepositoryListSystemMetricsSince(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := &opsRepository{db: db}
	since := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)

	rows := sqlmock.NewRows([]string{
		"id", "created_at", "window_minutes",
		"memory_used_mb", "goroutine_count",
		"heap_alloc_mb", "heap_sys_mb", "gc_count",
		"ws_active_conns", "ws_handshake_total",
	}).AddRow(int64(1), since.Add(time.Minute), 1, int64(300), int64(120), int64(200), int64(280), int64(5), int64(12), int64(500))

	mock.ExpectQuery(`SELECT[\s\S]*FROM ops_system_metrics[\s\S]*WHERE created_at >= \$1`).
		WithArgs(since).
		WillReturnRows(rows)

	out, err := repo.ListSystemMetricsSince(context.Background(), since)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, int64(200), *out[0].HeapAllocMB)
	require.Equal(t, 12, *out[0].WSActiveConns)
	require.NoError(t, mock.ExpectationsWereMet())
}
