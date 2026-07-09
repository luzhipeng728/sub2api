package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type fakeVistaraOpsRepo struct {
	OpsRepository
	inserted []int64
}

func (f *fakeVistaraOpsRepo) InsertVistaraQuotaSample(ctx context.Context, usedQuota int64) error {
	f.inserted = append(f.inserted, usedQuota)
	return nil
}

func newVistaraTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"items": []map[string]any{
					{"id": float64(42), "used_quota": float64(7_000_000)},
				},
			},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestOpsVistaraCollectorPollsAndInsertsQuota(t *testing.T) {
	srv := newVistaraTestServer(t)

	repo := &fakeVistaraOpsRepo{}
	cfg := &config.Config{
		Vistara: config.VistaraConfig{
			BaseURL:   srv.URL,
			APIToken:  "test-token",
			ChannelID: 42,
		},
	}

	// redisClient is nil here (no Redis wired), which must fail open so a
	// single-instance deployment (or a test) keeps collecting rather than
	// deadlocking on a lock it has no way to acquire.
	c := NewOpsVistaraCollector(repo, cfg, nil, "test-instance")
	if err := c.collectOnce(context.Background()); err != nil {
		t.Fatalf("collectOnce returned error: %v", err)
	}
	if len(repo.inserted) != 1 || repo.inserted[0] != 7_000_000 {
		t.Fatalf("inserted = %v, want [7000000]", repo.inserted)
	}
}

// TestOpsVistaraCollectorLeaderLockGatesCollection proves collectOnce is
// gated by the same Redis leader-lock pattern OpsMetricsCollector uses: a
// second instance holding no lock must skip collection entirely (no insert,
// no HTTP poll needed), and once the lock is released a peer can acquire it
// and collect normally. Without this, every instance in a multi-instance
// deployment would insert a used_quota sample every tick, corrupting the
// $/minute cost-rate calculation in GetVistaraCostSummary.
func TestOpsVistaraCollectorLeaderLockGatesCollection(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	srv := newVistaraTestServer(t)
	repo := &fakeVistaraOpsRepo{}
	cfg := &config.Config{
		Vistara: config.VistaraConfig{
			BaseURL:   srv.URL,
			APIToken:  "test-token",
			ChannelID: 42,
		},
	}

	leader := NewOpsVistaraCollector(repo, cfg, rdb, "instance-a")
	follower := NewOpsVistaraCollector(repo, cfg, rdb, "instance-b")

	release, ok := leader.tryAcquireLeaderLock(context.Background())
	require.True(t, ok, "leader should acquire the lock first")
	require.NotNil(t, release)

	require.NoError(t, follower.collectOnce(context.Background()))
	require.Empty(t, repo.inserted, "follower must skip collection while the leader holds the lock")

	release()

	require.NoError(t, follower.collectOnce(context.Background()))
	require.Equal(t, []int64{7_000_000}, repo.inserted, "follower should collect once the lock is released")
}
