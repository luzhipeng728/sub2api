package service

import (
	"context"
	"database/sql"
	"hash/fnv"
	"time"

	"github.com/redis/go-redis/v9"
)

func hashAdvisoryLockID(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64())
}

func tryAcquireDBAdvisoryLock(ctx context.Context, db *sql.DB, lockID int64) (func(), bool) {
	if db == nil {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false
	}

	acquired := false
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false
	}
	if !acquired {
		_ = conn.Close()
		return nil, false
	}

	release := func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", lockID)
		_ = conn.Close()
	}
	return release, true
}

// opsLeaderLockReleaseScript is a compare-and-delete release: it only deletes
// the lock key if it still holds the owner's instanceID, so a stale release
// (after TTL expiry and re-acquisition by another instance) can't clobber
// the new owner's lock. The script is generic over KEYS[1]/ARGV[1], so it is
// shared across all ops background collectors that use this lock pattern.
var opsLeaderLockReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// tryAcquireOpsLeaderLock is the shared Redis-SetNX leader-election dance
// used by the ops background collectors (OpsMetricsCollector,
// OpsVistaraCollector, ...) to ensure only one instance polls/persists per
// tick in a multi-instance deployment. It falls back to a Postgres advisory
// lock (via db, which may be nil) when Redis is present but erroring, and
// fails open (nil, true) when no redisClient is configured at all (e.g. in
// tests), matching each collector's pre-existing single-instance behavior.
func tryAcquireOpsLeaderLock(ctx context.Context, redisClient *redis.Client, db *sql.DB, lockKey string, lockTTL time.Duration, advisoryLockID int64, instanceID string) (func(), bool) {
	if redisClient == nil {
		return nil, true
	}
	if ctx == nil {
		ctx = context.Background()
	}

	ok, err := redisClient.SetNX(ctx, lockKey, instanceID, lockTTL).Result()
	if err != nil {
		// Prefer fail-closed to avoid stampeding the database when Redis is flaky.
		// Fallback to a DB advisory lock when Redis is present but unavailable.
		return tryAcquireDBAdvisoryLock(ctx, db, advisoryLockID)
	}
	if !ok {
		return nil, false
	}

	release := func() {
		relCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = opsLeaderLockReleaseScript.Run(relCtx, redisClient, []string{lockKey}, instanceID).Result()
	}
	return release, true
}
