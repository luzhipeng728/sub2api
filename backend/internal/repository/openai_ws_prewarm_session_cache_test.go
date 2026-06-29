//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newPrewarmSessionTestCache(t *testing.T) (*prewarmSessionCache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return &prewarmSessionCache{rdb: rdb}, mr
}

func TestPrewarmSessionCache_SetGetDelete(t *testing.T) {
	cache, _ := newPrewarmSessionTestCache(t)
	ctx := context.Background()
	const key = "openai:prewarm:session:1:abc"

	// 未命中
	val, ttl, err := cache.GetPrewarmSession(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "", val)
	require.Equal(t, time.Duration(0), ttl)

	// 写入
	require.NoError(t, cache.SetPrewarmSession(ctx, key, `{"response_id":"resp_123","created_at_unix":1}`, time.Minute))
	val, ttl, err = cache.GetPrewarmSession(ctx, key)
	require.NoError(t, err)
	require.Equal(t, `{"response_id":"resp_123","created_at_unix":1}`, val)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, time.Minute)

	// 删除
	require.NoError(t, cache.DeletePrewarmSession(ctx, key))
	val, ttl, err = cache.GetPrewarmSession(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "", val)
	require.Equal(t, time.Duration(0), ttl)
}

func TestPrewarmSessionCache_EmptyKeyNoOp(t *testing.T) {
	cache, _ := newPrewarmSessionTestCache(t)
	ctx := context.Background()

	require.NoError(t, cache.SetPrewarmSession(ctx, "", "v", time.Minute))
	require.NoError(t, cache.SetPrewarmSession(ctx, "k", "", time.Minute))
	require.NoError(t, cache.DeletePrewarmSession(ctx, ""))

	val, ttl, err := cache.GetPrewarmSession(ctx, "")
	require.NoError(t, err)
	require.Equal(t, "", val)
	require.Equal(t, time.Duration(0), ttl)
}

func TestPrewarmSessionCache_DefaultTTLWhenZero(t *testing.T) {
	cache, mr := newPrewarmSessionTestCache(t)
	ctx := context.Background()
	const key = "openai:prewarm:session:2:def"

	// ttl=0 时应使用默认 1h。
	require.NoError(t, cache.SetPrewarmSession(ctx, key, "v", 0))
	ttl := mr.TTL(key)
	require.Greater(t, ttl, 30*time.Minute)
}
