//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestGatewayCacheOpenAIAccountRuntimeBlock_SetListDelete(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	cache, ok := NewGatewayCache(rdb).(service.OpenAIAccountRuntimeBlockCache)
	require.True(t, ok)
	ctx := context.Background()
	accountID := int64(123)
	until := time.Now().Add(5 * time.Minute).Truncate(time.Millisecond)

	require.NoError(t, cache.SetOpenAIAccountRuntimeBlock(ctx, accountID, until, 5*time.Minute))
	blocks, err := cache.ListOpenAIAccountRuntimeBlocks(ctx)
	require.NoError(t, err)
	require.WithinDuration(t, until, blocks[accountID], time.Second)

	require.NoError(t, cache.DeleteOpenAIAccountRuntimeBlock(ctx, accountID))
	blocks, err = cache.ListOpenAIAccountRuntimeBlocks(ctx)
	require.NoError(t, err)
	require.NotContains(t, blocks, accountID)
}
