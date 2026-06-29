package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

// prewarmSessionCache 是 OpenAIPrewarmSessionCache 的 Redis 实现。
// service 层通过 OpenAIPrewarmSessionCache 接口解耦，本结构在 repository 层
// 持有 *redis.Client，与 gatewayCache / LeaderLockCache 保持一致的分层。
type prewarmSessionCache struct {
	rdb *redis.Client
}

// NewOpenAIPrewarmSessionCache 创建 OpenAI prewarm session 的 Redis 缓存实现。
func NewOpenAIPrewarmSessionCache(rdb *redis.Client) service.OpenAIPrewarmSessionCache {
	if rdb == nil {
		return nil
	}
	return &prewarmSessionCache{rdb: rdb}
}

func (c *prewarmSessionCache) SetPrewarmSession(ctx context.Context, key, value string, ttl time.Duration) error {
	if key == "" || value == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

func (c *prewarmSessionCache) GetPrewarmSession(ctx context.Context, key string) (string, time.Duration, error) {
	if key == "" {
		return "", 0, nil
	}
	// Pipeline 同时取 value 与剩余 TTL，单次往返。
	pipe := c.rdb.Pipeline()
	getCmd := pipe.Get(ctx, key)
	ttlCmd := pipe.TTL(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return "", 0, err
	}
	value, err := getCmd.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", 0, nil
		}
		return "", 0, err
	}
	ttl, err := ttlCmd.Result()
	if err != nil {
		ttl = 0
	}
	return value, ttl, nil
}

func (c *prewarmSessionCache) DeletePrewarmSession(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	return c.rdb.Del(ctx, key).Err()
}
