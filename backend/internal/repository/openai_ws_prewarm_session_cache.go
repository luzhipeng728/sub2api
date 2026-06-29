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

func (c *prewarmSessionCache) ClaimPrewarmSession(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", nil
	}
	// GETDEL：原子读取并删除，保证一次性消费的 prewarm id 不会被并发请求重复取到。
	value, err := c.rdb.GetDel(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return value, nil
}

func (c *prewarmSessionCache) DeletePrewarmSession(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	return c.rdb.Del(ctx, key).Err()
}

// PushPrewarmPool 把一个 prewarm id 追加到池(LIST)尾部，并裁剪到 maxDepth、刷新 TTL。
// 用 pipeline 单次往返完成 RPUSH + LTRIM + EXPIRE。
func (c *prewarmSessionCache) PushPrewarmPool(ctx context.Context, key, value string, maxDepth int, ttl time.Duration) error {
	if key == "" || value == "" {
		return nil
	}
	if maxDepth <= 0 {
		maxDepth = 1
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	pipe := c.rdb.Pipeline()
	pipe.RPush(ctx, key, value)
	// 只保留最新的 maxDepth 个，避免无界增长。
	pipe.LTrim(ctx, key, int64(-maxDepth), -1)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// PopPrewarmPool 从池(LIST)头部原子弹出一个 prewarm id；池空返回 ("", nil)。
func (c *prewarmSessionCache) PopPrewarmPool(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", nil
	}
	value, err := c.rdb.LPop(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return value, nil
}

// PrewarmPoolLen 返回池当前深度。
func (c *prewarmSessionCache) PrewarmPoolLen(ctx context.Context, key string) (int, error) {
	if key == "" {
		return 0, nil
	}
	n, err := c.rdb.LLen(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return int(n), nil
}
