package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// openAIWSPrewarmSessionCachePrefix 为 Redis 中 prewarm session 绑定的 key 前缀。
	// 完整 key 形如 openai:prewarm:session:{accountID}:{modelHash}。
	openAIWSPrewarmSessionCachePrefix = "openai:prewarm:session:"
	// openAIWSPrewarmSessionRedisTimeout 限制 Redis 读写时间，避免拖慢请求主路径。
	openAIWSPrewarmSessionRedisTimeout = 3 * time.Second
	// openAIWSPrewarmSessionStaleRatio 快过期阈值：剩余 TTL 低于总 TTL 的该比例即视为需要刷新。
	openAIWSPrewarmSessionStaleRatio = 0.2
)

// openAIWSPrewarmSessionValue 是 prewarm session 绑定在 Redis 中的序列化值。
// CreatedAtUnix 用于在不需要额外 TTL 查询的前提下判断绑定的新鲜度。
type openAIWSPrewarmSessionValue struct {
	ResponseID    string `json:"response_id"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

// OpenAIWSPrewarmSessionStore 管理「(accountID, model) → prewarm response_id」的持久化绑定。
// 与 OpenAIWSStateStore 的 response_id→account 绑定互补：
//   - state store 记录 response_id → account，用于续接路由（按 group 隔离）；
//   - 本 store 反向记录 account + model → response_id，用于在请求未带 previous_response_id
//     时取出该账号该模型的预热 id 并注入，让上游当作续接处理。
//
// prewarm 是账号级资源，与 group 无关，因此 key 仅用 accountID + model（跨 group 共享）。
// 实现需跨进程持久化（Redis），TTL 与 response_id→account 绑定保持一致。
type OpenAIWSPrewarmSessionStore interface {
	// SetPrewarmSession 写入 (accountID, model) → responseID 绑定，ttl 为有效期。
	SetPrewarmSession(ctx context.Context, accountID int64, model, responseID string, ttl time.Duration) error
	// GetPrewarmSession 读取 (accountID, model) 的预热 response_id；未命中返回 ("", false, nil)。
	GetPrewarmSession(ctx context.Context, accountID int64, model string) (string, bool, error)
	// ClaimPrewarmSession 原子地"读取并删除"(accountID, model) 的预热 response_id。
	// prewarm id 是一次性消费(store=false)，必须 claim-on-read：并发请求中只有一个能拿到缓存 id，
	// 其余返回未命中并各自铸造独立 id，从根本上避免多个请求复用同一个 id 导致 previous_response_not_found。
	ClaimPrewarmSession(ctx context.Context, accountID int64, model string) (string, bool, error)
	// GetPrewarmSessionWithFreshness 读取并返回新鲜度比例(剩余/总，0~1)；未命中返回 false。
	// worker 用它判断是否需要刷新（剩余比例 < openAIWSPrewarmSessionStaleRatio 时刷新）。
	GetPrewarmSessionWithFreshness(ctx context.Context, accountID int64, model string) (responseID string, freshness float64, ok bool, err error)
	// DeletePrewarmSession 删除绑定（用于 response_id 失效自愈）。
	DeletePrewarmSession(ctx context.Context, accountID int64, model string) error
}

// OpenAIPrewarmSessionCache 是 prewarm session 绑定的缓存后端抽象（Redis 实现）。
// 与 LeaderLockCache 一样放在 repository 层，service 层不直接依赖 redis。
type OpenAIPrewarmSessionCache interface {
	SetPrewarmSession(ctx context.Context, key, value string, ttl time.Duration) error
	GetPrewarmSession(ctx context.Context, key string) (value string, ttl time.Duration, err error) // ttl<=0 表示不存在
	// ClaimPrewarmSession 原子读取并删除 key（Redis GETDEL）；不存在返回 ("", nil)。
	ClaimPrewarmSession(ctx context.Context, key string) (value string, err error)
	DeletePrewarmSession(ctx context.Context, key string) error
}

// defaultOpenAIWSPrewarmSessionStore 是基于 OpenAIPrewarmSessionCache 的默认实现。
type defaultOpenAIWSPrewarmSessionStore struct {
	cache OpenAIPrewarmSessionCache
}

// NewOpenAIWSPrewarmSessionStore 创建默认的 prewarm session 存储实现。
func NewOpenAIWSPrewarmSessionStore(cache OpenAIPrewarmSessionCache) OpenAIWSPrewarmSessionStore {
	if cache == nil {
		return nil
	}
	return &defaultOpenAIWSPrewarmSessionStore{cache: cache}
}

func (s *defaultOpenAIWSPrewarmSessionStore) SetPrewarmSession(ctx context.Context, accountID int64, model, responseID string, ttl time.Duration) error {
	id := strings.TrimSpace(responseID)
	if accountID <= 0 || normalizeOpenAIPrewarmModelKey(model) == "" || id == "" {
		return nil
	}
	ttl = normalizeOpenAIPrewarmTTL(ttl)
	value := openAIWSPrewarmSessionValue{
		ResponseID:    id,
		CreatedAtUnix: time.Now().Unix(),
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal prewarm session value: %w", err)
	}
	keyCtx, cancel := withOpenAIPrewarmSessionTimeout(ctx)
	defer cancel()
	return s.cache.SetPrewarmSession(keyCtx, openAIPrewarmSessionCacheKey(accountID, model), string(payload), ttl)
}

func (s *defaultOpenAIWSPrewarmSessionStore) GetPrewarmSession(ctx context.Context, accountID int64, model string) (string, bool, error) {
	id, _, ok, err := s.GetPrewarmSessionWithFreshness(ctx, accountID, model)
	return id, ok, err
}

func (s *defaultOpenAIWSPrewarmSessionStore) GetPrewarmSessionWithFreshness(ctx context.Context, accountID int64, model string) (string, float64, bool, error) {
	if accountID <= 0 || normalizeOpenAIPrewarmModelKey(model) == "" {
		return "", 0, false, nil
	}
	keyCtx, cancel := withOpenAIPrewarmSessionTimeout(ctx)
	defer cancel()
	raw, ttl, err := s.cache.GetPrewarmSession(keyCtx, openAIPrewarmSessionCacheKey(accountID, model))
	if err != nil {
		return "", 0, false, err
	}
	if strings.TrimSpace(raw) == "" || ttl <= 0 {
		return "", 0, false, nil
	}
	var value openAIWSPrewarmSessionValue
	if err := json.Unmarshal([]byte(raw), &value); err != nil || strings.TrimSpace(value.ResponseID) == "" {
		return "", 0, false, nil
	}
	// freshness = 剩余 TTL / (now - created)。无法精确得知写入 TTL，用 created+ttl 近似。
	now := time.Now()
	created := time.Unix(value.CreatedAtUnix, 0)
	total := now.Sub(created)
	if total <= 0 {
		return value.ResponseID, 1, true, nil
	}
	freshness := float64(ttl) / float64(total.Seconds())
	if freshness < 0 {
		freshness = 0
	}
	if freshness > 1 {
		freshness = 1
	}
	return value.ResponseID, freshness, true, nil
}

func (s *defaultOpenAIWSPrewarmSessionStore) ClaimPrewarmSession(ctx context.Context, accountID int64, model string) (string, bool, error) {
	if accountID <= 0 || normalizeOpenAIPrewarmModelKey(model) == "" {
		return "", false, nil
	}
	keyCtx, cancel := withOpenAIPrewarmSessionTimeout(ctx)
	defer cancel()
	raw, err := s.cache.ClaimPrewarmSession(keyCtx, openAIPrewarmSessionCacheKey(accountID, model))
	if err != nil {
		return "", false, err
	}
	if strings.TrimSpace(raw) == "" {
		return "", false, nil
	}
	var value openAIWSPrewarmSessionValue
	if err := json.Unmarshal([]byte(raw), &value); err != nil || strings.TrimSpace(value.ResponseID) == "" {
		return "", false, nil
	}
	return value.ResponseID, true, nil
}

func (s *defaultOpenAIWSPrewarmSessionStore) DeletePrewarmSession(ctx context.Context, accountID int64, model string) error {
	if accountID <= 0 || normalizeOpenAIPrewarmModelKey(model) == "" {
		return nil
	}
	keyCtx, cancel := withOpenAIPrewarmSessionTimeout(ctx)
	defer cancel()
	return s.cache.DeletePrewarmSession(keyCtx, openAIPrewarmSessionCacheKey(accountID, model))
}

// noOpOpenAIWSPrewarmSessionStore 是禁用时的空实现，所有写操作为 no-op，读永远未命中。
type noOpOpenAIWSPrewarmSessionStore struct{}

func (noOpOpenAIWSPrewarmSessionStore) SetPrewarmSession(context.Context, int64, string, string, time.Duration) error {
	return nil
}
func (noOpOpenAIWSPrewarmSessionStore) GetPrewarmSession(context.Context, int64, string) (string, bool, error) {
	return "", false, nil
}
func (noOpOpenAIWSPrewarmSessionStore) ClaimPrewarmSession(context.Context, int64, string) (string, bool, error) {
	return "", false, nil
}
func (noOpOpenAIWSPrewarmSessionStore) GetPrewarmSessionWithFreshness(context.Context, int64, string) (string, float64, bool, error) {
	return "", 0, false, nil
}
func (noOpOpenAIWSPrewarmSessionStore) DeletePrewarmSession(context.Context, int64, string) error {
	return nil
}

// openAIPrewarmSessionCacheKey 构造 Redis key：openai:prewarm:session:{accountID}:{modelHash}。
func openAIPrewarmSessionCacheKey(accountID int64, model string) string {
	modelKey := normalizeOpenAIPrewarmModelKey(model)
	sum := sha256.Sum256([]byte(modelKey))
	return fmt.Sprintf("%s%d:%s", openAIWSPrewarmSessionCachePrefix, accountID, hex.EncodeToString(sum[:]))
}

// normalizeOpenAIPrewarmModelKey 把模型名归一到 codex 上游真实型号，作为 prewarm 维度 key。
// 复用 normalizeCodexModel（OAuth 账号模型归一），确保 gpt-5.4-high 与 gpt-5.4 命中同一预热。
func normalizeOpenAIPrewarmModelKey(model string) string {
	normalized := strings.TrimSpace(model)
	if normalized == "" {
		return ""
	}
	return normalizeCodexModel(normalized)
}

func normalizeOpenAIPrewarmTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return time.Hour
	}
	return ttl
}

func withOpenAIPrewarmSessionTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, openAIWSPrewarmSessionRedisTimeout)
}

// errOpenAIPrewarmSessionDisabled 是功能未启用时返回的哨兵错误（仅用于内部判断）。
var errOpenAIPrewarmSessionDisabled = errors.New("openai ws prewarm session disabled")
