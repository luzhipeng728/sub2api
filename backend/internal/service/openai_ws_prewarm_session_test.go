package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakePrewarmSessionCache 是 OpenAIPrewarmSessionCache 的内存实现，用于单测。
type fakePrewarmSessionCache struct {
	data map[string]fakePrewarmEntry
	pool map[string][]string
	err  error
}
type fakePrewarmEntry struct {
	value     string
	ttl       time.Duration
	createdAt time.Time
}

func newFakePrewarmSessionCache() *fakePrewarmSessionCache {
	return &fakePrewarmSessionCache{data: make(map[string]fakePrewarmEntry), pool: make(map[string][]string)}
}

func (c *fakePrewarmSessionCache) SetPrewarmSession(_ context.Context, key, value string, ttl time.Duration) error {
	if c.err != nil {
		return c.err
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	c.data[key] = fakePrewarmEntry{value: value, ttl: ttl, createdAt: time.Now()}
	return nil
}

func (c *fakePrewarmSessionCache) GetPrewarmSession(ctx context.Context, key string) (string, time.Duration, error) {
	if c.err != nil {
		return "", 0, c.err
	}
	entry, ok := c.data[key]
	if !ok {
		return "", 0, nil
	}
	// 模拟 TTL 衰减：已过期则视为不存在。
	elapsed := time.Since(entry.createdAt)
	remaining := entry.ttl - elapsed
	if remaining <= 0 {
		return "", 0, nil
	}
	return entry.value, remaining, nil
}

func (c *fakePrewarmSessionCache) ClaimPrewarmSession(ctx context.Context, key string) (string, error) {
	if c.err != nil {
		return "", c.err
	}
	value, _, err := c.GetPrewarmSession(ctx, key)
	if err != nil {
		return "", err
	}
	delete(c.data, key)
	return value, nil
}

func (c *fakePrewarmSessionCache) DeletePrewarmSession(_ context.Context, key string) error {
	delete(c.data, key)
	return nil
}

func (c *fakePrewarmSessionCache) PushPrewarmPool(_ context.Context, key, value string, maxDepth int, ttl time.Duration) error {
	if c.err != nil {
		return c.err
	}
	if maxDepth <= 0 {
		maxDepth = 1
	}
	c.pool[key] = append(c.pool[key], value)
	if n := len(c.pool[key]); n > maxDepth {
		c.pool[key] = c.pool[key][n-maxDepth:]
	}
	return nil
}

func (c *fakePrewarmSessionCache) PopPrewarmPool(_ context.Context, key string) (string, error) {
	if c.err != nil {
		return "", c.err
	}
	q := c.pool[key]
	if len(q) == 0 {
		return "", nil
	}
	v := q[0]
	c.pool[key] = q[1:]
	return v, nil
}

func (c *fakePrewarmSessionCache) PrewarmPoolLen(_ context.Context, key string) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	return len(c.pool[key]), nil
}

func TestPrewarmSessionStore_SetGetDelete(t *testing.T) {
	cache := newFakePrewarmSessionCache()
	store := NewOpenAIWSPrewarmSessionStore(cache)
	ctx := context.Background()

	// 未命中
	id, ok, err := store.GetPrewarmSession(ctx, 1, "gpt-5.4")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "", id)

	// 写入
	require.NoError(t, store.SetPrewarmSession(ctx, 1, "gpt-5.4", "resp_AAA", time.Minute))
	id, ok, err = store.GetPrewarmSession(ctx, 1, "gpt-5.4")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "resp_AAA", id)

	// 删除
	require.NoError(t, store.DeletePrewarmSession(ctx, 1, "gpt-5.4"))
	id, ok, err = store.GetPrewarmSession(ctx, 1, "gpt-5.4")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPrewarmSessionStore_ModelNormalizationKey(t *testing.T) {
	cache := newFakePrewarmSessionCache()
	store := NewOpenAIWSPrewarmSessionStore(cache)
	ctx := context.Background()

	// 不同 effort 后缀应归一到同一 key（命中同一预热）。
	require.NoError(t, store.SetPrewarmSession(ctx, 1, "gpt-5.4-high", "resp_NORM", time.Minute))
	id, ok, err := store.GetPrewarmSession(ctx, 1, "gpt-5.4")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "resp_NORM", id)
}

func TestPrewarmSessionStore_GetPrewarmSessionWithFreshness(t *testing.T) {
	cache := newFakePrewarmSessionCache()
	store := NewOpenAIWSPrewarmSessionStore(cache)
	ctx := context.Background()

	require.NoError(t, store.SetPrewarmSession(ctx, 2, "gpt-5.3-codex", "resp_FRESH", time.Hour))
	id, freshness, ok, err := store.GetPrewarmSessionWithFreshness(ctx, 2, "gpt-5.3-codex")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "resp_FRESH", id)
	// 刚写入，freshness 应接近 1。
	require.Greater(t, freshness, 0.9)
}

func TestPrewarmSessionStore_EmptyInputsNoOp(t *testing.T) {
	cache := newFakePrewarmSessionCache()
	store := NewOpenAIWSPrewarmSessionStore(cache)
	ctx := context.Background()

	// accountID<=0 / 空 model / 空 responseID 都应忽略。
	require.NoError(t, store.SetPrewarmSession(ctx, 0, "gpt-5.4", "resp", time.Minute))
	require.NoError(t, store.SetPrewarmSession(ctx, 1, "", "resp", time.Minute))
	require.NoError(t, store.SetPrewarmSession(ctx, 1, "gpt-5.4", "  ", time.Minute))
	id, ok, err := store.GetPrewarmSession(ctx, 1, "gpt-5.4")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "", id)
}

func TestPrewarmSessionStore_NilCacheReturnsNil(t *testing.T) {
	// NewOpenAIWSPrewarmSessionCache 传 nil 应返回 nil store。
	require.Nil(t, NewOpenAIWSPrewarmSessionStore(nil))
}

func TestNormalizeOpenAIPrewarmModelKey(t *testing.T) {
	// effort 后缀剥离
	require.Equal(t, "gpt-5.4", normalizeOpenAIPrewarmModelKey("gpt-5.4-high"))
	require.Equal(t, "gpt-5.4", normalizeOpenAIPrewarmModelKey("gpt-5.4-none"))
	// 旧版本升级
	require.Equal(t, "gpt-5.3-codex", normalizeOpenAIPrewarmModelKey("gpt-5.1-codex"))
	// 空值
	require.Equal(t, "", normalizeOpenAIPrewarmModelKey(""))
	require.Equal(t, "", normalizeOpenAIPrewarmModelKey("   "))
}

func TestEffectiveOpenAIPrewarmGroupIDs(t *testing.T) {
	require.Equal(t, []int64{0}, effectiveOpenAIPrewarmGroupIDs(nil))
	require.Equal(t, []int64{0}, effectiveOpenAIPrewarmGroupIDs([]int64{}))
	require.Equal(t, []int64{1, 2}, effectiveOpenAIPrewarmGroupIDs([]int64{1, 2}))
}

func TestEnsureOpenAIPrewarmContinuationInput_RemovesSystemMessages(t *testing.T) {
	// user prompt 转进 instructions，input 清空，system 被忽略。
	payload := map[string]any{
		"input": []any{
			map[string]any{"role": "system", "content": "sys"},
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{"role": "system", "content": "sys2"},
		},
	}
	ensureOpenAIPrewarmContinuationInput(payload, "gpt-5.4")
	require.Equal(t, " ", payload["instructions"])
	input, ok := payload["input"].([]any)
	require.True(t, ok)
	require.Len(t, input, 1)
}

func TestEnsureOpenAIPrewarmContinuationInput_NoSystemNoChange(t *testing.T) {
	payload := map[string]any{
		"input": []any{
			map[string]any{"role": "user", "content": "hi"},
		},
	}
	ensureOpenAIPrewarmContinuationInput(payload, "gpt-5.4")
	require.Equal(t, " ", payload["instructions"])
	input, ok := payload["input"].([]any)
	require.True(t, ok)
	require.Len(t, input, 1)
}

func TestEnsureOpenAIPrewarmContinuationInput_StringInput(t *testing.T) {
	// input 是字符串时，转进 instructions，input 清空。
	payload := map[string]any{"input": "hello world"}
	ensureOpenAIPrewarmContinuationInput(payload, "gpt-5.4")
	require.Equal(t, " ", payload["instructions"])
	input, ok := payload["input"].([]any)
	require.True(t, ok)
	require.Len(t, input, 1)
}

func TestEnsureOpenAIPrewarmContinuationInput_PreservesAssistantItems(t *testing.T) {
	// 多轮场景：首个 user 提取到 instructions，其余内容不保留（input 全清空）。
	payload := map[string]any{
		"input": []any{
			map[string]any{"role": "user", "content": "question"},
			map[string]any{"role": "assistant", "content": "prev answer"},
		},
	}
	ensureOpenAIPrewarmContinuationInput(payload, "gpt-5.4")
	require.Equal(t, " ", payload["instructions"])
	input, ok := payload["input"].([]any)
	require.True(t, ok)
	require.Len(t, input, 1)
}

func TestNoOpPrewarmSessionStore(t *testing.T) {
	var noop noOpOpenAIWSPrewarmSessionStore
	ctx := context.Background()
	err := noop.SetPrewarmSession(ctx, 1, "gpt-5.4", "resp", time.Minute)
	require.NoError(t, err)
	id, ok, err := noop.GetPrewarmSession(ctx, 1, "gpt-5.4")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "", id)
	require.NoError(t, noop.DeletePrewarmSession(ctx, 1, "gpt-5.4"))
}

func TestErrOpenAIPrewarmSessionDisabled(t *testing.T) {
	require.Error(t, errOpenAIPrewarmSessionDisabled)
	require.True(t, errors.Is(errOpenAIPrewarmSessionDisabled, errOpenAIPrewarmSessionDisabled))
}
