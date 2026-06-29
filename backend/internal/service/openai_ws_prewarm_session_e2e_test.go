package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// buildPrewarmSessionTestService 构造一个挂载了 prewarm session store + stateStore + fake 上游池的 service，
// 用于端到端验证注入逻辑。不依赖真实 OpenAI 账号、不依赖 Redis。
func buildPrewarmSessionTestService(t *testing.T, captureConn *openAIWSCaptureConn) (*OpenAIGatewayService, *openAIWSCaptureConn, OpenAIWSPrewarmSessionStore) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	// 开启账号级 prewarm session，注入逻辑才会触发。
	cfg.Gateway.OpenAIWS.PrewarmSessionEnabled = true
	cfg.Gateway.OpenAIWS.PrewarmSessionModels = []string{"gpt-5.1"}
	cfg.Gateway.OpenAIWS.PrewarmSessionTTLSeconds = 3600
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1

	captureDialer := &openAIWSCaptureDialer{conn: captureConn}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(captureDialer)

	cache := &stubGatewayCache{}
	svc := &OpenAIGatewayService{
		cfg:              cfg,
		cache:            cache,
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	// 注入内存版 prewarm session store（绕过 Redis）。
	prewarmCache := newFakePrewarmSessionCache()
	prewarmStore := NewOpenAIWSPrewarmSessionStore(prewarmCache)
	svc.SetOpenAIPrewarmSessionCache(prewarmCache)

	return svc, captureConn, prewarmStore
}

func newPrewarmSessionTestAccount() *Account {
	return &Account{
		ID:          9001,
		Name:        "openai-prewarm-e2e",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key": "sk-test",
		},
		Extra: map[string]any{
			"responses_websockets_v2_enabled": true,
		},
	}
}

// TestPrewarmSession_E2E_Injection 验证核心链路：
// store 里已有 prewarm_id → 正式请求未带 previous_response_id → 注入逻辑把 prewarm_id 注入 payload。
func TestPrewarmSession_E2E_Injection(t *testing.T) {
	gin.SetMode(gin.TestMode)

	account := newPrewarmSessionTestAccount()
	const prewarmID = "resp_prewarm_e2e_1"
	const mainID = "resp_main_e2e_1"
	// 用归一化后的 model（normalizeCodexModel 的输出）作为 store key，与注入逻辑读取的 key 对齐。
	const normModel = "gpt-5.4"

	captureConn := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"` + mainID + `","model":"gpt-5.1","usage":{"input_tokens":4,"output_tokens":2}}}`),
		},
	}
	svc, captureConn, prewarmStore := buildPrewarmSessionTestService(t, captureConn)

	ctx := context.Background()
	groupID := int64(0)

	// 预置：store 已有该账号该模型(归一化后)的 prewarm_id，且 stateStore 的 response_id→account 绑定也指向本账号。
	require.NoError(t, prewarmStore.SetPrewarmSession(ctx, account.ID, normModel, prewarmID, time.Hour))
	stateStore := svc.getOpenAIWSStateStore()
	require.NotNil(t, stateStore)
	require.NoError(t, stateStore.BindResponseAccount(ctx, groupID, prewarmID, account.ID, time.Hour))

	// 发起正式请求（不带 previous_response_id）。请求 model 用 gpt-5.1，注入逻辑会归一化后匹配 store。
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	c.Request.Header.Set("session_id", "session-e2e-inject")
	body := []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`)

	result, err := svc.Forward(ctx, c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, mainID, result.RequestID)

	// 断言：注入逻辑把 prewarm_id 写入了发给上游的 payload。
	require.NotEmpty(t, captureConn.writes, "应至少有一次上游写入")
	lastWrite := requestToJSONString(captureConn.writes[len(captureConn.writes)-1])
	require.Equal(t, prewarmID, gjson.Get(lastWrite, "previous_response_id").String(),
		"正式请求未带 previous_response_id 时应注入 store 里的 prewarm_id")
}

// TestPrewarmSession_E2E_NoInjectionWhenDisabled 验证：功能关闭时不注入，保持原有行为。
func TestPrewarmSession_E2E_NoInjectionWhenDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	account := newPrewarmSessionTestAccount()
	const mainID = "resp_main_disabled_1"

	captureConn := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"` + mainID + `","model":"gpt-5.1","usage":{"input_tokens":4,"output_tokens":2}}}`),
		},
	}
	svc, captureConn, _ := buildPrewarmSessionTestService(t, captureConn)
	// 关闭功能。
	svc.cfg.Gateway.OpenAIWS.PrewarmSessionEnabled = false

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	c.Request.Header.Set("session_id", "session-e2e-disabled")
	body := []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`)

	result, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, mainID, result.RequestID)

	// 断言：功能关闭时不应注入 previous_response_id。
	require.NotEmpty(t, captureConn.writes)
	lastWrite := requestToJSONString(captureConn.writes[len(captureConn.writes)-1])
	require.False(t, gjson.Get(lastWrite, "previous_response_id").Exists(),
		"功能关闭时不应注入 previous_response_id")
}

// TestPrewarmSession_Invalidate_OnStaleBinding 验证失效自愈：
// store 里的 prewarm_id 在 stateStore 中已不指向本账号（绑定陈旧/跨账号串），
// tryGetOpenAIPrewarmSession 应清理该绑定并返回不注入。
func TestPrewarmSession_Invalidate_OnStaleBinding(t *testing.T) {
	gin.SetMode(gin.TestMode)

	account := newPrewarmSessionTestAccount()
	const stalePrewarmID = "resp_stale_1"
	const mainID = "resp_main_stale_1"

	captureConn := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"` + mainID + `","model":"gpt-5.1","usage":{"input_tokens":4,"output_tokens":2}}}`),
		},
	}
	svc, captureConn, prewarmStore := buildPrewarmSessionTestService(t, captureConn)

	ctx := context.Background()
	groupID := int64(0)

	// 预置：store 有 prewarm_id，但 stateStore 的绑定指向「另一个账号」（模拟跨账号串/陈旧）。
	require.NoError(t, prewarmStore.SetPrewarmSession(ctx, account.ID, "gpt-5.1", stalePrewarmID, time.Hour))
	stateStore := svc.getOpenAIWSStateStore()
	require.NoError(t, stateStore.BindResponseAccount(ctx, groupID, stalePrewarmID, 99999, time.Hour))

	// 验证 tryGetOpenAIPrewarmSession 会因反向验证失败而清理绑定。
	id, ok := svc.tryGetOpenAIPrewarmSession(ctx, groupID, account, "gpt-5.1")
	require.False(t, ok, "反向验证失败时应返回不注入")
	require.Equal(t, "", id)

	// 断言：stale 绑定已被清理。
	remaining, stillThere, err := prewarmStore.GetPrewarmSession(ctx, account.ID, "gpt-5.1")
	require.NoError(t, err)
	require.False(t, stillThere, "失效的 prewarm 绑定应被清理")
	require.Equal(t, "", remaining)

	// 正式请求应退化为「不带 previous_response_id 的普通请求」。
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	c.Request.Header.Set("session_id", "session-e2e-stale")
	body := []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`)

	result, err := svc.Forward(ctx, c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, mainID, result.RequestID)

	require.NotEmpty(t, captureConn.writes)
	lastWrite := requestToJSONString(captureConn.writes[len(captureConn.writes)-1])
	require.False(t, gjson.Get(lastWrite, "previous_response_id").Exists(),
		"失效绑定清理后不应注入 previous_response_id")
}

// TestPrewarmSession_Invalidate_Explicit 验证显式失效方法直接清理绑定。
func TestPrewarmSession_Invalidate_Explicit(t *testing.T) {
	account := newPreshwarmSessionInvalidateAccount()
	ctx := context.Background()

	cache := newFakePrewarmSessionCache()
	store := NewOpenAIWSPrewarmSessionStore(cache)

	require.NoError(t, store.SetPrewarmSession(ctx, account.ID, "gpt-5.1", "resp_to_invalidate", time.Hour))
	_, ok, err := store.GetPrewarmSession(ctx, account.ID, "gpt-5.1")
	require.NoError(t, err)
	require.True(t, ok)

	// 构造一个最小 service 仅用于调用 invalidate。
	svc := &OpenAIGatewayService{
		cfg: &config.Config{},
	}
	svc.SetOpenAIPrewarmSessionCache(cache)
	svc.cfg.Gateway.OpenAIWS.PrewarmSessionEnabled = true
	svc.invalidateOpenAIPrewarmSession(ctx, account, "gpt-5.1")

	_, ok, err = store.GetPrewarmSession(ctx, account.ID, "gpt-5.1")
	require.NoError(t, err)
	require.False(t, ok, "显式 invalidate 后绑定应被清理")
}

// newPreshwarmSessionInvalidateAccount 返回一个用于 invalidate 单测的最小账号。
func newPreshwarmSessionInvalidateAccount() *Account {
	return &Account{
		ID:          9002,
		Name:        "openai-prewarm-invalidate",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
	}
}
