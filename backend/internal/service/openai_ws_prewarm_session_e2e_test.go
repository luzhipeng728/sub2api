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
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		// OAuth + access_token：即时现铸 prewarm 锚点需要拿到上游 token(与生产一致)。
		Credentials: map[string]any{
			"access_token":       "oauth-test-token",
			"chatgpt_account_id": "acct-test",
		},
		Extra: map[string]any{
			"responses_websockets_v2_enabled": true,
			"codex_5h_used_percent":           100.0,
			"codex_5h_reset_at":               time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
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

	_ = normModel
	// 即时现铸语义:第 1 轮读到 = 现铸 prewarm 锚点(返回 prewarmID),第 2 轮 = 正式请求(返回 mainID)。
	// 两轮共用同一 mock 连接,events 顺序消费。
	captureConn := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"` + prewarmID + `","model":"gpt-5.1"}}`),
			[]byte(`{"type":"response.completed","response":{"id":"` + mainID + `","model":"gpt-5.1","usage":{"input_tokens":4,"output_tokens":2}}}`),
		},
	}
	svc, captureConn, _ := buildPrewarmSessionTestService(t, captureConn)

	ctx := context.Background()

	// 发起正式请求（不带 previous_response_id）。Forward 会即时现铸一个锚点并注入。
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	c.Request.Header.Set("session_id", "session-e2e-inject")
	body := []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`)

	result, err := svc.Forward(ctx, c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, mainID, result.RequestID)

	// 断言:正式请求把"刚现铸出来的 prewarm 锚点"作为 previous_response_id 注入。
	require.GreaterOrEqual(t, len(captureConn.writes), 2, "应有现铸轮 + 正式轮两次上游写入")
	lastWrite := requestToJSONString(captureConn.writes[len(captureConn.writes)-1])
	require.Equal(t, prewarmID, gjson.Get(lastWrite, "previous_response_id").String(),
		"正式请求应注入即时现铸出来的 prewarm 锚点 id")
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

func TestPrewarmSession_E2E_NoInjectionWhen5hHasHeadroom(t *testing.T) {
	gin.SetMode(gin.TestMode)

	account := newPrewarmSessionTestAccount()
	account.Extra["codex_5h_used_percent"] = 42.0
	account.Extra["codex_5h_reset_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	const mainID = "resp_main_5h_headroom_1"

	captureConn := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"` + mainID + `","model":"gpt-5.1","usage":{"input_tokens":4,"output_tokens":2}}}`),
		},
	}
	svc, captureConn, _ := buildPrewarmSessionTestService(t, captureConn)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	c.Request.Header.Set("session_id", "session-e2e-5h-headroom")
	body := []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`)

	result, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, mainID, result.RequestID)

	require.Len(t, captureConn.writes, 1, "5h 未达软限制时应直接走原生请求，不应先现铸 prewarm")
	lastWrite := requestToJSONString(captureConn.writes[len(captureConn.writes)-1])
	require.False(t, gjson.Get(lastWrite, "previous_response_id").Exists(),
		"5h 未达软限制时不应注入 previous_response_id")
}

// TestPrewarmSession_Invalidate_OnStaleBinding 验证失效自愈：
// store 里的 prewarm_id 在 stateStore 中已不指向本账号（绑定陈旧/跨账号串），
// tryGetOpenAIPrewarmSession 应清理该绑定并返回不注入。
func TestPrewarmSession_Invalidate_OnStaleBinding(t *testing.T) {
	gin.SetMode(gin.TestMode)

	account := newPrewarmSessionTestAccount()
	const stalePrewarmID = "resp_stale_1"
	const freshPrewarmID = "resp_fresh_stale_1"
	const mainID = "resp_main_stale_1"

	// 即时现铸:第 1 轮 = 现铸新锚点(freshPrewarmID),第 2 轮 = 正式请求(mainID)。
	captureConn := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"` + freshPrewarmID + `","model":"gpt-5.1"}}`),
			[]byte(`{"type":"response.completed","response":{"id":"` + mainID + `","model":"gpt-5.1","usage":{"input_tokens":4,"output_tokens":2}}}`),
		},
	}
	svc, captureConn, prewarmStore := buildPrewarmSessionTestService(t, captureConn)

	ctx := context.Background()
	groupID := int64(0)

	// 预置：池里有 prewarm_id，但 stateStore 的绑定指向「另一个账号」（模拟跨账号串/陈旧）。
	require.NoError(t, prewarmStore.PushPrewarmSessionPool(ctx, account.ID, "gpt-5.1", stalePrewarmID, 8, time.Hour))
	stateStore := svc.getOpenAIWSStateStore()
	require.NoError(t, stateStore.BindResponseAccount(ctx, groupID, stalePrewarmID, 99999, time.Hour))

	// 验证 tryGetOpenAIPrewarmSession 会因反向验证失败而丢弃该 id 返回不注入。
	id, ok := svc.tryGetOpenAIPrewarmSession(ctx, groupID, account, "gpt-5.1")
	require.False(t, ok, "反向验证失败时应返回不注入")
	require.Equal(t, "", id)

	// 断言：陈旧 id 已被 pop 丢弃，池为空。
	poolLen, err := prewarmStore.PrewarmSessionPoolLen(ctx, account.ID, "gpt-5.1")
	require.NoError(t, err)
	require.Equal(t, 0, poolLen, "失效的 prewarm id 应被 pop 丢弃")

	// 即时现铸语义:正式请求不复用陈旧池 id,而是现铸一个全新锚点并注入。
	// 因此绝不会注入陈旧的 stalePrewarmID,而是注入新铸的 freshPrewarmID。
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	c.Request.Header.Set("session_id", "session-e2e-stale")
	body := []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`)

	result, err := svc.Forward(ctx, c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, mainID, result.RequestID)

	require.GreaterOrEqual(t, len(captureConn.writes), 2, "应有现铸轮 + 正式轮两次上游写入")
	lastWrite := requestToJSONString(captureConn.writes[len(captureConn.writes)-1])
	injected := gjson.Get(lastWrite, "previous_response_id").String()
	require.Equal(t, freshPrewarmID, injected, "应注入即时现铸的新锚点")
	require.NotEqual(t, stalePrewarmID, injected, "绝不应注入陈旧的池 id")
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
