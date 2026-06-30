package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tidwall/sjson"
	"golang.org/x/sync/singleflight"
)

// openAIPrewarmSessionPrewarmGroup 用于对「同账号同模型的同步兜底预热」做 singleflight 去重：
// 高并发下若多个请求同时 cache miss，只允许第一个真正执行 prewarm，其余等待复用结果，
// 避免瞬时 N 个请求 = N 次空 prewarm（浪费连接 + 上游资源）。
var openAIPrewarmSessionPrewarmGroup singleflight.Group

// openAIPrewarmSessionInstructions 是空预热轮注入的最小非空指令。
// OAuth 上游要求 instructions 字段非空（否则 400 "Instructions are required"），
// 预热轮无需真实系统提示，用单空格满足约束且尽量减少对上下文的污染。
const openAIPrewarmSessionInstructions = " "

// performOpenAIWSPrewarmSession 为指定 (account, model) 执行一次空预热：
// 发送 generate=false 的 response.create，拿到 response_id 后持久化到
// (accountID, model) → response_id 绑定，并反向绑定 response_id → account，
// 使后续未带 previous_response_id 的请求能取出该 id 续接。
//
// 与 performOpenAIWSGeneratePrewarm 的区别：
//   - generate prewarm 是连接级、复用客户端 payload、在请求路径内同步执行；
//   - 本方法是账号级、构造空 payload、后台/兜底执行、互不干扰。
//
// groupIDs 为账号所属的分组列表，预热 response_id 会绑定到每个 group（供续接路由）；
// 为空时仅绑定 groupID=0。
// 返回拿到的 response_id（成功时）或 error（失败时）。失败不持久化，由调用方决定是否重试。
func (s *OpenAIGatewayService) performOpenAIWSPrewarmSession(
	ctx context.Context,
	groupIDs []int64,
	account *Account,
	model string,
) (string, error) {
	if s == nil || account == nil {
		return "", errors.New("prewarm session: service or account is nil")
	}
	normalizedModel := normalizeOpenAIPrewarmModelKey(model)
	if normalizedModel == "" {
		return "", errors.New("prewarm session: empty model")
	}

	// 仅 WSv2 OAuth 账号才有意义（response_id 续接依赖 WSv2）。
	decision := s.getOpenAIWSProtocolResolver().Resolve(account)
	if decision.Transport != OpenAIUpstreamTransportResponsesWebsocketV2 {
		return "", errOpenAIPrewarmSessionDisabled
	}

	prewarmStore := s.getOpenAIPrewarmSessionStore()
	stateStore := s.getOpenAIWSStateStore()
	if prewarmStore == nil || stateStore == nil {
		return "", errOpenAIPrewarmSessionDisabled
	}

	token, err := s.resolveOpenAIPrewarmToken(ctx, account)
	if err != nil {
		return "", err
	}

	wsURL, err := s.buildOpenAIResponsesWSURL(account)
	if err != nil {
		return "", err
	}
	// 后台预热无客户端请求，传 nil gin.Context；buildOpenAIWSHeaders 对 nil 安全。
	headers, _ := s.buildOpenAIWSHeaders(nil, account, token, decision, true, "", "", "")

	prewarmPayload := s.buildOpenAIPrewarmSessionPayload(normalizedModel)

	acquireCtx, acquireCancel := context.WithTimeout(ctx, s.openAIWSAcquireTimeout())
	lease, err := s.getOpenAIWSConnPool().Acquire(acquireCtx, openAIWSAcquireRequest{
		Account:      account,
		WSURL:        wsURL,
		Headers:      headers,
		ForceNewConn: false,
		ProxyURL: func() string {
			if account.ProxyID != nil && account.Proxy != nil {
				return account.Proxy.URL()
			}
			return ""
		}(),
	})
	acquireCancel()
	if err != nil {
		return "", err
	}
	// 确保最终释放连接（无论成败）。
	released := false
	releaseOnce := func() {
		if released {
			return
		}
		released = true
		lease.Release()
	}
	defer releaseOnce()

	connID := strings.TrimSpace(lease.ConnID())
	prewarmStart := time.Now()
	logOpenAIWSModeInfo("prewarm_session_start account_id=%d conn_id=%s model=%s", account.ID, connID, normalizedModel)

	if err := lease.WriteJSONWithContextTimeout(ctx, prewarmPayload, s.openAIWSWriteTimeout()); err != nil {
		lease.MarkBroken()
		logOpenAIWSModeInfo(
			"prewarm_session_write_fail account_id=%d conn_id=%s model=%s cause=%s",
			account.ID, connID, normalizedModel,
			truncateOpenAIWSLogValue(err.Error(), openAIWSLogValueMaxLen),
		)
		return "", err
	}
	logOpenAIWSModeInfo(
		"prewarm_session_write_sent account_id=%d conn_id=%s model=%s payload_bytes=%d",
		account.ID, connID, normalizedModel, len(payloadAsJSONBytes(prewarmPayload)),
	)

	prewarmResponseID := ""
	eventCount := 0
	terminalCount := 0
	for {
		message, readErr := lease.ReadMessageWithContextTimeout(ctx, s.openAIWSReadTimeout())
		if readErr != nil {
			lease.MarkBroken()
			closeStatus, closeReason := summarizeOpenAIWSReadCloseError(readErr)
			logOpenAIWSModeInfo(
				"prewarm_session_read_fail account_id=%d conn_id=%s model=%s close_status=%s close_reason=%s cause=%s events=%d",
				account.ID, connID, normalizedModel,
				closeStatus, closeReason,
				truncateOpenAIWSLogValue(readErr.Error(), openAIWSLogValueMaxLen),
				eventCount,
			)
			return "", readErr
		}

		eventType, eventResponseID, _ := parseOpenAIWSEventEnvelope(message)
		if eventType == "" {
			continue
		}
		eventCount++
		if prewarmResponseID == "" && eventResponseID != "" {
			prewarmResponseID = eventResponseID
		}
		if eventCount <= openAIWSPrewarmEventLogHead || eventType == "error" || isOpenAIWSTerminalEvent(eventType) {
			logOpenAIWSModeInfo(
				"prewarm_session_event account_id=%d conn_id=%s idx=%d type=%s bytes=%d",
				account.ID, connID, eventCount,
				truncateOpenAIWSLogValue(eventType, openAIWSLogValueMaxLen),
				len(message),
			)
		}

		if eventType == "error" {
			errCodeRaw, errTypeRaw, errMsgRaw := parseOpenAIWSErrorEventFields(message)
			s.persistOpenAIWSRateLimitSignal(ctx, account, lease.HandshakeHeaders(), message, errCodeRaw, errTypeRaw, errMsgRaw)
			errMsg := strings.TrimSpace(errMsgRaw)
			if errMsg == "" {
				errMsg = "OpenAI websocket prewarm session error"
			}
			errCode, errType, errMessage := summarizeOpenAIWSErrorEventFieldsFromRaw(errCodeRaw, errTypeRaw, errMsgRaw)
			logOpenAIWSModeInfo(
				"prewarm_session_error_event account_id=%d conn_id=%s idx=%d err_code=%s err_type=%s err_message=%s",
				account.ID, connID, eventCount, errCode, errType, errMessage,
			)
			lease.MarkBroken()
			return "", errors.New(errMsg)
		}

		if isOpenAIWSTerminalEvent(eventType) {
			terminalCount++
			break
		}
	}

	prewarmResponseID = strings.TrimSpace(prewarmResponseID)
	if prewarmResponseID == "" {
		logOpenAIWSModeInfo(
			"prewarm_session_no_response_id account_id=%d conn_id=%s model=%s events=%d terminal_events=%d duration_ms=%d",
			account.ID, connID, normalizedModel, eventCount, terminalCount,
			time.Since(prewarmStart).Milliseconds(),
		)
		return "", errors.New("prewarm session: no response_id in upstream events")
	}

	// 仅做反向绑定：response_id → account（按每个 group 绑定，供续接路由 + pop 时反向校验）。
	// 注意：不再在此写 (account,model)→id 的单槽位绑定——id 的归属由调用方决定：
	//   - worker：把 id 压入 pool；
	//   - 请求兜底(ensure...)：直接返回给本次请求使用，不入池(避免一次性 id 被二次取用)。
	ttl := s.openAIPrewarmSessionTTL()
	_ = prewarmStore // 保留引用，避免下游签名变动；池写入由调用方负责。
	for _, gid := range effectiveOpenAIPrewarmGroupIDs(groupIDs) {
		logOpenAIWSBindResponseAccountWarn(gid, account.ID, prewarmResponseID, stateStore.BindResponseAccount(ctx, gid, prewarmResponseID, account.ID, ttl))
	}
	stateStore.BindResponseConn(prewarmResponseID, lease.ConnID(), ttl)

	logOpenAIWSModeInfo(
		"prewarm_session_done account_id=%d conn_id=%s model=%s response_id=%s events=%d terminal_events=%d duration_ms=%d",
		account.ID, connID, normalizedModel,
		truncateOpenAIWSLogValue(prewarmResponseID, openAIWSIDValueMaxLen),
		eventCount, terminalCount,
		time.Since(prewarmStart).Milliseconds(),
	)
	return prewarmResponseID, nil
}

// effectiveOpenAIPrewarmGroupIDs 规整 groupIDs：为空时返回 {0}（全局占位）。
func effectiveOpenAIPrewarmGroupIDs(groupIDs []int64) []int64 {
	if len(groupIDs) == 0 {
		return []int64{0}
	}
	return groupIDs
}

// performOpenAIWSPrewarmSessionDedup 是 performOpenAIWSPrewarmSession 的 singleflight 包装：
// 同 (account, model) 维度的并发预热请求只执行一次，其余等待复用结果。
// 高并发下避免 N 个请求同时 cache miss 各自发起 N 次空 prewarm。
// 返回 (responseID, fromCache)；fromCache=true 表示复用了并发的预热结果。
func (s *OpenAIGatewayService) performOpenAIWSPrewarmSessionDedup(
	ctx context.Context,
	groupIDs []int64,
	account *Account,
	model string,
) (string, error) {
	key := fmt.Sprintf("prewarm:%d:%s", account.ID, normalizeOpenAIPrewarmModelKey(model))
	v, err, _ := openAIPrewarmSessionPrewarmGroup.Do(key, func() (any, error) {
		return s.performOpenAIWSPrewarmSession(ctx, groupIDs, account, model)
	})
	if err != nil {
		return "", err
	}
	id, _ := v.(string)
	return id, nil
}

// openAIPrewarmSessionSyncBudget 返回请求路径同步兜底预热的等待预算（默认 25s）。
func (s *OpenAIGatewayService) openAIPrewarmSessionSyncBudget() time.Duration {
	if s != nil && s.cfg != nil {
		if seconds := s.cfg.Gateway.OpenAIWS.PrewarmSessionSyncBudgetSeconds; seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return 25 * time.Second
}

// openAIPrewarmSessionBackgroundBudget 返回后台（脱离请求生命周期）预热的最大执行时长（默认 60s）。
func (s *OpenAIGatewayService) openAIPrewarmSessionBackgroundBudget() time.Duration {
	if s != nil && s.cfg != nil {
		if seconds := s.cfg.Gateway.OpenAIWS.PrewarmSessionBackgroundBudgetSeconds; seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return 60 * time.Second
}

// ensureOpenAIPrewarmSessionForRequest 是请求路径上的"有界"兜底预热入口。
//
// ⚠️ 关键：prewarm response_id 是"一次性消费"(store=false)——并发请求绝不能共享同一个 id，
// 否则只有一个能续接，其余全部收到 previous_response_not_found(400)。因此这里**每个请求各自
// 铸造独立 id，不做 singleflight 去重**（之前用 singleflight 反而把一次性 id 的争抢放大了）。
//
// 仅用 sync budget(默认 25s) 限制单次预热的等待时长：超时即返回 ("", false) 让本次请求降级
// (不带 previous_response_id)，避免被上游 15min read 超时拖成首字节超长卡顿。
//
// 返回 (responseID, ok)。ok=false 表示本次未拿到 id，调用方应降级继续。
func (s *OpenAIGatewayService) ensureOpenAIPrewarmSessionForRequest(
	ctx context.Context,
	groupIDs []int64,
	account *Account,
	model string,
) (string, bool) {
	if s == nil || account == nil {
		return "", false
	}
	callCtx := ctx
	if budget := s.openAIPrewarmSessionSyncBudget(); budget > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	id, err := s.performOpenAIWSPrewarmSession(callCtx, groupIDs, account, model)
	if err != nil {
		return "", false
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return "", false
	}
	return id, true
}

// openAIPrewarmPoolMaxTargetDepth 是 worker 预填池深度的硬上限。
// prewarm id 池在有流量时主要靠续接成功(roll-update)回收维持，worker 只需提供一个"冷启动底库"。
// 若让 worker 按账号并发(可能高达 80)去预铸，会在空闲/冷启动时产生大量 WS 握手风暴
// (例如 5 个账号 ×80 = 400 次握手)，把单代理出口 IP 打爆并拖慢真实请求。
// 故把 worker 目标深度封顶在该值，超出部分由运行期 roll-update 自然补充。
const openAIPrewarmPoolMaxTargetDepth = 16

// openAIPrewarmPoolTargetDepth 返回某账号 prewarm id 池的 worker 预填目标深度。
// = min(账号并发, 上限)，下限 1；运行期池可借 roll-update 回收超过该值。
func openAIPrewarmPoolTargetDepth(account *Account) int {
	if account == nil {
		return 1
	}
	t := account.Concurrency
	if t < 1 {
		t = 1
	}
	if t > openAIPrewarmPoolMaxTargetDepth {
		t = openAIPrewarmPoolMaxTargetDepth
	}
	return t
}

// openAIPrewarmPoolMaxDepth 返回池(LIST)的硬上限。
// 注意：这是运行期 roll-update 回收能填到的上限，不是 worker 预填目标。
// 取账号并发上限：稳态有流量时，每个完成的请求把滚动出的新 id 压回池，池可自然涨到接近并发数，
// 从而让高并发(如 80)也大多命中池、避免每请求现铸；worker 只负责冷启动预填一个较小的底库。
func openAIPrewarmPoolMaxDepth(account *Account) int {
	if account == nil {
		return 4
	}
	c := account.Concurrency
	if c < 4 {
		c = 4
	}
	return c
}

// refillOpenAIPrewarmPool 把 (account, model) 的 id 池补足到目标深度：
// 铸造 (target-当前深度) 个独立 id 并压入池。worker 周期调用。
func (s *OpenAIGatewayService) refillOpenAIPrewarmPool(ctx context.Context, account *Account, model string) {
	if s == nil || account == nil {
		return
	}
	store := s.getOpenAIPrewarmSessionStore()
	if store == nil {
		return
	}
	normalizedModel := normalizeOpenAIPrewarmModelKey(model)
	if normalizedModel == "" {
		return
	}
	target := openAIPrewarmPoolTargetDepth(account)
	curLen, err := store.PrewarmSessionPoolLen(ctx, account.ID, normalizedModel)
	if err != nil {
		return
	}
	need := target - curLen
	if need <= 0 {
		return
	}
	ttl := s.openAIPrewarmSessionTTL()
	maxDepth := openAIPrewarmPoolMaxDepth(account)
	for i := 0; i < need; i++ {
		if ctx.Err() != nil {
			return
		}
		id, mintErr := s.performOpenAIWSPrewarmSession(ctx, account.GroupIDs, account, normalizedModel)
		if mintErr != nil || strings.TrimSpace(id) == "" {
			return
		}
		if pushErr := store.PushPrewarmSessionPool(ctx, account.ID, normalizedModel, id, maxDepth, ttl); pushErr != nil {
			return
		}
	}
}

// recycleOpenAIPrewarmPoolID 在续接成功后把滚动出的新 id 压回池，供后续请求复用（稳态自维持）。
func (s *OpenAIGatewayService) recycleOpenAIPrewarmPoolID(ctx context.Context, account *Account, model, responseID string) {
	if s == nil || account == nil {
		return
	}
	store := s.getOpenAIPrewarmSessionStore()
	if store == nil {
		return
	}
	_ = store.PushPrewarmSessionPool(ctx, account.ID, model, responseID, openAIPrewarmPoolMaxDepth(account), s.openAIPrewarmSessionTTL())
}

// resolveOpenAIPrewarmToken 解析账号的上游 token（OAuth 优先 TokenProvider 缓存）。
func (s *OpenAIGatewayService) resolveOpenAIPrewarmToken(ctx context.Context, account *Account) (string, error) {
	if account == nil {
		return "", errors.New("account is nil")
	}
	if account.Type == AccountTypeOAuth && s.openAITokenProvider != nil {
		token, err := s.openAITokenProvider.GetAccessToken(ctx, account)
		if err != nil {
			return "", err
		}
		return token, nil
	}
	token := account.GetOpenAIAccessToken()
	if token == "" {
		return "", errors.New("prewarm session: empty access token")
	}
	return token, nil
}

// buildOpenAIPrewarmSessionPayload 构造空预热轮的 response.create payload。
// 关键字段：
//   - generate:false —— 不生成模型输出，只建立 response 拿 id；
//   - input:[] —— 空 input，避免污染上下文；
//   - instructions: 单空格 —— 满足上游非空约束；
//   - store:false —— OAuth 硬约束（CODEX_FLOW.md 第四节 Step 3）。
func (s *OpenAIGatewayService) buildOpenAIPrewarmSessionPayload(normalizedModel string) map[string]any {
	payload := map[string]any{
		"type":         "response.create",
		"model":        normalizedModel,
		"input":        []any{},
		"instructions": openAIPrewarmSessionInstructions,
		"stream":       true,
		"store":        false,
		"generate":     false,
	}
	return payload
}

// openAIPrewarmSessionTTL 返回 prewarm session 绑定的 TTL。
// 默认沿用 response_id 粘性 TTL（1h），可通过 prewarm_session_ttl_seconds 覆盖。
func (s *OpenAIGatewayService) openAIPrewarmSessionTTL() time.Duration {
	if s != nil && s.cfg != nil {
		if seconds := s.cfg.Gateway.OpenAIWS.PrewarmSessionTTLSeconds; seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return s.openAIWSResponseStickyTTL()
}

// tryGetOpenAIPrewarmSession 读取 (accountID, model) 的预热 response_id，
// 并通过 GetResponseAccount 反向验证它仍路由回本账号（防止跨账号串）。
// groupID 用于反向验证（response_id→account 绑定按 group 隔离）。
// 未命中 / 验证失败返回 ("", false)。
func (s *OpenAIGatewayService) tryGetOpenAIPrewarmSession(
	ctx context.Context,
	groupID int64,
	account *Account,
	model string,
) (string, bool) {
	if s == nil || account == nil {
		return "", false
	}
	prewarmStore := s.getOpenAIPrewarmSessionStore()
	stateStore := s.getOpenAIWSStateStore()
	if prewarmStore == nil || stateStore == nil {
		return "", false
	}
	normalizedModel := normalizeOpenAIPrewarmModelKey(model)
	if normalizedModel == "" {
		return "", false
	}
	// 从 id 池弹出一个独立 id（LPOP，原子）。prewarm id 一次性消费，每个并发请求拿到不同 id，
	// 从根本上杜绝复用导致的 previous_response_not_found。池空则未命中，调用方各自铸造独立 id。
	// 池内 id 可能有少量陈旧/跨账号项，最多重试几次反向校验后再放弃。
	for attempt := 0; attempt < 4; attempt++ {
		prewarmID, ok, err := prewarmStore.PopPrewarmSessionPool(ctx, account.ID, normalizedModel)
		if err != nil || !ok || strings.TrimSpace(prewarmID) == "" {
			return "", false
		}
		// 反向验证：该 response_id 必须仍路由回本账号。
		boundAccountID, verr := stateStore.GetResponseAccount(ctx, groupID, prewarmID)
		if verr != nil || boundAccountID != account.ID {
			// 陈旧/跨账号项：丢弃，继续取下一个。
			continue
		}
		return prewarmID, true
	}
	return "", false
}

// invalidateOpenAIPrewarmSession 删除 (accountID, model) 的预热绑定，
// 用于上游返回 previous_response_not_found 等失效信号时的自愈。
func (s *OpenAIGatewayService) invalidateOpenAIPrewarmSession(
	ctx context.Context,
	account *Account,
	model string,
) {
	if s == nil || account == nil {
		return
	}
	prewarmStore := s.getOpenAIPrewarmSessionStore()
	if prewarmStore == nil {
		return
	}
	normalizedModel := normalizeOpenAIPrewarmModelKey(model)
	if normalizedModel == "" {
		return
	}
	_ = prewarmStore.DeletePrewarmSession(ctx, account.ID, normalizedModel)
}

// ensureOpenAIPrewarmContinuationInput 把续接 payload 规整为「prewarm 续接」格式。
//
// 机制（对齐 OpenAI Responses API 官方格式）：
//   - prewarm 轮：input=[] + generate=false → 0 token → 不计 user 配额 → 拿 response_id
//   - 续发轮：previous_response_id=prewarm_id + 把用户 prompt 放进 instructions 字段，
//     input 留空数组（没有 user-role input → usage_limit 不统计 → 绕过限额）。
//
// 这样上游把「用户输入」当作 instructions 续接到 prewarm 那轮：
//   - 不触发 user 配额检查（无 user-role input）
//   - instructions 用用户的真实 prompt（而非 sub2api 注入的超长默认 Codex base prompt）
//   - 模型基于 instructions 正常生成回复
func ensureOpenAIPrewarmContinuationInput(payload map[string]any, model string) {
	if len(payload) == 0 {
		return
	}
	// 提取用户 prompt 文本（input 数组里的 user-role 内容，或 input 是字符串）。
	var userPromptText string
	switch input := payload["input"].(type) {
	case string:
		userPromptText = strings.TrimSpace(input)
	case []any:
		for _, item := range input {
			if role, ok := item.(map[string]any); ok {
				if r, _ := role["role"].(string); r == "user" {
					userPromptText = strings.TrimSpace(extractOpenAIWSInputItemText(role))
					break
				}
			}
		}
	}

	if userPromptText == "" {
		// 没有用户文本可提取，保持原样（input 不动）。
		return
	}

	// 把用户 prompt 放进 developer-role input（绕过 user 配额统计），instructions 设为最小非空。
	// developer-role 不计入 user-role 配额 → 绕过 usage_limit。
	// instructions 设为单空格（避免 codex transform 注入超长默认 Codex base prompt）。
	payload["instructions"] = openAIPrewarmSessionInstructions
	payload["input"] = []any{
		map[string]any{
			"type":    "message",
			"role":    "developer",
			"content": userPromptText,
		},
	}
}

// extractOpenAIWSInputItemText 从一个 input item（map）中提取纯文本内容。
func extractOpenAIWSInputItemText(item map[string]any) string {
	if item == nil {
		return ""
	}
	switch content := item["content"].(type) {
	case string:
		return content
	case []any:
		var sb strings.Builder
		for _, c := range content {
			if cm, ok := c.(map[string]any); ok {
				if t, _ := cm["text"].(string); t != "" {
					sb.WriteString(t)
				}
			}
		}
		return sb.String()
	}
	return ""
}

// sanitizeOpenAIPrewarmResponse 清洗 prewarm 续接的非流式 JSON 响应。
// 仅在「用户没传 previous_response_id，由 prewarm 注入」时调用：
//   - previous_response_id → 删除（官方响应里该字段为 null 或不存在）
//   - instructions → 删除（用户没传就不该出现，避免泄露内部空格占位）
func sanitizeOpenAIPrewarmResponse(finalResponse []byte) []byte {
	if len(finalResponse) == 0 {
		return finalResponse
	}
	// previous_response_id 设为 null（删除字段，让 gjson 查不到 = null 语义）
	if updated, err := sjson.DeleteBytes(finalResponse, "previous_response_id"); err == nil {
		finalResponse = updated
	}
	// instructions 删除（用户没传则不出现）
	if updated, err := sjson.DeleteBytes(finalResponse, "instructions"); err == nil {
		finalResponse = updated
	}
	return finalResponse
}

// isOpenAIWSNonStandardCodexEvent 判断是否为 Codex 内部非官方事件（不应透传给客户端）。
// 官方 Responses API stream 事件类型以 "response." 开头，codex.rate_limits 是 ChatGPT 内部事件。
func isOpenAIWSNonStandardCodexEvent(eventType string) bool {
	switch eventType {
	case "codex.rate_limits":
		return true
	}
	return false
}

// sanitizeOpenAIPrewarmStreamEvent 清洗 prewarm 续接的流式 SSE 事件。
func sanitizeOpenAIPrewarmStreamEvent(message []byte) []byte {
	if len(message) == 0 {
		return message
	}
	// 清洗 response.previous_response_id
	if updated, err := sjson.DeleteBytes(message, "response.previous_response_id"); err == nil {
		message = updated
	}
	// 清洗 response.instructions
	if updated, err := sjson.DeleteBytes(message, "response.instructions"); err == nil {
		message = updated
	}
	return message
}
