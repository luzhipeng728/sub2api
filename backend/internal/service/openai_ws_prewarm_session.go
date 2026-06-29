package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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

	// 双向绑定：response_id → account（按每个 group 绑定，供续接路由）+ (account,model) → response_id（供注入）。
	ttl := s.openAIPrewarmSessionTTL()
	for _, gid := range effectiveOpenAIPrewarmGroupIDs(groupIDs) {
		logOpenAIWSBindResponseAccountWarn(gid, account.ID, prewarmResponseID, stateStore.BindResponseAccount(ctx, gid, prewarmResponseID, account.ID, ttl))
	}
	stateStore.BindResponseConn(prewarmResponseID, lease.ConnID(), ttl)
	if err := prewarmStore.SetPrewarmSession(ctx, account.ID, normalizedModel, prewarmResponseID, ttl); err != nil {
		logOpenAIWSModeInfo(
			"prewarm_session_persist_fail account_id=%d conn_id=%s model=%s response_id=%s cause=%s",
			account.ID, connID, normalizedModel,
			truncateOpenAIWSLogValue(prewarmResponseID, openAIWSIDValueMaxLen),
			truncateOpenAIWSLogValue(err.Error(), openAIWSLogValueMaxLen),
		)
		return "", err
	}

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
	prewarmID, ok, err := prewarmStore.GetPrewarmSession(ctx, account.ID, normalizedModel)
	if err != nil || !ok || strings.TrimSpace(prewarmID) == "" {
		return "", false
	}
	// 反向验证：该 response_id 必须仍路由回本账号。
	boundAccountID, err := stateStore.GetResponseAccount(ctx, groupID, prewarmID)
	if err != nil || boundAccountID != account.ID {
		// 绑定陈旧/跨账号串，清理掉避免后续重复命中。
		_ = prewarmStore.DeletePrewarmSession(ctx, account.ID, normalizedModel)
		return "", false
	}
	return prewarmID, true
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
//   - 续发轮：previous_response_id=prewarm_id + 把用户 prompt 放进 developer-role input
//     → 上游当作「续接进行中的 response」，usage_limit 只统计 user-role 全新请求，
//     developer-role 不计入 user 配额 → 绕过限额照常生成
//
// 因此本函数把 payload 里的 user-role 内容转成 developer-role：
//  1. 移除原有 system/developer 项（避免重复）
//  2. 把首个 user 项的 content 转成 developer-role 项
//  3. 保留其余 user/assistant 项（多轮工具调用等结构化内容需要保留）
//
// 这样上游把「用户输入」当作开发者指令续接到 prewarm 那轮，既不触发 user 配额检查，
// 又能正常生成。
func ensureOpenAIPrewarmContinuationInput(payload map[string]any, model string) {
	if len(payload) == 0 {
		return
	}
	input, ok := payload["input"].([]any)
	if !ok {
		// input 是字符串（等价 user-role），转成 developer-role message。
		if text, ok := payload["input"].(string); ok && strings.TrimSpace(text) != "" {
			payload["input"] = []any{
				map[string]any{
					"type":    "message",
					"role":    "developer",
					"content": text,
				},
			}
		}
		return
	}
	if len(input) == 0 {
		return
	}

	// 收集首个 user 项的文本内容 + 过滤掉原有 system/developer 项。
	var userPromptText string
	filtered := make([]any, 0, len(input))
	for _, item := range input {
		role, _ := item.(map[string]any)
		if role != nil {
			r, _ := role["role"].(string)
			if r == "system" || r == "developer" {
				// 丢弃原有系统/开发者指令（prewarm 轮已承担基础指令占位）。
				continue
			}
			if r == "user" && userPromptText == "" {
				// 提取首个 user 项的文本内容，稍后转成 developer-role。
				userPromptText = extractOpenAIWSInputItemText(role)
				continue
			}
		}
		filtered = append(filtered, item)
	}

	if strings.TrimSpace(userPromptText) == "" {
		// 没有可转换的 user 文本，直接用过滤结果（移除了 system/developer）。
		if len(filtered) != len(input) {
			payload["input"] = filtered
		}
		return
	}

	// 在最前面插入 developer-role 项（用户 prompt 转换而来），
	// 后面跟保留的其余 user/assistant/工具项。
	finalInput := make([]any, 0, len(filtered)+1)
	finalInput = append(finalInput, map[string]any{
		"type":    "message",
		"role":    "developer",
		"content": userPromptText,
	})
	finalInput = append(finalInput, filtered...)
	payload["input"] = finalInput
	// prewarm 续接时覆盖 instructions 为最小非空值（单空格）：
	// codex transform 在此之前可能已注入超长默认 Codex base prompt（伪装官方客户端），
	// 但 prewarm 续接场景下用户的 prompt 已转成 developer-role，不需要 Codex 人设指令，
	// 超长 instructions 反而让模型困惑（扮演 Codex 但无真实编码任务 → 空输出）。
	// 设为非空让模型只关注 developer-role 的用户 prompt。
	payload["instructions"] = openAIPrewarmSessionInstructions
}

// extractOpenAIWSInputItemText 从一个 input item（map）中提取纯文本内容。
// content 可能是 string，也可能是 [{type:"input_text", text:"..."}] 列表。
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
