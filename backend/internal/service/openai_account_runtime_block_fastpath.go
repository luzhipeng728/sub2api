package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	openAIAccountStateUpdateTimeout       = 5 * time.Second
	openAIStopSchedulingBridgeCooldown    = 2 * time.Minute
	openAIOAuth429StormWindow             = 10 * time.Second
	openAIOAuth429StormThreshold          = 20
	openAIOAuth429StormMaxAccountSwitches = 40
	// openAIWSDialFailoverCooldown: WS 握手返回账号级错误(401/403/5xx)时对该账号的冷却时长。
	// 让坏账号(如 Cloudflare 拒绝握手 403)被短暂剔除调度并把请求 failover 到健康账号。
	openAIWSDialFailoverCooldown = 60 * time.Second
)

func openAIAccountStateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(base, openAIAccountStateUpdateTimeout)
}

func isOpenAIOAuthAccount(account *Account) bool {
	return account != nil && account.Platform == PlatformOpenAI && account.Type == AccountTypeOAuth
}

func isOpenAIAccount(account *Account) bool {
	return account != nil && account.Platform == PlatformOpenAI
}

func (s *OpenAIGatewayService) handleOpenAIAccountUpstreamError(ctx context.Context, account *Account, statusCode int, headers http.Header, responseBody []byte, requestedModel ...string) bool {
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()

	if isOpenAIImageRateLimitError(statusCode, responseBody) {
		if s != nil && s.rateLimitService != nil {
			_ = s.rateLimitService.HandleOpenAIImageRateLimit(stateCtx, account, statusCode, headers, responseBody)
		}
		return false
	}

	if statusCode == http.StatusTooManyRequests {
		if isOpenAIOAuthAccount(account) {
			s.observeOpenAIOAuth429(stateCtx, account, headers, responseBody)
			return false
		}
	}
	// prewarm session 启用时，5h/7d 等 429 不作为账号不可调度条件；
	// 401/403 是账号/边缘权限类错误，继续交给 RateLimitService 做临时不可调度或禁用。
	if s != nil && s.isOpenAIPrewarmSessionEnabled() {
		switch statusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			// fall through
		default:
			return false
		}
	}
	if s == nil || account == nil || s.rateLimitService == nil {
		return false
	}
	if len(requestedModel) > 0 && s.rateLimitService.HandleUpstreamModelNotFound(stateCtx, account, requestedModel[0], statusCode, responseBody) {
		return true
	}
	shouldDisable := s.rateLimitService.HandleUpstreamError(stateCtx, account, statusCode, headers, responseBody)
	if shouldDisable {
		s.BlockAccountScheduling(account, time.Time{}, "upstream_disable")
	}
	return shouldDisable
}

func (s *OpenAIGatewayService) observeOpenAIOAuth429(ctx context.Context, account *Account, headers http.Header, responseBody []byte) {
	if s == nil || !isOpenAIOAuthAccount(account) {
		return
	}
	s.recordOpenAIOAuth429()
	if s.rateLimitService == nil || s.rateLimitService.accountRepo == nil {
		return
	}
	persistOpenAI429PlanType(ctx, s.rateLimitService.accountRepo, account, responseBody)
	s.rateLimitService.persistOpenAICodexSnapshot(ctx, account, headers)
}

func (s *OpenAIGatewayService) handleOpenAIWSDialAccountFailure(ctx context.Context, account *Account, statusCode int, cause string) {
	if s == nil || !isOpenAIAccount(account) {
		return
	}
	reason := fmt.Sprintf("ws_dial_%d", statusCode)
	if statusCode != http.StatusUnauthorized {
		s.BlockAccountScheduling(account, time.Now().Add(openAIWSDialFailoverCooldown), reason)
		return
	}

	s.BlockAccountScheduling(account, time.Time{}, reason)
	if s.accountRepo == nil {
		return
	}
	errorMsg := "WS dial 401: account authentication failed"
	if trimmed := strings.TrimSpace(cause); trimmed != "" {
		errorMsg = "WS dial 401: " + trimmed
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	if err := setAccountDisabledOrError(stateCtx, s.accountRepo, account.ID, errorMsg); err != nil {
		slog.Warn("openai_ws_dial_401_set_disabled_failed", "account_id", account.ID, "error", err)
		return
	}
	slog.Warn("openai_ws_dial_401_account_disabled", "account_id", account.ID)
}

func (s *OpenAIGatewayService) BlockAccountScheduling(account *Account, until time.Time, reason string) {
	if s == nil || !isOpenAIAccount(account) {
		return
	}
	// prewarm session 启用时仍跳过传输类抖动，但保留会影响调度正确性的账号级错误：
	// - 429 仅统计/记录使用状态，不进入 runtime block，让续接/新锚点持续尝试。
	// - 401 使用已有禁用逻辑，坏凭证不会继续抢流量。
	// - WS 握手 403/5xx 只做短 runtime block，不禁用账号；代理/边缘冷却时避免同账号反复抢流量。
	if s.isOpenAIPrewarmSessionEnabled() {
		if !isOpenAIPrewarmRuntimeBlockReasonAllowed(reason) {
			return
		}
	}
	now := time.Now()
	blockUntil := until
	if blockUntil.IsZero() || !blockUntil.After(now) {
		blockUntil = now.Add(openAIStopSchedulingBridgeCooldown)
	}

	for {
		current, loaded := s.openaiAccountRuntimeBlockUntil.Load(account.ID)
		if !loaded {
			actual, stored := s.openaiAccountRuntimeBlockUntil.LoadOrStore(account.ID, blockUntil)
			if !stored {
				return
			}
			current = actual
		}

		currentUntil, ok := current.(time.Time)
		if !ok || currentUntil.IsZero() {
			if s.openaiAccountRuntimeBlockUntil.CompareAndSwap(account.ID, current, blockUntil) {
				return
			}
			continue
		}
		if currentUntil.After(blockUntil) {
			return
		}
		if s.openaiAccountRuntimeBlockUntil.CompareAndSwap(account.ID, current, blockUntil) {
			return
		}
	}
}

func isOpenAIPrewarmRuntimeBlockReasonAllowed(reason string) bool {
	switch reason {
	case "openai_403_temp", "oauth_401",
		"missing_refresh_token", "token_refresh_failed", "token_refresh_non_retryable",
		"token_refresh_retry_exhausted", "auth_failed", "auth_error", "upstream_disable",
		"ws_dial_401":
		return true
	default:
		return strings.HasPrefix(reason, "ws_dial_")
	}
}

func (s *OpenAIGatewayService) ClearAccountSchedulingBlock(accountID int64) {
	if s == nil || accountID <= 0 {
		return
	}
	s.openaiAccountRuntimeBlockUntil.Delete(accountID)
}

func (s *OpenAIGatewayService) isOpenAIAccountRuntimeBlocked(account *Account) bool {
	if s == nil || !isOpenAIAccount(account) {
		return false
	}
	if s.isOpenAIWSProxyRuntimeBlocked(account) {
		return true
	}
	value, ok := s.openaiAccountRuntimeBlockUntil.Load(account.ID)
	if !ok {
		return false
	}
	cooldownUntil, ok := value.(time.Time)
	if !ok || cooldownUntil.IsZero() {
		s.openaiAccountRuntimeBlockUntil.Delete(account.ID)
		return false
	}
	if time.Now().Before(cooldownUntil) {
		return true
	}
	s.openaiAccountRuntimeBlockUntil.Delete(account.ID)
	return false
}

func (s *OpenAIGatewayService) isOpenAIWSProxyRuntimeBlocked(account *Account) bool {
	if s == nil || account == nil || account.Proxy == nil {
		return false
	}
	proxyURL := strings.TrimSpace(account.Proxy.URL())
	if proxyURL == "" {
		return false
	}
	pool := s.getOpenAIWSConnPool()
	if pool == nil {
		return false
	}
	return pool.proxyBreakerOpen(proxyURL)
}

func (s *OpenAIGatewayService) recordOpenAIOAuth429() {
	if s == nil {
		return
	}
	now := time.Now()
	windowStart := s.openaiOAuth429WindowStartUnixNano.Load()
	if windowStart == 0 || now.Sub(time.Unix(0, windowStart)) >= openAIOAuth429StormWindow {
		if s.openaiOAuth429WindowStartUnixNano.CompareAndSwap(windowStart, now.UnixNano()) {
			s.openaiOAuth429WindowCount.Store(1)
			return
		}
	}
	s.openaiOAuth429WindowCount.Add(1)
}

func (s *OpenAIGatewayService) isOpenAIOAuth429Storm() bool {
	if s == nil {
		return false
	}
	windowStart := s.openaiOAuth429WindowStartUnixNano.Load()
	if windowStart == 0 || time.Since(time.Unix(0, windowStart)) >= openAIOAuth429StormWindow {
		return false
	}
	return s.openaiOAuth429WindowCount.Load() >= openAIOAuth429StormThreshold
}

func (s *OpenAIGatewayService) ShouldStopOpenAIOAuth429Failover(account *Account, statusCode int, failedSwitches int) bool {
	if statusCode != http.StatusTooManyRequests || failedSwitches < openAIOAuth429StormMaxAccountSwitches {
		return false
	}
	if !isOpenAIOAuthAccount(account) {
		return false
	}
	return s.isOpenAIOAuth429Storm()
}
