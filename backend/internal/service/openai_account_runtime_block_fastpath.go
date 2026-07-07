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
	openAIOAuth429StormStopFailedSwitches = 1
	openAIOAuth429SoftLockMin             = 10 * time.Second
	openAIOAuth429SoftLockMax             = 60 * time.Second
	openAIOAuth429SoftLockMidPoolSize     = 500
	openAIOAuth429SoftLockFullPoolSize    = 1000
	// 账号级重复 429 升级锁:单次 429 先按可用池规模短软锁(最多 60s);同账号跨短锁仍重复 429,
	// 说明它大概率已到真实 5h/7d usage limit。累计达 Threshold 次 → 软锁 Lock 时长,
	// 期间该账号不参与调度(runtime block),让流量集中到仍可用账号。
	openAIOAuth429StreakThreshold = 3
	openAIOAuth429StreakGap       = 5 * time.Minute
	openAIOAuth429StreakLock      = 20 * time.Minute
	// openAIWSDialFailoverCooldown: WS 握手返回账号级错误(401/403/5xx)时对该账号的冷却时长。
	// 让坏账号(如 Cloudflare 拒绝握手 403)被短暂剔除调度并把请求 failover 到健康账号。
	openAIWSDialFailoverCooldown = 60 * time.Second
)

// OpenAIAccountRuntimeBlockCache is an optional GatewayCache extension used to
// survive process restarts without probing recently blocked accounts again.
type OpenAIAccountRuntimeBlockCache interface {
	SetOpenAIAccountRuntimeBlock(ctx context.Context, accountID int64, until time.Time, ttl time.Duration) error
	ListOpenAIAccountRuntimeBlocks(ctx context.Context) (map[int64]time.Time, error)
	DeleteOpenAIAccountRuntimeBlock(ctx context.Context, accountID int64) error
}

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
	// prewarm session 启用时，未被上方 OAuth 429 fast-path 处理的 5h/7d 等 429
	// 不再交给 RateLimitService 做持久不可调度；401/403 继续按账号/边缘权限类错误处理。
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
	s.softLockOpenAIOAuth429Account(ctx, account)
	s.bumpOpenAIOAuth429Streak(account)
	if s.rateLimitService == nil || s.rateLimitService.accountRepo == nil {
		return
	}
	persistOpenAI429PlanType(ctx, s.rateLimitService.accountRepo, account, responseBody)
	s.rateLimitService.persistOpenAICodexSnapshot(ctx, account, headers)
}

func openAIOAuth429SoftLockDurationForAvailableAccounts(availableAccounts int) time.Duration {
	if availableAccounts <= 100 {
		return openAIOAuth429SoftLockMin
	}
	if availableAccounts >= openAIOAuth429SoftLockFullPoolSize {
		return openAIOAuth429SoftLockMax
	}
	if availableAccounts <= openAIOAuth429SoftLockMidPoolSize {
		span := 30*time.Second - openAIOAuth429SoftLockMin
		return openAIOAuth429SoftLockMin + time.Duration(availableAccounts-100)*span/time.Duration(openAIOAuth429SoftLockMidPoolSize-100)
	}
	span := openAIOAuth429SoftLockMax - 30*time.Second
	return 30*time.Second + time.Duration(availableAccounts-openAIOAuth429SoftLockMidPoolSize)*span/time.Duration(openAIOAuth429SoftLockFullPoolSize-openAIOAuth429SoftLockMidPoolSize)
}

func (s *OpenAIGatewayService) softLockOpenAIOAuth429Account(ctx context.Context, account *Account) {
	if s == nil || !isOpenAIOAuthAccount(account) {
		return
	}
	availableAccounts := s.countOpenAIOAuth429SoftLockAvailableAccounts(ctx, account)
	duration := openAIOAuth429SoftLockDurationForAvailableAccounts(availableAccounts)
	s.BlockAccountScheduling(account, time.Now().Add(duration), "oauth_429_soft_lock")
	logOpenAIWSModeInfo("oauth_429_soft_lock account_id=%d available_accounts=%d lock_sec=%d", account.ID, availableAccounts, int(duration.Seconds()))
}

func (s *OpenAIGatewayService) countOpenAIOAuth429SoftLockAvailableAccounts(ctx context.Context, account *Account) int {
	if s == nil || account == nil {
		return 0
	}
	if s.schedulerSnapshot == nil && s.accountRepo == nil {
		return 0
	}
	var groupID *int64
	if len(account.GroupIDs) > 0 {
		id := account.GroupIDs[0]
		groupID = &id
	}
	accounts, err := s.listSchedulableAccounts(ctx, groupID)
	if err != nil {
		return 0
	}
	count := 0
	for i := range accounts {
		acc := &accounts[i]
		if !acc.IsOpenAI() || !acc.IsSchedulable() {
			continue
		}
		if s.isOpenAIAccountRuntimeBlocked(acc) {
			continue
		}
		count++
	}
	return count
}

type oauth429StreakState struct {
	count  int
	lastAt time.Time
}

// bumpOpenAIOAuth429Streak 累计账号连续 429;两次间隔 > Gap 视为断连并重置。
// 达阈值即对该账号软锁 openAIOAuth429StreakLock,期间不参与调度。
func (s *OpenAIGatewayService) bumpOpenAIOAuth429Streak(account *Account) {
	if s == nil || account == nil {
		return
	}
	now := time.Now()
	v, _ := s.openaiAccount429Streak.LoadOrStore(account.ID, &oauth429StreakState{})
	st := v.(*oauth429StreakState)
	s.openaiAccount429StreakMu.Lock()
	if !st.lastAt.IsZero() && now.Sub(st.lastAt) > openAIOAuth429StreakGap {
		st.count = 0
	}
	st.count++
	st.lastAt = now
	shouldLock := st.count >= openAIOAuth429StreakThreshold
	if shouldLock {
		st.count = 0
	}
	s.openaiAccount429StreakMu.Unlock()
	if shouldLock {
		s.BlockAccountScheduling(account, now.Add(openAIOAuth429StreakLock), "oauth_429_streak")
		logOpenAIWSModeInfo("oauth_429_streak_lock account_id=%d threshold=%d lock_min=%d", account.ID, openAIOAuth429StreakThreshold, int(openAIOAuth429StreakLock.Minutes()))
	}
}

// resetOpenAIOAuth429Streak 账号成功产出 → 清零连续 429 计数(打断误锁)。
func (s *OpenAIGatewayService) resetOpenAIOAuth429Streak(accountID int64) {
	if s == nil || accountID <= 0 {
		return
	}
	if v, ok := s.openaiAccount429Streak.Load(accountID); ok {
		st := v.(*oauth429StreakState)
		s.openaiAccount429StreakMu.Lock()
		st.count = 0
		st.lastAt = time.Time{}
		s.openaiAccount429StreakMu.Unlock()
	}
}

func (s *OpenAIGatewayService) handleOpenAIWSDialAccountFailure(ctx context.Context, account *Account, statusCode int, cause string) {
	if s == nil || !isOpenAIAccount(account) {
		return
	}
	reason := fmt.Sprintf("ws_dial_%d", statusCode)
	// 401(token 失效)/402(无有效订阅) 是账号级死号:握手已到 OpenAI、账号本身不可用,
	// 永久禁用避免持续空转。403(Cloudflare 边缘拦截)/5xx 不是账号的错,只做短冷却。
	if statusCode != http.StatusUnauthorized && statusCode != http.StatusPaymentRequired {
		s.BlockAccountScheduling(account, time.Now().Add(openAIWSDialFailoverCooldown), reason)
		return
	}

	s.BlockAccountScheduling(account, time.Time{}, reason)
	if s.accountRepo == nil {
		return
	}
	errorMsg := fmt.Sprintf("WS dial %d: account unavailable (dead credential/subscription)", statusCode)
	if trimmed := strings.TrimSpace(cause); trimmed != "" {
		errorMsg = fmt.Sprintf("WS dial %d: %s", statusCode, trimmed)
	}
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	if err := setAccountDisabledOrError(stateCtx, s.accountRepo, account.ID, errorMsg); err != nil {
		slog.Warn("openai_ws_dial_account_disable_failed", "account_id", account.ID, "status_code", statusCode, "error", err)
		return
	}
	slog.Warn("openai_ws_dial_account_disabled", "account_id", account.ID, "status_code", statusCode)
}

func (s *OpenAIGatewayService) BlockAccountScheduling(account *Account, until time.Time, reason string) {
	if s == nil || !isOpenAIAccount(account) {
		return
	}
	// prewarm session 启用时仍跳过传输类抖动，但保留会影响调度正确性的账号级错误：
	// - 单次 429 进入短 runtime soft lock，避免下一次选号继续打刚失败的账号。
	// - 连续 429 streak 升级为更长 soft lock，避免 429 风暴继续打满 failover。
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

	shouldEvictIdleWS := false
	persistUntil := time.Time{}
	for {
		current, loaded := s.openaiAccountRuntimeBlockUntil.Load(account.ID)
		if !loaded {
			actual, stored := s.openaiAccountRuntimeBlockUntil.LoadOrStore(account.ID, blockUntil)
			if !stored {
				shouldEvictIdleWS = true
				persistUntil = blockUntil
				break
			}
			current = actual
		}

		currentUntil, ok := current.(time.Time)
		if !ok || currentUntil.IsZero() {
			if s.openaiAccountRuntimeBlockUntil.CompareAndSwap(account.ID, current, blockUntil) {
				shouldEvictIdleWS = true
				break
			}
			continue
		}
		if currentUntil.After(blockUntil) {
			shouldEvictIdleWS = true
			break
		}
		if s.openaiAccountRuntimeBlockUntil.CompareAndSwap(account.ID, current, blockUntil) {
			shouldEvictIdleWS = true
			persistUntil = blockUntil
			break
		}
	}
	if !persistUntil.IsZero() {
		s.persistOpenAIAccountRuntimeBlock(account.ID, persistUntil)
	}
	if shouldEvictIdleWS {
		s.evictOpenAIWSAccountIdleConns(account.ID)
	}
}

func (s *OpenAIGatewayService) evictOpenAIWSAccountIdleConns(accountID int64) {
	if s == nil || accountID <= 0 || s.openaiWSPool == nil {
		return
	}
	s.openaiWSPool.evictIdleConnsForAccount(accountID)
}

func isOpenAIPrewarmRuntimeBlockReasonAllowed(reason string) bool {
	switch reason {
	case "openai_403_temp", "oauth_401", "oauth_429_soft_lock", "oauth_429_streak",
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
	s.deleteOpenAIAccountRuntimeBlock(accountID)
}

func (s *OpenAIGatewayService) isOpenAIAccountRuntimeBlocked(account *Account) bool {
	if s == nil || !isOpenAIAccount(account) {
		return false
	}
	if s.isOpenAIWSProxyRuntimeBlocked(account) {
		return true
	}
	s.hydrateOpenAIAccountRuntimeBlocks()
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
	s.deleteOpenAIAccountRuntimeBlock(account.ID)
	return false
}

func (s *OpenAIGatewayService) openAIAccountRuntimeBlockCache() OpenAIAccountRuntimeBlockCache {
	if s == nil || s.cache == nil {
		return nil
	}
	cache, _ := s.cache.(OpenAIAccountRuntimeBlockCache)
	return cache
}

func (s *OpenAIGatewayService) hydrateOpenAIAccountRuntimeBlocks() {
	if s == nil {
		return
	}
	s.openaiAccountRuntimeBlockHydrateOnce.Do(func() {
		cache := s.openAIAccountRuntimeBlockCache()
		if cache == nil {
			return
		}
		ctx, cancel := openAIAccountStateContext(context.Background())
		defer cancel()
		blocks, err := cache.ListOpenAIAccountRuntimeBlocks(ctx)
		if err != nil {
			slog.Warn("openai_runtime_block_hydrate_failed", "error", err)
			return
		}
		now := time.Now()
		for accountID, until := range blocks {
			if accountID <= 0 || !until.After(now) {
				continue
			}
			s.openaiAccountRuntimeBlockUntil.Store(accountID, until)
		}
	})
}

func (s *OpenAIGatewayService) persistOpenAIAccountRuntimeBlock(accountID int64, until time.Time) {
	if s == nil || accountID <= 0 || !until.After(time.Now()) {
		return
	}
	cache := s.openAIAccountRuntimeBlockCache()
	if cache == nil {
		return
	}
	ttl := time.Until(until)
	if ttl <= 0 {
		return
	}
	ctx, cancel := openAIAccountStateContext(context.Background())
	defer cancel()
	if err := cache.SetOpenAIAccountRuntimeBlock(ctx, accountID, until, ttl); err != nil {
		slog.Warn("openai_runtime_block_persist_failed", "account_id", accountID, "error", err)
	}
}

func (s *OpenAIGatewayService) deleteOpenAIAccountRuntimeBlock(accountID int64) {
	if s == nil || accountID <= 0 {
		return
	}
	cache := s.openAIAccountRuntimeBlockCache()
	if cache == nil {
		return
	}
	ctx, cancel := openAIAccountStateContext(context.Background())
	defer cancel()
	if err := cache.DeleteOpenAIAccountRuntimeBlock(ctx, accountID); err != nil {
		slog.Warn("openai_runtime_block_delete_failed", "account_id", accountID, "error", err)
	}
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
	if statusCode != http.StatusTooManyRequests || failedSwitches < openAIOAuth429StormStopFailedSwitches {
		return false
	}
	if !isOpenAIOAuthAccount(account) {
		return false
	}
	return s.isOpenAIOAuth429Storm()
}
