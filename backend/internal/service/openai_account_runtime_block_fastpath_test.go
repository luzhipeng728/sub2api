//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestOpenAI429FastPath_RuntimeSoftBlocksOAuthAccount(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 42, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	apiKeyAccount := &Account{ID: 43, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	shouldDisable := svc.handleOpenAIAccountUpstreamError(context.Background(), account, http.StatusTooManyRequests, http.Header{}, nil)
	apiKeyShouldDisable := svc.handleOpenAIAccountUpstreamError(context.Background(), apiKeyAccount, http.StatusTooManyRequests, http.Header{}, nil)

	require.False(t, shouldDisable)
	require.False(t, apiKeyShouldDisable)
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(apiKeyAccount))
}

func TestOpenAI429FastPath_PrewarmRuntimeSoftBlocksOAuthAccount(t *testing.T) {
	svc := &OpenAIGatewayService{
		cfg: &config.Config{
			Gateway: config.GatewayConfig{
				OpenAIWS: config.GatewayOpenAIWSConfig{PrewarmSessionEnabled: true},
			},
		},
	}
	svc.SetOpenAIPrewarmSessionCache(newFakePrewarmSessionCache())
	account := &Account{ID: 49, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	shouldDisable := svc.handleOpenAIAccountUpstreamError(
		context.Background(),
		account,
		http.StatusTooManyRequests,
		http.Header{},
		[]byte(`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":60}}`),
	)

	require.False(t, shouldDisable)
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAI429SoftLockDuration_ScalesWithAvailableAccounts(t *testing.T) {
	require.Equal(t, 10*time.Second, openAIOAuth429SoftLockDurationForAvailableAccounts(0))
	require.Equal(t, 10*time.Second, openAIOAuth429SoftLockDurationForAvailableAccounts(100))
	require.Equal(t, 15*time.Second, openAIOAuth429SoftLockDurationForAvailableAccounts(200))
	require.Equal(t, 30*time.Second, openAIOAuth429SoftLockDurationForAvailableAccounts(500))
	require.Equal(t, 45*time.Second, openAIOAuth429SoftLockDurationForAvailableAccounts(750))
	require.Equal(t, 60*time.Second, openAIOAuth429SoftLockDurationForAvailableAccounts(1000))
	require.Equal(t, 60*time.Second, openAIOAuth429SoftLockDurationForAvailableAccounts(1500))
}

func TestOpenAI429Streak_PrewarmRuntimeBlocksOAuthAccount(t *testing.T) {
	svc := &OpenAIGatewayService{
		cfg: &config.Config{
			Gateway: config.GatewayConfig{
				OpenAIWS: config.GatewayOpenAIWSConfig{PrewarmSessionEnabled: true},
			},
		},
	}
	svc.SetOpenAIPrewarmSessionCache(newFakePrewarmSessionCache())
	account := &Account{ID: 55, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	for i := 0; i < openAIOAuth429StreakThreshold; i++ {
		shouldDisable := svc.handleOpenAIAccountUpstreamError(
			context.Background(),
			account,
			http.StatusTooManyRequests,
			http.Header{},
			[]byte(`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":60}}`),
		)
		require.False(t, shouldDisable)
	}

	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAI429Streak_AccumulatesAcrossSoftLockCooldown(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 56, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	svc.openaiAccount429Streak.Store(account.ID, &oauth429StreakState{
		count:  openAIOAuth429StreakThreshold - 1,
		lastAt: time.Now().Add(-openAIOAuth429SoftLockMax - 5*time.Second),
	})

	svc.handleOpenAIAccountUpstreamError(
		context.Background(),
		account,
		http.StatusTooManyRequests,
		http.Header{},
		[]byte(`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached"}}`),
	)

	value, ok := svc.openaiAccountRuntimeBlockUntil.Load(account.ID)
	require.True(t, ok)
	until, ok := value.(time.Time)
	require.True(t, ok)
	require.GreaterOrEqual(t, time.Until(until), openAIOAuth429StreakLock-time.Second)
}

func TestOpenAI403FastPath_PrewarmStillTemporarilyBlocksOAuthAccount(t *testing.T) {
	repo := &rateLimitAccountRepoStub{}
	rateLimitService := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	rateLimitService.SetOpenAI403CounterCache(&openAI403CounterCacheStub{counts: []int64{1}})
	svc := &OpenAIGatewayService{
		cfg: &config.Config{
			Gateway: config.GatewayConfig{
				OpenAIWS: config.GatewayOpenAIWSConfig{PrewarmSessionEnabled: true},
			},
		},
		rateLimitService: rateLimitService,
	}
	svc.SetOpenAIPrewarmSessionCache(newFakePrewarmSessionCache())
	rateLimitService.SetAccountRuntimeBlocker(svc)
	account := &Account{ID: 50, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	shouldDisable := svc.handleOpenAIAccountUpstreamError(
		context.Background(),
		account,
		http.StatusForbidden,
		http.Header{},
		[]byte(`{"error":{"message":"temporary edge rejection"}}`),
	)

	require.True(t, shouldDisable)
	require.Equal(t, 1, repo.tempCalls)
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIRuntimeBlock_AppliesToOpenAIAPIKeyWhenRateLimitServiceStopsScheduling(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 44, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	svc.BlockAccountScheduling(account, time.Time{}, "custom_error_code")

	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIRuntimeBlock_EvictsIdleWSConnections(t *testing.T) {
	pool := newOpenAIWSConnPool(&config.Config{})
	defer pool.Close()
	svc := &OpenAIGatewayService{openaiWSPool: pool}
	account := &Account{ID: 47, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	ap := pool.getOrCreateAccountPool(account.ID)
	idle := newOpenAIWSConn("idle_runtime_block", account.ID, &openAIWSFakeConn{}, nil)
	leased := newOpenAIWSConn("leased_runtime_block", account.ID, &openAIWSFakeConn{}, nil)
	require.True(t, leased.tryAcquire())
	ap.mu.Lock()
	ap.conns[idle.id] = idle
	ap.conns[leased.id] = leased
	ap.mu.Unlock()

	svc.BlockAccountScheduling(account, time.Now().Add(time.Minute), "oauth_429_soft_lock")

	select {
	case <-idle.closedCh:
	default:
		t.Fatal("runtime block should evict idle ws connections for the account")
	}

	ap.mu.Lock()
	_, idleExists := ap.conns[idle.id]
	_, leasedExists := ap.conns[leased.id]
	ap.mu.Unlock()
	require.False(t, idleExists)
	require.True(t, leasedExists)
	select {
	case <-leased.closedCh:
		t.Fatal("runtime block should not close an active leased connection")
	default:
	}
}

func TestOpenAIRuntimeBlock_PrewarmKeepsWSDial403(t *testing.T) {
	svc := &OpenAIGatewayService{
		cfg: &config.Config{
			Gateway: config.GatewayConfig{
				OpenAIWS: config.GatewayOpenAIWSConfig{PrewarmSessionEnabled: true},
			},
		},
	}
	svc.SetOpenAIPrewarmSessionCache(newFakePrewarmSessionCache())
	account := &Account{ID: 48, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	svc.BlockAccountScheduling(account, time.Now().Add(time.Minute), "ws_dial_403")

	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIRuntimeBlock_PrewarmKeepsWSDial401(t *testing.T) {
	svc := &OpenAIGatewayService{
		cfg: &config.Config{
			Gateway: config.GatewayConfig{
				OpenAIWS: config.GatewayOpenAIWSConfig{PrewarmSessionEnabled: true},
			},
		},
	}
	svc.SetOpenAIPrewarmSessionCache(newFakePrewarmSessionCache())
	account := &Account{ID: 51, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	svc.BlockAccountScheduling(account, time.Now().Add(time.Minute), "ws_dial_401")

	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIRuntimeBlock_SkipsAccountWhenWSProxyBreakerOpen(t *testing.T) {
	proxy := &Proxy{
		ID:       88,
		Name:     "ipdeep-test",
		Protocol: "http",
		Host:     "proxy.ipdeep.com",
		Port:     7085,
		Username: "u",
		Password: "p",
	}
	pool := &openAIWSConnPool{}
	pool.proxyBreakers.Store(proxy.URL(), &proxyHandshakeBreaker{openUntil: time.Now().Add(time.Minute)})
	proxyID := proxy.ID
	svc := &OpenAIGatewayService{openaiWSPool: pool}
	account := &Account{
		ID:       54,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		ProxyID:  &proxyID,
		Proxy:    proxy,
	}

	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIWSDial401DisablesAccountImmediately(t *testing.T) {
	repo := &rateLimitAccountRepoStub{}
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := &Account{ID: 52, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	svc.handleOpenAIWSDialAccountFailure(context.Background(), account, http.StatusUnauthorized, "ws dial unauthorized")

	require.Equal(t, 1, repo.setDisabledCalls)
	require.Equal(t, 0, repo.setErrorCalls)
	require.Contains(t, repo.lastDisabledMsg, "WS dial 401")
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIWSDial403DoesNotDisableAccount(t *testing.T) {
	repo := &rateLimitAccountRepoStub{}
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := &Account{ID: 53, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	svc.handleOpenAIWSDialAccountFailure(context.Background(), account, http.StatusForbidden, "cloudflare rejected")

	require.Zero(t, repo.setErrorCalls)
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIRuntimeBlock_DoesNotApplyToOtherPlatforms(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 45, Platform: PlatformGemini, Type: AccountTypeOAuth}

	svc.BlockAccountScheduling(account, time.Time{}, "custom_error_code")

	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIRuntimeBlocker_IgnoresNonOpenAIFromRateLimitService(t *testing.T) {
	gateway := &OpenAIGatewayService{}
	repo := &rateLimitAccountRepoStub{}
	rateLimitService := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	rateLimitService.SetAccountRuntimeBlocker(gateway)
	account := &Account{ID: 45, Platform: PlatformGemini, Type: AccountTypeOAuth}

	shouldDisable := rateLimitService.HandleUpstreamError(context.Background(), account, http.StatusForbidden, http.Header{}, []byte("forbidden"))

	require.True(t, shouldDisable)
	require.False(t, gateway.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAIModelNotFound_DoesNotRuntimeBlockWholeAccount(t *testing.T) {
	repo := &modelNotFoundAccountRepoStub{}
	svc := &OpenAIGatewayService{
		rateLimitService: &RateLimitService{accountRepo: repo},
	}
	account := openAIModelNotFoundTempAccount()

	shouldDisable := svc.handleOpenAIAccountUpstreamError(
		context.Background(),
		account,
		http.StatusNotFound,
		http.Header{},
		[]byte(`{"error":{"code":"model_not_found","message":"model not found"}}`),
		"gpt-5.4",
	)

	require.True(t, shouldDisable)
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
	require.Zero(t, repo.tempCalls)
	require.Len(t, repo.modelRateLimitCalls, 1)
}

func TestOpenAIRuntimeBlock_DoesNotShortenExistingBlock(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 46, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	longUntil := time.Now().Add(10 * time.Minute)

	svc.BlockAccountScheduling(account, longUntil, "oauth_401")
	svc.BlockAccountScheduling(account, time.Time{}, "upstream_disable")

	value, ok := svc.openaiAccountRuntimeBlockUntil.Load(account.ID)
	require.True(t, ok)
	actualUntil, ok := value.(time.Time)
	require.True(t, ok)
	require.WithinDuration(t, longUntil, actualUntil, time.Second)
}

func TestOpenAIRuntimeBlock_ClearAccountSchedulingBlock(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 47, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	svc.BlockAccountScheduling(account, time.Now().Add(time.Minute), "429")
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))

	svc.ClearAccountSchedulingBlock(account.ID)
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestShouldStopOpenAIOAuth429Failover_OnlyDuringStorm(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 42, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	apiKeyAccount := &Account{ID: 43, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	require.False(t, svc.ShouldStopOpenAIOAuth429Failover(account, http.StatusTooManyRequests, 1))

	for i := 0; i < openAIOAuth429StormThreshold; i++ {
		svc.recordOpenAIOAuth429()
	}

	require.False(t, svc.ShouldStopOpenAIOAuth429Failover(account, http.StatusTooManyRequests, 0))
	require.True(t, svc.ShouldStopOpenAIOAuth429Failover(account, http.StatusTooManyRequests, 1))
	require.False(t, svc.ShouldStopOpenAIOAuth429Failover(apiKeyAccount, http.StatusTooManyRequests, 1))
	require.False(t, svc.ShouldStopOpenAIOAuth429Failover(account, http.StatusInternalServerError, 1))
}
