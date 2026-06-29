package service

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// openAIPrewarmSessionLeaderLockKey 用于多实例间选主，确保同一周期只有一个实例执行预热。
	openAIPrewarmSessionLeaderLockKey = "openai:ws:prewarm:session:leader"
	// openAIPrewarmSessionLeaderLockTTL 略大于一个预热周期，防止运行中过期。
	openAIPrewarmSessionLeaderLockTTL = 10 * time.Minute
	// openAIPrewarmSessionRunTimeout 单次 runOnce 的总超时。
	openAIPrewarmSessionRunTimeout = 3 * time.Minute
)

// OpenAIWSPrewarmSessionService 是账号级 prewarm session 的后台预热 worker。
// 周期性遍历所有 WSv2 OAuth 账号，为每个 (account, model) 维护一个持久的 prewarm response_id。
// 请求路径（forwardOpenAIWSV2）通过 tryGetOpenAIPrewarmSession 读取并注入该 id。
type OpenAIWSPrewarmSessionService struct {
	gw       *OpenAIGatewayService
	interval time.Duration
	models   []string
	// concurrency 限制同周期内并发的预热请求数（每个 (account, model) 一个 goroutine）。
	concurrency int

	lockCache  LeaderLockCache
	db         *sql.DB
	instanceID string

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewOpenAIWSPrewarmSessionService 创建 prewarm session worker。
func NewOpenAIWSPrewarmSessionService(gw *OpenAIGatewayService) *OpenAIWSPrewarmSessionService {
	if gw == nil || gw.cfg == nil {
		return nil
	}
	ws := gw.cfg.Gateway.OpenAIWS
	interval := time.Duration(ws.PrewarmSessionIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	concurrency := ws.PrewarmSessionConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	models := ws.PrewarmSessionModels
	if len(models) == 0 {
		models = []string{"gpt-5.4"}
	}
	return &OpenAIWSPrewarmSessionService{
		gw:          gw,
		interval:    interval,
		models:      models,
		concurrency: concurrency,
		instanceID:  uuid.NewString(),
		stopCh:      make(chan struct{}),
	}
}

// SetLeaderLock 注入跨实例选主所需的缓存与 DB（与 SubscriptionExpiryService 一致）。
func (s *OpenAIWSPrewarmSessionService) SetLeaderLock(lockCache LeaderLockCache, db *sql.DB) {
	if s == nil {
		return
	}
	s.lockCache = lockCache
	s.db = db
}

// Start 启动后台预热循环。遵循 account_expiry_service.go 的 ticker + stopCh 模式。
func (s *OpenAIWSPrewarmSessionService) Start() {
	if s == nil || s.gw == nil || s.interval <= 0 {
		return
	}
	if !s.isEnabled() {
		log.Printf("[OpenAIPrewarmSession] disabled, worker not started")
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		// 启动后先跑一次，缩短冷启动窗口。
		s.runOnce()
		for {
			select {
			case <-ticker.C:
				s.runOnce()
			case <-s.stopCh:
				return
			}
		}
	}()
	log.Printf("[OpenAIPrewarmSession] worker started interval=%s models=%v concurrency=%d", s.interval, s.models, s.concurrency)
}

// Stop 停止后台预热循环。
func (s *OpenAIWSPrewarmSessionService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

func (s *OpenAIWSPrewarmSessionService) isEnabled() bool {
	return s != nil && s.gw != nil && s.gw.cfg != nil && s.gw.cfg.Gateway.OpenAIWS.PrewarmSessionEnabled && s.gw.isOpenAIPrewarmSessionEnabled()
}

func (s *OpenAIWSPrewarmSessionService) runOnce() {
	if !s.isEnabled() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), openAIPrewarmSessionRunTimeout)
	defer cancel()

	// 多实例选主，避免所有实例同时预热造成上游压力与重复绑定。
	release, acquired := tryAcquireSingletonLeaderLock(ctx, s.lockCache, s.db, openAIPrewarmSessionLeaderLockKey, s.instanceID, openAIPrewarmSessionLeaderLockTTL)
	if !acquired {
		return
	}
	defer release()

	accounts, err := s.gw.listAllSchedulableOpenAIAccounts(ctx)
	if err != nil {
		log.Printf("[OpenAIPrewarmSession] list schedulable accounts failed: %v", err)
		return
	}

	// 过滤出 WSv2 OAuth、非配额暂停的账号。
	candidates := make([]*Account, 0, len(accounts))
	for i := range accounts {
		account := accounts[i]
		if account.Type != AccountTypeOAuth {
			continue
		}
		decision := s.gw.getOpenAIWSProtocolResolver().Resolve(&account)
		if decision.Transport != OpenAIUpstreamTransportResponsesWebsocketV2 {
			continue
		}
		if paused, _ := shouldAutoPauseOpenAIAccountByQuota(ctx, &account); paused {
			continue
		}
		candidates = append(candidates, &account)
	}
	if len(candidates) == 0 {
		return
	}

	// 信号量限制并发预热数。
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for i := range candidates {
		account := candidates[i]
		for _, model := range s.models {
			// 池已满(达到目标深度)的跳过，避免重复预热。
			if s.shouldSkipPrewarm(ctx, account, model) {
				continue
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(acc *Account, mdl string) {
				defer wg.Done()
				defer func() { <-sem }()
				// 池补足可能要铸造多个 id，给足时间预算。
				prewarmCtx, prewarmCancel := context.WithTimeout(ctx, 120*time.Second)
				defer prewarmCancel()
				s.gw.refillOpenAIPrewarmPool(prewarmCtx, acc, mdl)
			}(account, model)
		}
	}
	wg.Wait()
}

// shouldSkipPrewarm 判断 (account, model) 的 id 池是否已达目标深度，已满则跳过本次补池。
func (s *OpenAIWSPrewarmSessionService) shouldSkipPrewarm(ctx context.Context, account *Account, model string) bool {
	store := s.gw.getOpenAIPrewarmSessionStore()
	if store == nil || account == nil {
		return false
	}
	curLen, err := store.PrewarmSessionPoolLen(ctx, account.ID, normalizeOpenAIPrewarmModelKey(model))
	if err != nil {
		return false
	}
	return curLen >= openAIPrewarmPoolTargetDepth(account)
}
