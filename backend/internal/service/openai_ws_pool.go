package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"golang.org/x/sync/errgroup"
)

const (
	openAIWSConnMaxAge             = 60 * time.Minute
	openAIWSConnHealthCheckIdle    = 90 * time.Second
	openAIWSConnHealthCheckTO      = 2 * time.Second
	openAIWSConnPrewarmExtraDelay  = 2 * time.Second
	openAIWSAcquireCleanupInterval = 3 * time.Second
	openAIWSBackgroundPingInterval = 30 * time.Second
	openAIWSBackgroundSweepTicker  = 30 * time.Second

	openAIWSPrewarmFailureWindow   = 30 * time.Second
	openAIWSPrewarmFailureSuppress = 2
)

var (
	errOpenAIWSConnClosed               = errors.New("openai ws connection closed")
	errOpenAIWSConnQueueFull            = errors.New("openai ws connection queue full")
	errOpenAIWSPreferredConnUnavailable = errors.New("openai ws preferred connection unavailable")
	errOpenAIWSAccountRuntimeBlocked    = errors.New("openai ws account runtime blocked")
)

type openAIWSDialError struct {
	StatusCode      int
	ResponseHeaders http.Header
	Err             error
}

func (e *openAIWSDialError) Error() string {
	if e == nil {
		return ""
	}
	if e.StatusCode > 0 {
		return fmt.Sprintf("openai ws dial failed: status=%d err=%v", e.StatusCode, e.Err)
	}
	return fmt.Sprintf("openai ws dial failed: %v", e.Err)
}

func (e *openAIWSDialError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type openAIWSAcquireRequest struct {
	Account  *Account
	WSURL    string
	Headers  http.Header
	ProxyURL string
	// TLSProfile 非空时,WS 握手用 utls 伪装该 TLS 指纹(与 HTTP 上游一致);nil 则用原生 TLS。
	TLSProfile      *tlsfingerprint.Profile
	PreferredConnID string
	// ForceNewConn: 强制本次获取新连接（避免复用导致连接内续链状态互相污染）。
	ForceNewConn bool
	// ForcePreferredConn: 强制本次只使用 PreferredConnID，禁止漂移到其它连接。
	ForcePreferredConn bool
}

type openAIWSConnLease struct {
	pool      *openAIWSConnPool
	accountID int64
	conn      *openAIWSConn
	queueWait time.Duration
	connPick  time.Duration
	reused    bool
	released  atomic.Bool
}

func (l *openAIWSConnLease) activeConn() (*openAIWSConn, error) {
	if l == nil || l.conn == nil {
		return nil, errOpenAIWSConnClosed
	}
	if l.released.Load() {
		return nil, errOpenAIWSConnClosed
	}
	return l.conn, nil
}

func (l *openAIWSConnLease) ConnID() string {
	if l == nil || l.conn == nil {
		return ""
	}
	return l.conn.id
}

func (l *openAIWSConnLease) QueueWaitDuration() time.Duration {
	if l == nil {
		return 0
	}
	return l.queueWait
}

func (l *openAIWSConnLease) ConnPickDuration() time.Duration {
	if l == nil {
		return 0
	}
	return l.connPick
}

func (l *openAIWSConnLease) Reused() bool {
	if l == nil {
		return false
	}
	return l.reused
}

func (l *openAIWSConnLease) HandshakeHeader(name string) string {
	if l == nil || l.conn == nil {
		return ""
	}
	return l.conn.handshakeHeader(name)
}

func (l *openAIWSConnLease) HandshakeHeaders() http.Header {
	if l == nil || l.conn == nil {
		return nil
	}
	return cloneHeader(l.conn.handshakeHeaders)
}

func (l *openAIWSConnLease) IsPrewarmed() bool {
	if l == nil || l.conn == nil {
		return false
	}
	return l.conn.isPrewarmed()
}

func (l *openAIWSConnLease) MarkPrewarmed() {
	if l == nil || l.conn == nil {
		return
	}
	l.conn.markPrewarmed()
}

func (l *openAIWSConnLease) WriteJSON(value any, timeout time.Duration) error {
	conn, err := l.activeConn()
	if err != nil {
		return err
	}
	return conn.writeJSONWithTimeout(context.Background(), value, timeout)
}

func (l *openAIWSConnLease) WriteJSONWithContextTimeout(ctx context.Context, value any, timeout time.Duration) error {
	conn, err := l.activeConn()
	if err != nil {
		return err
	}
	return conn.writeJSONWithTimeout(ctx, value, timeout)
}

func (l *openAIWSConnLease) WriteJSONContext(ctx context.Context, value any) error {
	conn, err := l.activeConn()
	if err != nil {
		return err
	}
	return conn.writeJSON(value, ctx)
}

func (l *openAIWSConnLease) ReadMessage(timeout time.Duration) ([]byte, error) {
	conn, err := l.activeConn()
	if err != nil {
		return nil, err
	}
	return conn.readMessageWithTimeout(timeout)
}

func (l *openAIWSConnLease) ReadMessageContext(ctx context.Context) ([]byte, error) {
	conn, err := l.activeConn()
	if err != nil {
		return nil, err
	}
	return conn.readMessage(ctx)
}

func (l *openAIWSConnLease) ReadMessageWithContextTimeout(ctx context.Context, timeout time.Duration) ([]byte, error) {
	conn, err := l.activeConn()
	if err != nil {
		return nil, err
	}
	return conn.readMessageWithContextTimeout(ctx, timeout)
}

func (l *openAIWSConnLease) PingWithTimeout(timeout time.Duration) error {
	conn, err := l.activeConn()
	if err != nil {
		return err
	}
	return conn.pingWithTimeout(timeout)
}

func (l *openAIWSConnLease) MarkBroken() {
	if l == nil || l.pool == nil || l.conn == nil || l.released.Load() {
		return
	}
	l.pool.evictConn(l.accountID, l.conn.id)
}

func (l *openAIWSConnLease) Release() {
	if l == nil || l.conn == nil {
		return
	}
	if !l.released.CompareAndSwap(false, true) {
		return
	}
	l.conn.release()
}

type openAIWSConn struct {
	id string
	ws openAIWSClientConn

	handshakeHeaders http.Header

	leaseCh   chan struct{}
	closedCh  chan struct{}
	closeOnce sync.Once

	readMu  sync.Mutex
	writeMu sync.Mutex

	waiters       atomic.Int32
	createdAtNano atomic.Int64
	lastUsedNano  atomic.Int64
	prewarmed     atomic.Bool
}

func newOpenAIWSConn(id string, _ int64, ws openAIWSClientConn, handshakeHeaders http.Header) *openAIWSConn {
	now := time.Now()
	conn := &openAIWSConn{
		id:               id,
		ws:               ws,
		handshakeHeaders: cloneHeader(handshakeHeaders),
		leaseCh:          make(chan struct{}, 1),
		closedCh:         make(chan struct{}),
	}
	conn.leaseCh <- struct{}{}
	conn.createdAtNano.Store(now.UnixNano())
	conn.lastUsedNano.Store(now.UnixNano())
	return conn
}

func (c *openAIWSConn) tryAcquire() bool {
	if c == nil {
		return false
	}
	select {
	case <-c.closedCh:
		return false
	default:
	}
	select {
	case <-c.leaseCh:
		select {
		case <-c.closedCh:
			c.release()
			return false
		default:
		}
		return true
	default:
		return false
	}
}

func (c *openAIWSConn) acquire(ctx context.Context) error {
	if c == nil {
		return errOpenAIWSConnClosed
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closedCh:
			return errOpenAIWSConnClosed
		case <-c.leaseCh:
			select {
			case <-c.closedCh:
				c.release()
				return errOpenAIWSConnClosed
			default:
			}
			return nil
		}
	}
}

func (c *openAIWSConn) release() {
	if c == nil {
		return
	}
	select {
	case c.leaseCh <- struct{}{}:
	default:
	}
	c.touch()
}

func (c *openAIWSConn) close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		close(c.closedCh)
		if c.ws != nil {
			_ = c.ws.Close()
		}
		select {
		case c.leaseCh <- struct{}{}:
		default:
		}
	})
}

func (c *openAIWSConn) writeJSONWithTimeout(parent context.Context, value any, timeout time.Duration) error {
	if c == nil {
		return errOpenAIWSConnClosed
	}
	select {
	case <-c.closedCh:
		return errOpenAIWSConnClosed
	default:
	}

	writeCtx := parent
	if writeCtx == nil {
		writeCtx = context.Background()
	}
	if timeout <= 0 {
		return c.writeJSON(value, writeCtx)
	}
	var cancel context.CancelFunc
	writeCtx, cancel = context.WithTimeout(writeCtx, timeout)
	defer cancel()
	return c.writeJSON(value, writeCtx)
}

func (c *openAIWSConn) writeJSON(value any, writeCtx context.Context) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.ws == nil {
		return errOpenAIWSConnClosed
	}
	if writeCtx == nil {
		writeCtx = context.Background()
	}
	if err := c.ws.WriteJSON(writeCtx, value); err != nil {
		return err
	}
	c.touch()
	return nil
}

func (c *openAIWSConn) readMessageWithTimeout(timeout time.Duration) ([]byte, error) {
	return c.readMessageWithContextTimeout(context.Background(), timeout)
}

func (c *openAIWSConn) readMessageWithContextTimeout(parent context.Context, timeout time.Duration) ([]byte, error) {
	if c == nil {
		return nil, errOpenAIWSConnClosed
	}
	select {
	case <-c.closedCh:
		return nil, errOpenAIWSConnClosed
	default:
	}

	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		return c.readMessage(parent)
	}
	readCtx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return c.readMessage(readCtx)
}

func (c *openAIWSConn) readMessage(readCtx context.Context) ([]byte, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.ws == nil {
		return nil, errOpenAIWSConnClosed
	}
	if readCtx == nil {
		readCtx = context.Background()
	}
	payload, err := c.ws.ReadMessage(readCtx)
	if err != nil {
		return nil, err
	}
	c.touch()
	return payload, nil
}

func (c *openAIWSConn) pingWithTimeout(timeout time.Duration) error {
	if c == nil {
		return errOpenAIWSConnClosed
	}
	select {
	case <-c.closedCh:
		return errOpenAIWSConnClosed
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.ws == nil {
		return errOpenAIWSConnClosed
	}
	if timeout <= 0 {
		timeout = openAIWSConnHealthCheckTO
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.ws.Ping(pingCtx); err != nil {
		return err
	}
	return nil
}

func (c *openAIWSConn) touch() {
	if c == nil {
		return
	}
	c.lastUsedNano.Store(time.Now().UnixNano())
}

func (c *openAIWSConn) createdAt() time.Time {
	if c == nil {
		return time.Time{}
	}
	nano := c.createdAtNano.Load()
	if nano <= 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}

func (c *openAIWSConn) lastUsedAt() time.Time {
	if c == nil {
		return time.Time{}
	}
	nano := c.lastUsedNano.Load()
	if nano <= 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}

func (c *openAIWSConn) idleDuration(now time.Time) time.Duration {
	if c == nil {
		return 0
	}
	last := c.lastUsedAt()
	if last.IsZero() {
		return 0
	}
	return now.Sub(last)
}

func (c *openAIWSConn) age(now time.Time) time.Duration {
	if c == nil {
		return 0
	}
	created := c.createdAt()
	if created.IsZero() {
		return 0
	}
	return now.Sub(created)
}

func (c *openAIWSConn) isLeased() bool {
	if c == nil {
		return false
	}
	return len(c.leaseCh) == 0
}

func (c *openAIWSConn) handshakeHeader(name string) string {
	if c == nil || c.handshakeHeaders == nil {
		return ""
	}
	return strings.TrimSpace(c.handshakeHeaders.Get(strings.TrimSpace(name)))
}

func (c *openAIWSConn) isPrewarmed() bool {
	if c == nil {
		return false
	}
	return c.prewarmed.Load()
}

func (c *openAIWSConn) markPrewarmed() {
	if c == nil {
		return
	}
	c.prewarmed.Store(true)
}

type openAIWSAccountPool struct {
	mu            sync.Mutex
	conns         map[string]*openAIWSConn
	pinnedConns   map[string]int
	creating      int
	lastCleanupAt time.Time
	lastAcquire   *openAIWSAcquireRequest
	prewarmActive bool
	prewarmUntil  time.Time
	prewarmFails  int
	prewarmFailAt time.Time
}

type OpenAIWSPoolMetricsSnapshot struct {
	AcquireTotal            int64
	AcquireReuseTotal       int64
	AcquireCreateTotal      int64
	AcquireQueueWaitTotal   int64
	AcquireQueueWaitMsTotal int64
	ConnPickTotal           int64
	ConnPickMsTotal         int64
	ScaleUpTotal            int64
	ScaleDownTotal          int64
}

type openAIWSPoolMetrics struct {
	acquireTotal          atomic.Int64
	acquireReuseTotal     atomic.Int64
	acquireCreateTotal    atomic.Int64
	acquireQueueWaitTotal atomic.Int64
	acquireQueueWaitMs    atomic.Int64
	connPickTotal         atomic.Int64
	connPickMs            atomic.Int64
	scaleUpTotal          atomic.Int64
	scaleDownTotal        atomic.Int64
}

type openAIWSConnPool struct {
	cfg *config.Config
	// 通过接口解耦底层 WS 客户端实现，默认使用 coder/websocket。
	clientDialer openAIWSClientDialer
	// runtimeBlocked 由服务层注入；账号进入短软锁/熔断时，连接池不再为其复用或预热连接。
	runtimeBlocked func(*Account) bool

	accounts sync.Map // key: int64(accountID), value: *openAIWSAccountPool
	seq      atomic.Uint64

	// handshakeSems: 每出口代理IP的握手并发信号量(key: proxyURL, value: chan struct{})。
	// 限制单IP同时进行的WS握手数,避免突发触发 Cloudflare 403 风暴。
	handshakeSems sync.Map

	// proxyBreakers: 每出口代理IP的握手熔断器(key: proxyURL, value: *proxyHandshakeBreaker)。
	// CF 403 封 IP 后,重试雪崩会把 CF 越打越死;熔断后该代理的新握手直接 fail-fast(不再打 CF),
	// 给 IP 冷却机会,并打断"403→重试→更多403"的雪崩。
	proxyBreakers sync.Map

	// 全局 egress 握手熔断:汇总所有代理的握手 403;窗口内超阈值即判定 CF 整体封出口,
	// 对所有新握手 fail-fast。单代理熔断只能防"个别坏代理",防不住"全体被封"的雪崩。
	globalBreakerMu        sync.Mutex
	global403Count         int
	global403WindowStart   time.Time
	globalBreakerOpenUntil time.Time

	metrics openAIWSPoolMetrics

	workerStopCh chan struct{}
	workerWg     sync.WaitGroup
	closeOnce    sync.Once
}

func newOpenAIWSConnPool(cfg *config.Config) *openAIWSConnPool {
	pool := &openAIWSConnPool{
		cfg:          cfg,
		clientDialer: newDefaultOpenAIWSClientDialer(),
		workerStopCh: make(chan struct{}),
	}
	pool.startBackgroundWorkers()
	return pool
}

func (p *openAIWSConnPool) SnapshotMetrics() OpenAIWSPoolMetricsSnapshot {
	if p == nil {
		return OpenAIWSPoolMetricsSnapshot{}
	}
	return OpenAIWSPoolMetricsSnapshot{
		AcquireTotal:            p.metrics.acquireTotal.Load(),
		AcquireReuseTotal:       p.metrics.acquireReuseTotal.Load(),
		AcquireCreateTotal:      p.metrics.acquireCreateTotal.Load(),
		AcquireQueueWaitTotal:   p.metrics.acquireQueueWaitTotal.Load(),
		AcquireQueueWaitMsTotal: p.metrics.acquireQueueWaitMs.Load(),
		ConnPickTotal:           p.metrics.connPickTotal.Load(),
		ConnPickMsTotal:         p.metrics.connPickMs.Load(),
		ScaleUpTotal:            p.metrics.scaleUpTotal.Load(),
		ScaleDownTotal:          p.metrics.scaleDownTotal.Load(),
	}
}

func (p *openAIWSConnPool) SnapshotTransportMetrics() OpenAIWSTransportMetricsSnapshot {
	if p == nil {
		return OpenAIWSTransportMetricsSnapshot{}
	}
	if dialer, ok := p.clientDialer.(openAIWSTransportMetricsDialer); ok {
		return dialer.SnapshotTransportMetrics()
	}
	return OpenAIWSTransportMetricsSnapshot{}
}

func (p *openAIWSConnPool) setClientDialerForTest(dialer openAIWSClientDialer) {
	if p == nil || dialer == nil {
		return
	}
	p.clientDialer = dialer
}

func (p *openAIWSConnPool) isAccountRuntimeBlocked(account *Account) bool {
	return p != nil && p.runtimeBlocked != nil && p.runtimeBlocked(account)
}

// Close 停止后台 worker 并关闭所有空闲连接，应在优雅关闭时调用。
func (p *openAIWSConnPool) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		if p.workerStopCh != nil {
			close(p.workerStopCh)
		}
		p.workerWg.Wait()
		// 遍历所有账户池，关闭全部空闲连接。
		p.accounts.Range(func(key, value any) bool {
			ap, ok := value.(*openAIWSAccountPool)
			if !ok || ap == nil {
				return true
			}
			ap.mu.Lock()
			for _, conn := range ap.conns {
				if conn != nil && !conn.isLeased() {
					conn.close()
				}
			}
			ap.mu.Unlock()
			return true
		})
	})
}

func (p *openAIWSConnPool) startBackgroundWorkers() {
	if p == nil || p.workerStopCh == nil {
		return
	}
	p.workerWg.Add(2)
	go func() {
		defer p.workerWg.Done()
		p.runBackgroundPingWorker()
	}()
	go func() {
		defer p.workerWg.Done()
		p.runBackgroundCleanupWorker()
	}()
}

type openAIWSIdlePingCandidate struct {
	accountID int64
	conn      *openAIWSConn
}

func (p *openAIWSConnPool) runBackgroundPingWorker() {
	if p == nil {
		return
	}
	ticker := time.NewTicker(openAIWSBackgroundPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.runBackgroundPingSweep()
		case <-p.workerStopCh:
			return
		}
	}
}

func (p *openAIWSConnPool) runBackgroundPingSweep() {
	if p == nil {
		return
	}
	candidates := p.snapshotIdleConnsForPing()
	var g errgroup.Group
	g.SetLimit(10)
	for _, item := range candidates {
		item := item
		if item.conn == nil || item.conn.isLeased() || item.conn.waiters.Load() > 0 {
			continue
		}
		g.Go(func() error {
			if err := item.conn.pingWithTimeout(openAIWSConnHealthCheckTO); err != nil {
				p.evictConn(item.accountID, item.conn.id)
			}
			return nil
		})
	}
	_ = g.Wait()
}

func (p *openAIWSConnPool) snapshotIdleConnsForPing() []openAIWSIdlePingCandidate {
	if p == nil {
		return nil
	}
	candidates := make([]openAIWSIdlePingCandidate, 0)
	p.accounts.Range(func(key, value any) bool {
		accountID, ok := key.(int64)
		if !ok || accountID <= 0 {
			return true
		}
		ap, ok := value.(*openAIWSAccountPool)
		if !ok || ap == nil {
			return true
		}
		ap.mu.Lock()
		for _, conn := range ap.conns {
			if conn == nil || conn.isLeased() || conn.waiters.Load() > 0 {
				continue
			}
			candidates = append(candidates, openAIWSIdlePingCandidate{
				accountID: accountID,
				conn:      conn,
			})
		}
		ap.mu.Unlock()
		return true
	})
	return candidates
}

func (p *openAIWSConnPool) runBackgroundCleanupWorker() {
	if p == nil {
		return
	}
	ticker := time.NewTicker(openAIWSBackgroundSweepTicker)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.runBackgroundCleanupSweep(time.Now())
		case <-p.workerStopCh:
			return
		}
	}
}

func (p *openAIWSConnPool) runBackgroundCleanupSweep(now time.Time) {
	if p == nil {
		return
	}
	type cleanupResult struct {
		evicted []*openAIWSConn
	}
	results := make([]cleanupResult, 0)
	p.accounts.Range(func(_ any, value any) bool {
		ap, ok := value.(*openAIWSAccountPool)
		if !ok || ap == nil {
			return true
		}
		maxConns := p.maxConnsHardCap()
		ap.mu.Lock()
		if ap.lastAcquire != nil && ap.lastAcquire.Account != nil {
			maxConns = p.effectiveMaxConnsByAccount(ap.lastAcquire.Account)
		}
		evicted := p.cleanupAccountLocked(ap, now, maxConns)
		ap.lastCleanupAt = now
		ap.mu.Unlock()
		if len(evicted) > 0 {
			results = append(results, cleanupResult{evicted: evicted})
		}
		return true
	})
	for _, result := range results {
		closeOpenAIWSConns(result.evicted)
	}
}

func (p *openAIWSConnPool) Acquire(ctx context.Context, req openAIWSAcquireRequest) (*openAIWSConnLease, error) {
	if p != nil {
		p.metrics.acquireTotal.Add(1)
	}
	return p.acquire(ctx, cloneOpenAIWSAcquireRequest(req), 0)
}

func (p *openAIWSConnPool) acquire(ctx context.Context, req openAIWSAcquireRequest, retry int) (*openAIWSConnLease, error) {
	if p == nil || req.Account == nil || req.Account.ID <= 0 {
		return nil, errors.New("invalid ws acquire request")
	}
	if stringsTrim(req.WSURL) == "" {
		return nil, errors.New("ws url is empty")
	}
	if p.isAccountRuntimeBlocked(req.Account) {
		return nil, errOpenAIWSAccountRuntimeBlocked
	}

	accountID := req.Account.ID
	effectiveMaxConns := p.effectiveMaxConnsByAccount(req.Account)
	if effectiveMaxConns <= 0 {
		return nil, errOpenAIWSConnQueueFull
	}
	var evicted []*openAIWSConn
	ap := p.getOrCreateAccountPool(accountID)
	ap.mu.Lock()
	ap.lastAcquire = cloneOpenAIWSAcquireRequestPtr(&req)
	now := time.Now()
	if ap.lastCleanupAt.IsZero() || now.Sub(ap.lastCleanupAt) >= openAIWSAcquireCleanupInterval {
		evicted = p.cleanupAccountLocked(ap, now, effectiveMaxConns)
		ap.lastCleanupAt = now
	}
	pickStartedAt := time.Now()
	allowReuse := !req.ForceNewConn
	preferredConnID := stringsTrim(req.PreferredConnID)
	forcePreferredConn := allowReuse && req.ForcePreferredConn

	if allowReuse {
		if forcePreferredConn {
			if preferredConnID == "" {
				p.recordConnPickDuration(time.Since(pickStartedAt))
				ap.mu.Unlock()
				closeOpenAIWSConns(evicted)
				return nil, errOpenAIWSPreferredConnUnavailable
			}
			preferredConn, ok := ap.conns[preferredConnID]
			if !ok || preferredConn == nil {
				p.recordConnPickDuration(time.Since(pickStartedAt))
				ap.mu.Unlock()
				closeOpenAIWSConns(evicted)
				return nil, errOpenAIWSPreferredConnUnavailable
			}
			if preferredConn.tryAcquire() {
				connPick := time.Since(pickStartedAt)
				p.recordConnPickDuration(connPick)
				ap.mu.Unlock()
				closeOpenAIWSConns(evicted)
				if p.shouldHealthCheckConn(preferredConn) {
					if err := preferredConn.pingWithTimeout(openAIWSConnHealthCheckTO); err != nil {
						preferredConn.close()
						p.evictConn(accountID, preferredConn.id)
						if retry < 1 {
							return p.acquire(ctx, req, retry+1)
						}
						return nil, err
					}
				}
				lease := &openAIWSConnLease{
					pool:      p,
					accountID: accountID,
					conn:      preferredConn,
					connPick:  connPick,
					reused:    true,
				}
				p.metrics.acquireReuseTotal.Add(1)
				p.ensureTargetIdleAsync(accountID)
				return lease, nil
			}

			connPick := time.Since(pickStartedAt)
			p.recordConnPickDuration(connPick)
			if int(preferredConn.waiters.Load()) >= p.queueLimitPerConn() {
				ap.mu.Unlock()
				closeOpenAIWSConns(evicted)
				return nil, errOpenAIWSConnQueueFull
			}
			preferredConn.waiters.Add(1)
			ap.mu.Unlock()
			closeOpenAIWSConns(evicted)
			defer preferredConn.waiters.Add(-1)
			waitStart := time.Now()
			p.metrics.acquireQueueWaitTotal.Add(1)

			if err := preferredConn.acquire(ctx); err != nil {
				if errors.Is(err, errOpenAIWSConnClosed) && retry < 1 {
					return p.acquire(ctx, req, retry+1)
				}
				return nil, err
			}
			if p.shouldHealthCheckConn(preferredConn) {
				if err := preferredConn.pingWithTimeout(openAIWSConnHealthCheckTO); err != nil {
					preferredConn.release()
					preferredConn.close()
					p.evictConn(accountID, preferredConn.id)
					if retry < 1 {
						return p.acquire(ctx, req, retry+1)
					}
					return nil, err
				}
			}

			queueWait := time.Since(waitStart)
			p.metrics.acquireQueueWaitMs.Add(queueWait.Milliseconds())
			lease := &openAIWSConnLease{
				pool:      p,
				accountID: accountID,
				conn:      preferredConn,
				queueWait: queueWait,
				connPick:  connPick,
				reused:    true,
			}
			p.metrics.acquireReuseTotal.Add(1)
			p.ensureTargetIdleAsync(accountID)
			return lease, nil
		}

		if preferredConnID != "" {
			if conn, ok := ap.conns[preferredConnID]; ok && conn.tryAcquire() {
				connPick := time.Since(pickStartedAt)
				p.recordConnPickDuration(connPick)
				ap.mu.Unlock()
				closeOpenAIWSConns(evicted)
				if p.shouldHealthCheckConn(conn) {
					if err := conn.pingWithTimeout(openAIWSConnHealthCheckTO); err != nil {
						conn.close()
						p.evictConn(accountID, conn.id)
						if retry < 1 {
							return p.acquire(ctx, req, retry+1)
						}
						return nil, err
					}
				}
				lease := &openAIWSConnLease{pool: p, accountID: accountID, conn: conn, connPick: connPick, reused: true}
				p.metrics.acquireReuseTotal.Add(1)
				p.ensureTargetIdleAsync(accountID)
				return lease, nil
			}
		}

		best := p.pickLeastBusyConnLocked(ap, "")
		if best != nil && best.tryAcquire() {
			connPick := time.Since(pickStartedAt)
			p.recordConnPickDuration(connPick)
			ap.mu.Unlock()
			closeOpenAIWSConns(evicted)
			if p.shouldHealthCheckConn(best) {
				if err := best.pingWithTimeout(openAIWSConnHealthCheckTO); err != nil {
					best.close()
					p.evictConn(accountID, best.id)
					if retry < 1 {
						return p.acquire(ctx, req, retry+1)
					}
					return nil, err
				}
			}
			lease := &openAIWSConnLease{pool: p, accountID: accountID, conn: best, connPick: connPick, reused: true}
			p.metrics.acquireReuseTotal.Add(1)
			p.ensureTargetIdleAsync(accountID)
			return lease, nil
		}
		for _, conn := range ap.conns {
			if conn == nil || conn == best {
				continue
			}
			if conn.tryAcquire() {
				connPick := time.Since(pickStartedAt)
				p.recordConnPickDuration(connPick)
				ap.mu.Unlock()
				closeOpenAIWSConns(evicted)
				if p.shouldHealthCheckConn(conn) {
					if err := conn.pingWithTimeout(openAIWSConnHealthCheckTO); err != nil {
						conn.close()
						p.evictConn(accountID, conn.id)
						if retry < 1 {
							return p.acquire(ctx, req, retry+1)
						}
						return nil, err
					}
				}
				lease := &openAIWSConnLease{pool: p, accountID: accountID, conn: conn, connPick: connPick, reused: true}
				p.metrics.acquireReuseTotal.Add(1)
				p.ensureTargetIdleAsync(accountID)
				return lease, nil
			}
		}
	}

	if req.ForceNewConn && len(ap.conns)+ap.creating >= effectiveMaxConns {
		if idle := p.pickOldestIdleConnLocked(ap); idle != nil {
			delete(ap.conns, idle.id)
			evicted = append(evicted, idle)
			p.metrics.scaleDownTotal.Add(1)
		}
	}

	if len(ap.conns)+ap.creating < effectiveMaxConns {
		connPick := time.Since(pickStartedAt)
		p.recordConnPickDuration(connPick)
		ap.creating++
		ap.mu.Unlock()
		closeOpenAIWSConns(evicted)

		conn, dialErr := p.dialConn(ctx, req)

		ap = p.getOrCreateAccountPool(accountID)
		ap.mu.Lock()
		ap.creating--
		if dialErr != nil {
			ap.prewarmFails++
			ap.prewarmFailAt = time.Now()
			ap.mu.Unlock()
			return nil, dialErr
		}
		ap.conns[conn.id] = conn
		ap.prewarmFails = 0
		ap.prewarmFailAt = time.Time{}
		ap.mu.Unlock()
		p.metrics.acquireCreateTotal.Add(1)

		if !conn.tryAcquire() {
			if err := conn.acquire(ctx); err != nil {
				conn.close()
				p.evictConn(accountID, conn.id)
				return nil, err
			}
		}
		lease := &openAIWSConnLease{pool: p, accountID: accountID, conn: conn, connPick: connPick}
		p.ensureTargetIdleAsync(accountID)
		return lease, nil
	}

	if req.ForceNewConn {
		p.recordConnPickDuration(time.Since(pickStartedAt))
		ap.mu.Unlock()
		closeOpenAIWSConns(evicted)
		return nil, errOpenAIWSConnQueueFull
	}

	target := p.pickLeastBusyConnLocked(ap, req.PreferredConnID)
	connPick := time.Since(pickStartedAt)
	p.recordConnPickDuration(connPick)
	if target == nil {
		ap.mu.Unlock()
		closeOpenAIWSConns(evicted)
		return nil, errOpenAIWSConnClosed
	}
	if int(target.waiters.Load()) >= p.queueLimitPerConn() {
		ap.mu.Unlock()
		closeOpenAIWSConns(evicted)
		return nil, errOpenAIWSConnQueueFull
	}
	target.waiters.Add(1)
	ap.mu.Unlock()
	closeOpenAIWSConns(evicted)
	defer target.waiters.Add(-1)
	waitStart := time.Now()
	p.metrics.acquireQueueWaitTotal.Add(1)

	if err := target.acquire(ctx); err != nil {
		if errors.Is(err, errOpenAIWSConnClosed) && retry < 1 {
			return p.acquire(ctx, req, retry+1)
		}
		return nil, err
	}
	if p.shouldHealthCheckConn(target) {
		if err := target.pingWithTimeout(openAIWSConnHealthCheckTO); err != nil {
			target.release()
			target.close()
			p.evictConn(accountID, target.id)
			if retry < 1 {
				return p.acquire(ctx, req, retry+1)
			}
			return nil, err
		}
	}

	queueWait := time.Since(waitStart)
	p.metrics.acquireQueueWaitMs.Add(queueWait.Milliseconds())
	lease := &openAIWSConnLease{pool: p, accountID: accountID, conn: target, queueWait: queueWait, connPick: connPick, reused: true}
	p.metrics.acquireReuseTotal.Add(1)
	p.ensureTargetIdleAsync(accountID)
	return lease, nil
}

func (p *openAIWSConnPool) recordConnPickDuration(duration time.Duration) {
	if p == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	p.metrics.connPickTotal.Add(1)
	p.metrics.connPickMs.Add(duration.Milliseconds())
}

func (p *openAIWSConnPool) pickOldestIdleConnLocked(ap *openAIWSAccountPool) *openAIWSConn {
	if ap == nil || len(ap.conns) == 0 {
		return nil
	}
	var oldest *openAIWSConn
	for _, conn := range ap.conns {
		if conn == nil || conn.isLeased() || conn.waiters.Load() > 0 || p.isConnPinnedLocked(ap, conn.id) {
			continue
		}
		if oldest == nil || conn.lastUsedAt().Before(oldest.lastUsedAt()) {
			oldest = conn
		}
	}
	return oldest
}

func (p *openAIWSConnPool) getOrCreateAccountPool(accountID int64) *openAIWSAccountPool {
	if p == nil || accountID <= 0 {
		return nil
	}
	if existing, ok := p.accounts.Load(accountID); ok {
		if ap, typed := existing.(*openAIWSAccountPool); typed && ap != nil {
			return ap
		}
	}
	ap := &openAIWSAccountPool{
		conns:       make(map[string]*openAIWSConn),
		pinnedConns: make(map[string]int),
	}
	actual, _ := p.accounts.LoadOrStore(accountID, ap)
	if typed, ok := actual.(*openAIWSAccountPool); ok && typed != nil {
		return typed
	}
	return ap
}

// ensureAccountPoolLocked 兼容旧调用。
func (p *openAIWSConnPool) ensureAccountPoolLocked(accountID int64) *openAIWSAccountPool {
	return p.getOrCreateAccountPool(accountID)
}

func (p *openAIWSConnPool) getAccountPool(accountID int64) (*openAIWSAccountPool, bool) {
	if p == nil || accountID <= 0 {
		return nil, false
	}
	value, ok := p.accounts.Load(accountID)
	if !ok || value == nil {
		return nil, false
	}
	ap, typed := value.(*openAIWSAccountPool)
	return ap, typed && ap != nil
}

func (p *openAIWSConnPool) isConnPinnedLocked(ap *openAIWSAccountPool, connID string) bool {
	if ap == nil || connID == "" || len(ap.pinnedConns) == 0 {
		return false
	}
	return ap.pinnedConns[connID] > 0
}

func (p *openAIWSConnPool) cleanupAccountLocked(ap *openAIWSAccountPool, now time.Time, maxConns int) []*openAIWSConn {
	if ap == nil {
		return nil
	}
	maxAge := p.maxConnAge()

	evicted := make([]*openAIWSConn, 0)
	for id, conn := range ap.conns {
		if conn == nil {
			delete(ap.conns, id)
			if len(ap.pinnedConns) > 0 {
				delete(ap.pinnedConns, id)
			}
			continue
		}
		select {
		case <-conn.closedCh:
			delete(ap.conns, id)
			if len(ap.pinnedConns) > 0 {
				delete(ap.pinnedConns, id)
			}
			evicted = append(evicted, conn)
			continue
		default:
		}
		if p.isConnPinnedLocked(ap, id) {
			continue
		}
		if maxAge > 0 && !conn.isLeased() && conn.age(now) > maxAge {
			delete(ap.conns, id)
			if len(ap.pinnedConns) > 0 {
				delete(ap.pinnedConns, id)
			}
			evicted = append(evicted, conn)
		}
	}

	if maxConns <= 0 {
		maxConns = p.maxConnsHardCap()
	}
	maxIdle := p.maxIdlePerAccount()
	if maxIdle < 0 || maxIdle > maxConns {
		maxIdle = maxConns
	}
	if maxIdle >= 0 && len(ap.conns) > maxIdle {
		idleConns := make([]*openAIWSConn, 0, len(ap.conns))
		for id, conn := range ap.conns {
			if conn == nil {
				delete(ap.conns, id)
				if len(ap.pinnedConns) > 0 {
					delete(ap.pinnedConns, id)
				}
				continue
			}
			// 有等待者的连接不能在清理阶段被淘汰，否则等待中的 acquire 会收到 closed 错误。
			if conn.isLeased() || conn.waiters.Load() > 0 || p.isConnPinnedLocked(ap, conn.id) {
				continue
			}
			idleConns = append(idleConns, conn)
		}
		sort.SliceStable(idleConns, func(i, j int) bool {
			return idleConns[i].lastUsedAt().Before(idleConns[j].lastUsedAt())
		})
		redundant := len(ap.conns) - maxIdle
		if redundant > len(idleConns) {
			redundant = len(idleConns)
		}
		for i := 0; i < redundant; i++ {
			conn := idleConns[i]
			delete(ap.conns, conn.id)
			if len(ap.pinnedConns) > 0 {
				delete(ap.pinnedConns, conn.id)
			}
			evicted = append(evicted, conn)
		}
		if redundant > 0 {
			p.metrics.scaleDownTotal.Add(int64(redundant))
		}
	}

	return evicted
}

func (p *openAIWSConnPool) pickLeastBusyConnLocked(ap *openAIWSAccountPool, preferredConnID string) *openAIWSConn {
	if ap == nil || len(ap.conns) == 0 {
		return nil
	}
	preferredConnID = stringsTrim(preferredConnID)
	if preferredConnID != "" {
		if conn, ok := ap.conns[preferredConnID]; ok {
			return conn
		}
	}
	var best *openAIWSConn
	var bestWaiters int32
	var bestLastUsed time.Time
	for _, conn := range ap.conns {
		if conn == nil {
			continue
		}
		waiters := conn.waiters.Load()
		lastUsed := conn.lastUsedAt()
		if best == nil ||
			waiters < bestWaiters ||
			(waiters == bestWaiters && lastUsed.Before(bestLastUsed)) {
			best = conn
			bestWaiters = waiters
			bestLastUsed = lastUsed
		}
	}
	return best
}

func accountPoolLoadLocked(ap *openAIWSAccountPool) (inflight int, waiters int) {
	if ap == nil {
		return 0, 0
	}
	for _, conn := range ap.conns {
		if conn == nil {
			continue
		}
		if conn.isLeased() {
			inflight++
		}
		waiters += int(conn.waiters.Load())
	}
	return inflight, waiters
}

// AccountPoolLoad 返回指定账号连接池的并发与排队快照。
func (p *openAIWSConnPool) AccountPoolLoad(accountID int64) (inflight int, waiters int, conns int) {
	if p == nil || accountID <= 0 {
		return 0, 0, 0
	}
	ap, ok := p.getAccountPool(accountID)
	if !ok || ap == nil {
		return 0, 0, 0
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	inflight, waiters = accountPoolLoadLocked(ap)
	return inflight, waiters, len(ap.conns)
}

func (p *openAIWSConnPool) ensureTargetIdleAsync(accountID int64) {
	if p == nil || accountID <= 0 {
		return
	}

	var req openAIWSAcquireRequest
	need := 0
	ap, ok := p.getAccountPool(accountID)
	if !ok || ap == nil {
		return
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if ap.lastAcquire == nil {
		return
	}
	if ap.prewarmActive {
		return
	}
	if p.isAccountRuntimeBlocked(ap.lastAcquire.Account) {
		return
	}
	now := time.Now()
	if !ap.prewarmUntil.IsZero() && now.Before(ap.prewarmUntil) {
		return
	}
	if p.shouldSuppressPrewarmLocked(ap, now) {
		return
	}
	effectiveMaxConns := p.maxConnsHardCap()
	if ap.lastAcquire != nil && ap.lastAcquire.Account != nil {
		effectiveMaxConns = p.effectiveMaxConnsByAccount(ap.lastAcquire.Account)
	}
	target := p.targetConnCountLocked(ap, effectiveMaxConns)
	current := len(ap.conns) + ap.creating
	if current >= target {
		return
	}
	need = target - current
	if need <= 0 {
		return
	}
	req = cloneOpenAIWSAcquireRequest(*ap.lastAcquire)
	ap.prewarmActive = true
	if cooldown := p.prewarmCooldown(); cooldown > 0 {
		ap.prewarmUntil = now.Add(cooldown)
	}
	ap.creating += need
	p.metrics.scaleUpTotal.Add(int64(need))

	go p.prewarmConns(accountID, req, need)
}

func (p *openAIWSConnPool) targetConnCountLocked(ap *openAIWSAccountPool, maxConns int) int {
	if ap == nil {
		return 0
	}

	if maxConns <= 0 {
		return 0
	}

	minIdle := p.minIdlePerAccount()
	if minIdle < 0 {
		minIdle = 0
	}
	if minIdle > maxConns {
		minIdle = maxConns
	}

	inflight, waiters := accountPoolLoadLocked(ap)
	utilization := p.targetUtilization()
	demand := inflight + waiters
	if demand <= 0 {
		return minIdle
	}

	target := 1
	if demand > 1 {
		target = int(math.Ceil(float64(demand) / utilization))
	}
	if waiters > 0 && target < len(ap.conns)+1 {
		target = len(ap.conns) + 1
	}
	if target < minIdle {
		target = minIdle
	}
	if target > maxConns {
		target = maxConns
	}
	return target
}

func (p *openAIWSConnPool) prewarmConns(accountID int64, req openAIWSAcquireRequest, total int) {
	// remaining 记录本 goroutine 已加到 ap.creating 但尚未回退的额度。
	// 正常路径每次 dial 后 remaining-- 并同步 ap.creating--；
	// 若 dialConn panic 或中途 return，defer 用 remaining 把残留的 creating 额度补回，
	// 防止 creating 计数泄漏导致 len(conns)+creating 永久虚高、再也不新建/预热。
	remaining := total
	defer func() {
		_ = recover()
		if ap, ok := p.getAccountPool(accountID); ok && ap != nil {
			ap.mu.Lock()
			ap.prewarmActive = false
			if remaining > 0 {
				if ap.creating >= remaining {
					ap.creating -= remaining
				} else {
					ap.creating = 0
				}
			}
			ap.mu.Unlock()
		}
	}()

	for i := 0; i < total; i++ {
		if p.isAccountRuntimeBlocked(req.Account) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.dialTimeout()+openAIWSConnPrewarmExtraDelay)
		conn, err := p.dialConn(ctx, req)
		cancel()

		ap, ok := p.getAccountPool(accountID)
		if !ok || ap == nil {
			if conn != nil {
				conn.close()
			}
			return
		}
		ap.mu.Lock()
		if ap.creating > 0 {
			ap.creating--
		}
		remaining--
		if err != nil {
			ap.prewarmFails++
			ap.prewarmFailAt = time.Now()
			ap.mu.Unlock()
			continue
		}
		if p.isAccountRuntimeBlocked(req.Account) {
			ap.mu.Unlock()
			conn.close()
			return
		}
		if len(ap.conns) >= p.effectiveMaxConnsByAccount(req.Account) {
			ap.mu.Unlock()
			conn.close()
			continue
		}
		ap.conns[conn.id] = conn
		ap.prewarmFails = 0
		ap.prewarmFailAt = time.Time{}
		ap.mu.Unlock()
	}
}

func (p *openAIWSConnPool) evictConn(accountID int64, connID string) {
	if p == nil || accountID <= 0 || stringsTrim(connID) == "" {
		return
	}
	var conn *openAIWSConn
	ap, ok := p.getAccountPool(accountID)
	if ok && ap != nil {
		ap.mu.Lock()
		if c, exists := ap.conns[connID]; exists {
			conn = c
			delete(ap.conns, connID)
			if len(ap.pinnedConns) > 0 {
				delete(ap.pinnedConns, connID)
			}
		}
		ap.mu.Unlock()
	}
	if conn != nil {
		conn.close()
	}
}

func (p *openAIWSConnPool) evictIdleConnsForAccount(accountID int64) int {
	if p == nil || accountID <= 0 {
		return 0
	}
	ap, ok := p.getAccountPool(accountID)
	if !ok || ap == nil {
		return 0
	}
	evicted := make([]*openAIWSConn, 0)
	ap.mu.Lock()
	for id, conn := range ap.conns {
		if conn == nil {
			delete(ap.conns, id)
			if len(ap.pinnedConns) > 0 {
				delete(ap.pinnedConns, id)
			}
			continue
		}
		if conn.isLeased() || conn.waiters.Load() > 0 || p.isConnPinnedLocked(ap, id) {
			continue
		}
		delete(ap.conns, id)
		if len(ap.pinnedConns) > 0 {
			delete(ap.pinnedConns, id)
		}
		evicted = append(evicted, conn)
	}
	ap.mu.Unlock()
	closeOpenAIWSConns(evicted)
	if len(evicted) > 0 {
		p.metrics.scaleDownTotal.Add(int64(len(evicted)))
	}
	return len(evicted)
}

func (p *openAIWSConnPool) PinConn(accountID int64, connID string) bool {
	if p == nil || accountID <= 0 {
		return false
	}
	connID = stringsTrim(connID)
	if connID == "" {
		return false
	}
	ap, ok := p.getAccountPool(accountID)
	if !ok || ap == nil {
		return false
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if _, exists := ap.conns[connID]; !exists {
		return false
	}
	if ap.pinnedConns == nil {
		ap.pinnedConns = make(map[string]int)
	}
	ap.pinnedConns[connID]++
	return true
}

func (p *openAIWSConnPool) UnpinConn(accountID int64, connID string) {
	if p == nil || accountID <= 0 {
		return
	}
	connID = stringsTrim(connID)
	if connID == "" {
		return
	}
	ap, ok := p.getAccountPool(accountID)
	if !ok || ap == nil {
		return
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if len(ap.pinnedConns) == 0 {
		return
	}
	count := ap.pinnedConns[connID]
	if count <= 1 {
		delete(ap.pinnedConns, connID)
		return
	}
	ap.pinnedConns[connID] = count - 1
}

func (p *openAIWSConnPool) handshakeLimit() int {
	if p != nil && p.cfg != nil && p.cfg.Gateway.OpenAIWS.HandshakeMaxConcurrentPerProxy > 0 {
		return p.cfg.Gateway.OpenAIWS.HandshakeMaxConcurrentPerProxy
	}
	return 0
}

// acquireHandshakeSlot 限制单个出口代理IP的并发握手数。返回 release;超时/取消返回 err。
func (p *openAIWSConnPool) acquireHandshakeSlot(ctx context.Context, proxyURL string) (func(), error) {
	limit := p.handshakeLimit()
	if limit <= 0 {
		return func() {}, nil
	}
	v, _ := p.handshakeSems.LoadOrStore(proxyURL, make(chan struct{}, limit))
	sem := v.(chan struct{})
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// 握手熔断参数:窗口内 403 达阈值 → 该代理进入冷却期,期间新握手直接 fail-fast。
const (
	openAIWSProxy403Window    = 10 * time.Second
	openAIWSProxy403Threshold = 5
	openAIWSProxy403Cooldown  = 45 * time.Second
)

// 全局 egress 握手熔断参数:汇总所有代理的握手 403;窗口内超阈值即判定 Cloudflare
// 对整个出口整体封禁。此时单代理熔断因流量分散到上千代理各自达不到阈值而失效,
// 故用全局闸对所有新握手 fail-fast,彻底打断"全体 403 → 重试雪崩 → 连接/内存风暴"。
// 冷却期结束自然放行探测;任一握手成功即解除(egress 恢复)。
const (
	openAIWSGlobal403Window    = 5 * time.Second
	openAIWSGlobal403Threshold = 40
	openAIWSGlobal403Cooldown  = 20 * time.Second
)

type proxyHandshakeBreaker struct {
	mu          sync.Mutex
	failCount   int
	windowStart time.Time
	openUntil   time.Time
}

// proxyBreakerOpen 判断该代理是否处于握手熔断冷却期(冷却期内不该再打 CF)。
func (p *openAIWSConnPool) proxyBreakerOpen(proxyURL string) bool {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return false
	}
	v, ok := p.proxyBreakers.Load(proxyURL)
	if !ok {
		return false
	}
	b := v.(*proxyHandshakeBreaker)
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Now().Before(b.openUntil)
}

// recordProxyHandshake403 累计该代理握手 403;窗口内超阈值则开启熔断冷却,打断重试雪崩。
func (p *openAIWSConnPool) recordProxyHandshake403(proxyURL string) {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return
	}
	v, _ := p.proxyBreakers.LoadOrStore(proxyURL, &proxyHandshakeBreaker{})
	b := v.(*proxyHandshakeBreaker)
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.Before(b.openUntil) {
		return
	}
	if b.windowStart.IsZero() || now.Sub(b.windowStart) > openAIWSProxy403Window {
		b.windowStart = now
		b.failCount = 0
	}
	b.failCount++
	if b.failCount >= openAIWSProxy403Threshold {
		b.openUntil = now.Add(openAIWSProxy403Cooldown)
		b.failCount = 0
		b.windowStart = time.Time{}
		logOpenAIWSModeInfo("proxy_handshake_breaker_open cooldown_s=%d", int(openAIWSProxy403Cooldown.Seconds()))
	}
}

// resetProxyHandshakeBreaker 握手成功 → 清零该代理的失败计数与熔断。
func (p *openAIWSConnPool) resetProxyHandshakeBreaker(proxyURL string) {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return
	}
	if v, ok := p.proxyBreakers.Load(proxyURL); ok {
		b := v.(*proxyHandshakeBreaker)
		b.mu.Lock()
		b.failCount = 0
		b.windowStart = time.Time{}
		b.openUntil = time.Time{}
		b.mu.Unlock()
	}
}

// globalBreakerOpen 判断全局 egress 握手熔断是否处于冷却期(期间所有新握手直接 fail-fast)。
func (p *openAIWSConnPool) globalBreakerOpen() bool {
	p.globalBreakerMu.Lock()
	defer p.globalBreakerMu.Unlock()
	return time.Now().Before(p.globalBreakerOpenUntil)
}

// recordGlobalHandshake403 汇总所有代理的握手 403;窗口内超阈值 → 开启全局熔断冷却。
func (p *openAIWSConnPool) recordGlobalHandshake403() {
	now := time.Now()
	p.globalBreakerMu.Lock()
	defer p.globalBreakerMu.Unlock()
	if now.Before(p.globalBreakerOpenUntil) {
		return
	}
	if p.global403WindowStart.IsZero() || now.Sub(p.global403WindowStart) > openAIWSGlobal403Window {
		p.global403WindowStart = now
		p.global403Count = 0
	}
	p.global403Count++
	if p.global403Count >= openAIWSGlobal403Threshold {
		p.globalBreakerOpenUntil = now.Add(openAIWSGlobal403Cooldown)
		p.global403Count = 0
		p.global403WindowStart = time.Time{}
		logOpenAIWSModeInfo("global_handshake_breaker_open cooldown_s=%d reason=cloudflare_egress_wide_403", int(openAIWSGlobal403Cooldown.Seconds()))
	}
}

// resetGlobalBreaker 任一握手成功 → 解除全局熔断(egress 已恢复)。
func (p *openAIWSConnPool) resetGlobalBreaker() {
	p.globalBreakerMu.Lock()
	defer p.globalBreakerMu.Unlock()
	if p.global403Count == 0 && p.global403WindowStart.IsZero() && p.globalBreakerOpenUntil.IsZero() {
		return
	}
	p.global403Count = 0
	p.global403WindowStart = time.Time{}
	p.globalBreakerOpenUntil = time.Time{}
}

func (p *openAIWSConnPool) dialConn(ctx context.Context, req openAIWSAcquireRequest) (*openAIWSConn, error) {
	if p == nil || p.clientDialer == nil {
		return nil, errors.New("openai ws client dialer is nil")
	}
	// 全局 egress 熔断:CF 已对整个出口封锁 → 所有新握手直接 fail-fast(不再打 CF、不排握手槽),
	// 防"全体代理被封"雪崩的总闸;单代理熔断在流量分散到上千代理时无法触发。
	if p.globalBreakerOpen() {
		return nil, &openAIWSDialError{StatusCode: http.StatusForbidden, Err: errors.New("global egress handshake circuit open (cloudflare egress-wide 403 cooldown)")}
	}
	// 熔断:该代理正处于 CF 403 冷却期 → 直接 fail-fast,不再打 CF(打断"403→重试→更多403"雪崩)。
	if p.proxyBreakerOpen(req.ProxyURL) {
		return nil, &openAIWSDialError{StatusCode: http.StatusForbidden, Err: errors.New("proxy handshake circuit open (cloudflare 403 cooldown)")}
	}
	// 每出口IP握手并发限制:避免突发狂建握手触发 Cloudflare 403。
	release, err := p.acquireHandshakeSlot(ctx, req.ProxyURL)
	if err != nil {
		return nil, &openAIWSDialError{StatusCode: 0, Err: fmt.Errorf("handshake slot wait: %w", err)}
	}
	defer release()
	conn, status, handshakeHeaders, err := p.clientDialer.Dial(ctx, req.WSURL, req.Headers, req.ProxyURL, req.TLSProfile)
	if err != nil {
		if status == http.StatusForbidden {
			p.recordProxyHandshake403(req.ProxyURL)
			p.recordGlobalHandshake403()
		}
		return nil, &openAIWSDialError{
			StatusCode:      status,
			ResponseHeaders: cloneHeader(handshakeHeaders),
			Err:             err,
		}
	}
	if conn == nil {
		return nil, &openAIWSDialError{
			StatusCode:      status,
			ResponseHeaders: cloneHeader(handshakeHeaders),
			Err:             errors.New("openai ws dialer returned nil connection"),
		}
	}
	// 握手成功 → 清零该代理熔断计数 + 解除全局熔断(CF 已放行,egress 恢复)。
	p.resetProxyHandshakeBreaker(req.ProxyURL)
	p.resetGlobalBreaker()
	id := p.nextConnID(req.Account.ID)
	return newOpenAIWSConn(id, req.Account.ID, conn, handshakeHeaders), nil
}

func (p *openAIWSConnPool) nextConnID(accountID int64) string {
	seq := p.seq.Add(1)
	buf := make([]byte, 0, 32)
	buf = append(buf, "oa_ws_"...)
	buf = strconv.AppendInt(buf, accountID, 10)
	buf = append(buf, '_')
	buf = strconv.AppendUint(buf, seq, 10)
	return string(buf)
}

func (p *openAIWSConnPool) shouldHealthCheckConn(conn *openAIWSConn) bool {
	if conn == nil {
		return false
	}
	return conn.idleDuration(time.Now()) >= openAIWSConnHealthCheckIdle
}

func (p *openAIWSConnPool) maxConnsHardCap() int {
	if p != nil && p.cfg != nil && p.cfg.Gateway.OpenAIWS.MaxConnsPerAccount > 0 {
		return p.cfg.Gateway.OpenAIWS.MaxConnsPerAccount
	}
	return 8
}

func (p *openAIWSConnPool) dynamicMaxConnsEnabled() bool {
	if p != nil && p.cfg != nil {
		return p.cfg.Gateway.OpenAIWS.DynamicMaxConnsByAccountConcurrencyEnabled
	}
	return false
}

func (p *openAIWSConnPool) modeRouterV2Enabled() bool {
	if p != nil && p.cfg != nil {
		return p.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled
	}
	return false
}

func (p *openAIWSConnPool) maxConnsFactorByAccount(account *Account) float64 {
	if p == nil || p.cfg == nil || account == nil {
		return 1.0
	}
	switch account.Type {
	case AccountTypeOAuth:
		if p.cfg.Gateway.OpenAIWS.OAuthMaxConnsFactor > 0 {
			return p.cfg.Gateway.OpenAIWS.OAuthMaxConnsFactor
		}
	case AccountTypeAPIKey:
		if p.cfg.Gateway.OpenAIWS.APIKeyMaxConnsFactor > 0 {
			return p.cfg.Gateway.OpenAIWS.APIKeyMaxConnsFactor
		}
	}
	return 1.0
}

func (p *openAIWSConnPool) effectiveMaxConnsByAccount(account *Account) int {
	hardCap := p.maxConnsHardCap()
	if hardCap <= 0 {
		return 0
	}
	if p.modeRouterV2Enabled() {
		if account == nil {
			return hardCap
		}
		if account.Concurrency <= 0 {
			return 0
		}
		return account.Concurrency
	}
	if account == nil || !p.dynamicMaxConnsEnabled() {
		return hardCap
	}
	if account.Concurrency <= 0 {
		// 0/-1 等“无限制”并发场景下，仍由全局硬上限兜底。
		return hardCap
	}
	factor := p.maxConnsFactorByAccount(account)
	if factor <= 0 {
		factor = 1.0
	}
	effective := int(math.Ceil(float64(account.Concurrency) * factor))
	if effective < 1 {
		effective = 1
	}
	if effective > hardCap {
		effective = hardCap
	}
	return effective
}

func (p *openAIWSConnPool) minIdlePerAccount() int {
	if p != nil && p.cfg != nil && p.cfg.Gateway.OpenAIWS.MinIdlePerAccount >= 0 {
		return p.cfg.Gateway.OpenAIWS.MinIdlePerAccount
	}
	return 0
}

func (p *openAIWSConnPool) maxIdlePerAccount() int {
	if p != nil && p.cfg != nil && p.cfg.Gateway.OpenAIWS.MaxIdlePerAccount >= 0 {
		return p.cfg.Gateway.OpenAIWS.MaxIdlePerAccount
	}
	return 4
}

func (p *openAIWSConnPool) maxConnAge() time.Duration {
	return openAIWSConnMaxAge
}

func (p *openAIWSConnPool) queueLimitPerConn() int {
	if p != nil && p.cfg != nil && p.cfg.Gateway.OpenAIWS.QueueLimitPerConn > 0 {
		return p.cfg.Gateway.OpenAIWS.QueueLimitPerConn
	}
	return 256
}

func (p *openAIWSConnPool) targetUtilization() float64 {
	if p != nil && p.cfg != nil {
		ratio := p.cfg.Gateway.OpenAIWS.PoolTargetUtilization
		if ratio > 0 && ratio <= 1 {
			return ratio
		}
	}
	return 0.7
}

func (p *openAIWSConnPool) prewarmCooldown() time.Duration {
	if p != nil && p.cfg != nil && p.cfg.Gateway.OpenAIWS.PrewarmCooldownMS > 0 {
		return time.Duration(p.cfg.Gateway.OpenAIWS.PrewarmCooldownMS) * time.Millisecond
	}
	return 0
}

func (p *openAIWSConnPool) shouldSuppressPrewarmLocked(ap *openAIWSAccountPool, now time.Time) bool {
	if ap == nil {
		return true
	}
	if ap.prewarmFails <= 0 {
		return false
	}
	if ap.prewarmFailAt.IsZero() {
		ap.prewarmFails = 0
		return false
	}
	if now.Sub(ap.prewarmFailAt) > openAIWSPrewarmFailureWindow {
		ap.prewarmFails = 0
		ap.prewarmFailAt = time.Time{}
		return false
	}
	return ap.prewarmFails >= openAIWSPrewarmFailureSuppress
}

func (p *openAIWSConnPool) dialTimeout() time.Duration {
	if p != nil && p.cfg != nil && p.cfg.Gateway.OpenAIWS.DialTimeoutSeconds > 0 {
		return time.Duration(p.cfg.Gateway.OpenAIWS.DialTimeoutSeconds) * time.Second
	}
	return 10 * time.Second
}

func cloneOpenAIWSAcquireRequest(req openAIWSAcquireRequest) openAIWSAcquireRequest {
	copied := req
	copied.Headers = cloneHeader(req.Headers)
	copied.WSURL = stringsTrim(req.WSURL)
	copied.ProxyURL = stringsTrim(req.ProxyURL)
	copied.PreferredConnID = stringsTrim(req.PreferredConnID)
	return copied
}

func cloneOpenAIWSAcquireRequestPtr(req *openAIWSAcquireRequest) *openAIWSAcquireRequest {
	if req == nil {
		return nil
	}
	copied := cloneOpenAIWSAcquireRequest(*req)
	return &copied
}

func cloneHeader(src http.Header) http.Header {
	if src == nil {
		return nil
	}
	dst := make(http.Header, len(src))
	for k, vals := range src {
		if len(vals) == 0 {
			dst[k] = nil
			continue
		}
		copied := make([]string, len(vals))
		copy(copied, vals)
		dst[k] = copied
	}
	return dst
}

func closeOpenAIWSConns(conns []*openAIWSConn) {
	if len(conns) == 0 {
		return
	}
	for _, conn := range conns {
		if conn == nil {
			continue
		}
		conn.close()
	}
}

func stringsTrim(value string) string {
	return strings.TrimSpace(value)
}
