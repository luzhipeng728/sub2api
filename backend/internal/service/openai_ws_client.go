package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const openAIWSMessageReadLimitBytes int64 = 16 * 1024 * 1024
const (
	openAIWSProxyTransportMaxIdleConns        = 128
	openAIWSProxyTransportMaxIdleConnsPerHost = 64
	openAIWSProxyTransportIdleConnTimeout     = 90 * time.Second
	openAIWSProxyClientCacheMaxEntries        = 256
	openAIWSProxyClientCacheIdleTTL           = 15 * time.Minute
)

type OpenAIWSTransportMetricsSnapshot struct {
	ProxyClientCacheHits   int64   `json:"proxy_client_cache_hits"`
	ProxyClientCacheMisses int64   `json:"proxy_client_cache_misses"`
	TransportReuseRatio    float64 `json:"transport_reuse_ratio"`
}

// openAIWSClientConn 抽象 WS 客户端连接，便于替换底层实现。
type openAIWSClientConn interface {
	WriteJSON(ctx context.Context, value any) error
	ReadMessage(ctx context.Context) ([]byte, error)
	Ping(ctx context.Context) error
	Close() error
}

// openAIWSClientDialer 抽象 WS 建连器。
// profile 非空时用 utls 伪装 TLS 指纹(与 HTTP 上游路一致),绕过 Cloudflare 对 WS 握手的指纹拦截;
// 为 nil 时维持 Go 原生 TLS 行为(账号未开启 TLS 指纹伪装)。
type openAIWSClientDialer interface {
	Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string, profile *tlsfingerprint.Profile) (openAIWSClientConn, int, http.Header, error)
}

type openAIWSTransportMetricsDialer interface {
	SnapshotTransportMetrics() OpenAIWSTransportMetricsSnapshot
}

func newDefaultOpenAIWSClientDialer() openAIWSClientDialer {
	return &coderOpenAIWSClientDialer{
		proxyClients: make(map[string]*openAIWSProxyClientEntry),
	}
}

type coderOpenAIWSClientDialer struct {
	proxyMu      sync.Mutex
	proxyClients map[string]*openAIWSProxyClientEntry
	proxyHits    atomic.Int64
	proxyMisses  atomic.Int64
}

type openAIWSProxyClientEntry struct {
	client           *http.Client
	lastUsedUnixNano int64
}

func (d *coderOpenAIWSClientDialer) Dial(
	ctx context.Context,
	wsURL string,
	headers http.Header,
	proxyURL string,
	profile *tlsfingerprint.Profile,
) (openAIWSClientConn, int, http.Header, error) {
	targetURL := strings.TrimSpace(wsURL)
	if targetURL == "" {
		return nil, 0, nil, errors.New("ws url is empty")
	}

	opts := &coderws.DialOptions{
		HTTPHeader:      cloneHeader(headers),
		CompressionMode: coderws.CompressionContextTakeover,
	}
	// 走代理、或需要 TLS 指纹伪装(profile 非空)时,都需要自定义 HTTPClient 承载握手:
	//   - 代理:CONNECT/SOCKS5 隧道;
	//   - 指纹伪装:transport.DialTLSContext 换成 utls 拨号器(WS 走 h1 Upgrade)。
	// 两者都不需要时,保持 coder/websocket 默认(Go 原生 TLS 直连),行为不变。
	proxy := strings.TrimSpace(proxyURL)
	if proxy != "" || profile != nil {
		handshakeClient, err := d.handshakeHTTPClient(proxy, profile)
		if err != nil {
			return nil, 0, nil, err
		}
		opts.HTTPClient = handshakeClient
	}

	conn, resp, err := coderws.Dial(ctx, targetURL, opts)
	if err != nil {
		status := 0
		respHeaders := http.Header(nil)
		if resp != nil {
			status = resp.StatusCode
			respHeaders = cloneHeader(resp.Header)
		}
		return nil, status, respHeaders, err
	}
	// coder/websocket 默认单消息读取上限为 32KB，Codex WS 事件（如 rate_limits/大 delta）
	// 可能超过该阈值，需显式提高上限，避免本地 read_fail(message too big)。
	conn.SetReadLimit(openAIWSMessageReadLimitBytes)
	respHeaders := http.Header(nil)
	if resp != nil {
		respHeaders = cloneHeader(resp.Header)
	}
	return &coderOpenAIWSClientConn{conn: conn}, 0, respHeaders, nil
}

// handshakeHTTPClient 返回用于 WS 握手的 *http.Client,按 (proxy + 指纹profile) 缓存复用。
// proxy 为空代表直连(此时必然是因为需要指纹伪装才会走到这里)。
func (d *coderOpenAIWSClientDialer) handshakeHTTPClient(proxy string, profile *tlsfingerprint.Profile) (*http.Client, error) {
	if d == nil {
		return nil, errors.New("openai ws dialer is nil")
	}
	normalizedProxy := strings.TrimSpace(proxy)
	var parsedProxyURL *url.URL
	if normalizedProxy != "" {
		parsed, err := url.Parse(normalizedProxy)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy url: %w", err)
		}
		parsedProxyURL = parsed
	}
	// 不同指纹的 client 不能混用,缓存 key 需带上 profile 名称。
	cacheKey := normalizedProxy + "\x00" + openAIWSProfileCacheKey(profile)
	now := time.Now().UnixNano()

	d.proxyMu.Lock()
	defer d.proxyMu.Unlock()
	if entry, ok := d.proxyClients[cacheKey]; ok && entry != nil && entry.client != nil {
		entry.lastUsedUnixNano = now
		d.proxyHits.Add(1)
		return entry.client, nil
	}
	d.cleanupProxyClientsLocked(now)
	transport, err := buildOpenAIWSHandshakeTransport(parsedProxyURL, profile)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: transport}
	d.proxyClients[cacheKey] = &openAIWSProxyClientEntry{
		client:           client,
		lastUsedUnixNano: now,
	}
	d.ensureProxyClientCapacityLocked()
	d.proxyMisses.Add(1)
	return client, nil
}

func openAIWSProfileCacheKey(profile *tlsfingerprint.Profile) string {
	if profile == nil {
		return ""
	}
	return "fp:" + profile.Name
}

// buildOpenAIWSHandshakeTransport 构建 WS 握手用的 Transport。
// profile 非空:用 utls dialer 挂 DialTLSContext 伪装 TLS 指纹(WS 是 HTTP/1.1 Upgrade,强制禁用 h2):
//   - 直连:tlsfingerprint.NewDialer
//   - http/https 代理:tlsfingerprint.NewHTTPProxyDialer(CONNECT 隧道 + utls 握手)
//   - socks5 代理:tlsfingerprint.NewSOCKS5ProxyDialer
//
// profile 为空:维持原生 TLS 行为,仅按需设置 http 代理(与改动前一致)。
func buildOpenAIWSHandshakeTransport(proxyURL *url.URL, profile *tlsfingerprint.Profile) (*http.Transport, error) {
	transport := &http.Transport{
		MaxIdleConns:        openAIWSProxyTransportMaxIdleConns,
		MaxIdleConnsPerHost: openAIWSProxyTransportMaxIdleConnsPerHost,
		IdleConnTimeout:     openAIWSProxyTransportIdleConnTimeout,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	if profile == nil {
		transport.ForceAttemptHTTP2 = true
		if proxyURL != nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
		return transport, nil
	}
	// WS 握手是 HTTP/1.1 Upgrade,必须禁用 h2,并由 utls 接管 TLS 握手。
	transport.ForceAttemptHTTP2 = false
	if proxyURL == nil {
		transport.DialTLSContext = tlsfingerprint.NewDialer(profile, nil).DialTLSContext
		return transport, nil
	}
	switch strings.ToLower(proxyURL.Scheme) {
	case "socks5", "socks5h":
		transport.DialTLSContext = tlsfingerprint.NewSOCKS5ProxyDialer(profile, proxyURL).DialTLSContext
	case "http", "https":
		transport.DialTLSContext = tlsfingerprint.NewHTTPProxyDialer(profile, proxyURL).DialTLSContext
	default:
		// 未知代理类型:回退为普通代理(无指纹伪装)。
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	return transport, nil
}

func (d *coderOpenAIWSClientDialer) cleanupProxyClientsLocked(nowUnixNano int64) {
	if d == nil || len(d.proxyClients) == 0 {
		return
	}
	idleTTL := openAIWSProxyClientCacheIdleTTL
	if idleTTL <= 0 {
		return
	}
	now := time.Unix(0, nowUnixNano)
	for key, entry := range d.proxyClients {
		if entry == nil || entry.client == nil {
			delete(d.proxyClients, key)
			continue
		}
		lastUsed := time.Unix(0, entry.lastUsedUnixNano)
		if now.Sub(lastUsed) > idleTTL {
			closeOpenAIWSProxyClient(entry.client)
			delete(d.proxyClients, key)
		}
	}
}

func (d *coderOpenAIWSClientDialer) ensureProxyClientCapacityLocked() {
	if d == nil {
		return
	}
	maxEntries := openAIWSProxyClientCacheMaxEntries
	if maxEntries <= 0 {
		return
	}
	for len(d.proxyClients) > maxEntries {
		var oldestKey string
		var oldestLastUsed int64
		hasOldest := false
		for key, entry := range d.proxyClients {
			lastUsed := int64(0)
			if entry != nil {
				lastUsed = entry.lastUsedUnixNano
			}
			if !hasOldest || lastUsed < oldestLastUsed {
				hasOldest = true
				oldestKey = key
				oldestLastUsed = lastUsed
			}
		}
		if !hasOldest {
			return
		}
		if entry := d.proxyClients[oldestKey]; entry != nil {
			closeOpenAIWSProxyClient(entry.client)
		}
		delete(d.proxyClients, oldestKey)
	}
}

func closeOpenAIWSProxyClient(client *http.Client) {
	if client == nil || client.Transport == nil {
		return
	}
	if transport, ok := client.Transport.(*http.Transport); ok && transport != nil {
		transport.CloseIdleConnections()
	}
}

func (d *coderOpenAIWSClientDialer) SnapshotTransportMetrics() OpenAIWSTransportMetricsSnapshot {
	if d == nil {
		return OpenAIWSTransportMetricsSnapshot{}
	}
	hits := d.proxyHits.Load()
	misses := d.proxyMisses.Load()
	total := hits + misses
	reuseRatio := 0.0
	if total > 0 {
		reuseRatio = float64(hits) / float64(total)
	}
	return OpenAIWSTransportMetricsSnapshot{
		ProxyClientCacheHits:   hits,
		ProxyClientCacheMisses: misses,
		TransportReuseRatio:    reuseRatio,
	}
}

type coderOpenAIWSClientConn struct {
	conn *coderws.Conn
}

var _ openaiwsv2.FrameConn = (*coderOpenAIWSClientConn)(nil)

func (c *coderOpenAIWSClientConn) WriteJSON(ctx context.Context, value any) error {
	if c == nil || c.conn == nil {
		return errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return wsjson.Write(ctx, c.conn, value)
}

func (c *coderOpenAIWSClientConn) ReadMessage(ctx context.Context) ([]byte, error) {
	if c == nil || c.conn == nil {
		return nil, errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	msgType, payload, err := c.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	switch msgType {
	case coderws.MessageText, coderws.MessageBinary:
		return payload, nil
	default:
		return nil, errOpenAIWSConnClosed
	}
}

func (c *coderOpenAIWSClientConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	if c == nil || c.conn == nil {
		return coderws.MessageText, nil, errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	msgType, payload, err := c.conn.Read(ctx)
	if err != nil {
		return coderws.MessageText, nil, err
	}
	return msgType, payload, nil
}

func (c *coderOpenAIWSClientConn) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	if c == nil || c.conn == nil {
		return errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return c.conn.Write(ctx, msgType, payload)
}

func (c *coderOpenAIWSClientConn) Ping(ctx context.Context) error {
	if c == nil || c.conn == nil {
		return errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return c.conn.Ping(ctx)
}

func (c *coderOpenAIWSClientConn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	// Close 为幂等，忽略重复关闭错误。
	_ = c.conn.Close(coderws.StatusNormalClosure, "")
	_ = c.conn.CloseNow()
	return nil
}
