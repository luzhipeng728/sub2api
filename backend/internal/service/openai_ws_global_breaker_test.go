package service

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// TestGlobalHandshakeBreaker 验证全局 egress 握手熔断:
// 单代理熔断在流量分散到上千代理时各自达不到阈值而失效;全局熔断汇总所有代理的 403,
// 超阈值即对所有新握手 fail-fast,解决"CF 整体封出口"的雪崩。
func TestGlobalHandshakeBreaker(t *testing.T) {
	p := &openAIWSConnPool{cfg: &config.Config{}}

	if p.globalBreakerOpen() {
		t.Fatal("初始不应熔断")
	}
	// 未达阈值:不熔断(403 可来自不同代理,汇总到全局)
	for i := 0; i < openAIWSGlobal403Threshold-1; i++ {
		p.recordGlobalHandshake403()
	}
	if p.globalBreakerOpen() {
		t.Fatalf("低于阈值(%d)不应熔断", openAIWSGlobal403Threshold)
	}
	// 达阈值:全局熔断
	p.recordGlobalHandshake403()
	if !p.globalBreakerOpen() {
		t.Fatal("全局达阈值应熔断")
	}
	// 任一握手成功 → 解除(egress 恢复)
	p.resetGlobalBreaker()
	if p.globalBreakerOpen() {
		t.Fatal("reset 后不应熔断")
	}
}
