package service

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

func TestProxyHandshakeBreaker(t *testing.T) {
	p := &openAIWSConnPool{cfg: &config.Config{}}
	const px = "http://u:pw@proxy.example.com:7085"

	if p.proxyBreakerOpen(px) {
		t.Fatal("初始不应熔断")
	}
	// 未达阈值:不熔断
	for i := 0; i < openAIWSProxy403Threshold-1; i++ {
		p.recordProxyHandshake403(px)
	}
	if p.proxyBreakerOpen(px) {
		t.Fatalf("低于阈值(%d)不应熔断", openAIWSProxy403Threshold)
	}
	// 达阈值:熔断
	p.recordProxyHandshake403(px)
	if !p.proxyBreakerOpen(px) {
		t.Fatal("达阈值应熔断")
	}
	// 成功握手:清零
	p.resetProxyHandshakeBreaker(px)
	if p.proxyBreakerOpen(px) {
		t.Fatal("reset 后不应熔断")
	}
	// 空 proxyURL 安全
	if p.proxyBreakerOpen("") {
		t.Fatal("空 proxy 不应熔断")
	}
	p.recordProxyHandshake403("")
	p.resetProxyHandshakeBreaker("")
}
