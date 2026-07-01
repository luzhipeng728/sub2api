package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

func TestAcquireHandshakeSlot_Caps(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.HandshakeMaxConcurrentPerProxy = 2
	p := &openAIWSConnPool{cfg: cfg}
	ctx := context.Background()

	r1, err := p.acquireHandshakeSlot(ctx, "proxyA")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := p.acquireHandshakeSlot(ctx, "proxyA")
	if err != nil {
		t.Fatal(err)
	}
	// 第3个应阻塞 → 用短超时 ctx 触发 err
	tctx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	if _, err := p.acquireHandshakeSlot(tctx, "proxyA"); err == nil {
		t.Fatal("超过上限应阻塞/超时")
	}
	// 另一个代理IP不受影响
	if _, err := p.acquireHandshakeSlot(ctx, "proxyB"); err != nil {
		t.Fatal("不同代理应独立计数")
	}
	// 释放一个后可再获取
	r1()
	if _, err := p.acquireHandshakeSlot(ctx, "proxyA"); err != nil {
		t.Fatal("释放后应可再获取")
	}
	r2()
}

func TestAcquireHandshakeSlot_Disabled(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.HandshakeMaxConcurrentPerProxy = 0 // 禁用
	p := &openAIWSConnPool{cfg: cfg}
	for i := 0; i < 100; i++ {
		if _, err := p.acquireHandshakeSlot(context.Background(), "x"); err != nil {
			t.Fatal("禁用时不应限制")
		}
	}
}
