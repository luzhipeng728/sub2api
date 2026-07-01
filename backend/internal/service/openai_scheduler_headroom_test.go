package service

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

func headroomTestScheduler() *defaultOpenAIAccountScheduler {
	cfg := &config.Config{}
	cfg.Gateway.Scheduling.CodexHeadroomAware = true
	cfg.Gateway.Scheduling.Codex5hSoftLimit = 95
	cfg.Gateway.Scheduling.Codex7dSoftLimit = 99
	return &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: cfg}}
}

func TestCodexUsedPercentFromExtra(t *testing.T) {
	a := &Account{Extra: map[string]any{"codex_5h_used_percent": float64(100), "codex_7d_used_percent": 32.0}}
	if got := codexUsedPercentFromExtra(a, "codex_5h_used_percent"); got != 100 {
		t.Fatalf("5h = %v, want 100", got)
	}
	if got := codexUsedPercentFromExtra(a, "codex_7d_used_percent"); got != 32 {
		t.Fatalf("7d = %v, want 32", got)
	}
	if got := codexUsedPercentFromExtra(a, "missing"); got != -1 {
		t.Fatalf("missing = %v, want -1", got)
	}
	if got := codexUsedPercentFromExtra(&Account{}, "codex_5h_used_percent"); got != -1 {
		t.Fatalf("nil extra = %v, want -1", got)
	}
}

func TestIsCodexAccountMaxed(t *testing.T) {
	s := headroomTestScheduler()
	if !s.isCodexAccountMaxed(&Account{Extra: map[string]any{"codex_5h_used_percent": 100.0, "codex_7d_used_percent": 32.0}}) {
		t.Fatal("5h=100 应判为 maxed")
	}
	if s.isCodexAccountMaxed(&Account{Extra: map[string]any{"codex_5h_used_percent": 60.0, "codex_7d_used_percent": 10.0}}) {
		t.Fatal("5h=60 应判为有余量")
	}
	if !s.isCodexAccountMaxed(&Account{Extra: map[string]any{"codex_5h_used_percent": 10.0, "codex_7d_used_percent": 99.5}}) {
		t.Fatal("7d=99.5 应判为 maxed")
	}
	if s.isCodexAccountMaxed(&Account{}) {
		t.Fatal("无用量数据不应判为 maxed")
	}
}
