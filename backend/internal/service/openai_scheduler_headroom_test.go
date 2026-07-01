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

func TestApplyProvenAliveFailover(t *testing.T) {
	mk := func(id int64, hasTTFT bool, errorRate float64, load int) openAIAccountCandidateScore {
		return openAIAccountCandidateScore{
			account:   &Account{ID: id},
			loadInfo:  &AccountLoadInfo{LoadRate: load},
			hasTTFT:   hasTTFT,
			errorRate: errorRate,
		}
	}
	// [0]=首选(headroom,假设已死 id=1)必须保留;尾段(1+)期望按 proven-alive 重排:
	// 有出活(hasTTFT)优先 → 其中错误率低优先 → 无出活的排最后。
	order := []openAIAccountCandidateScore{
		mk(1, false, 0.9, 0),  // 首选,保留在 [0]
		mk(2, false, 0.1, 50), // 无 TTFT(近期没出活)
		mk(3, true, 0.5, 50),  // 有 TTFT,错误率 0.5
		mk(4, true, 0.1, 50),  // 有 TTFT,错误率 0.1 → 尾段最前
	}
	out := applyProvenAliveFailover(order)
	got := []int64{out[0].account.ID, out[1].account.ID, out[2].account.ID, out[3].account.ID}
	want := []int64{1, 4, 3, 2}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("proven-alive 排序错误: got %v want %v", got, want)
		}
	}
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
