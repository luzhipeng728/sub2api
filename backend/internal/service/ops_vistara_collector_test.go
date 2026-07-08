package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type fakeVistaraOpsRepo struct {
	OpsRepository
	inserted []int64
}

func (f *fakeVistaraOpsRepo) InsertVistaraQuotaSample(ctx context.Context, usedQuota int64) error {
	f.inserted = append(f.inserted, usedQuota)
	return nil
}

func TestOpsVistaraCollectorPollsAndInsertsQuota(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"items": []map[string]any{
					{"id": float64(42), "used_quota": float64(7_000_000)},
				},
			},
		})
	}))
	defer srv.Close()

	repo := &fakeVistaraOpsRepo{}
	cfg := &config.Config{
		Vistara: config.VistaraConfig{
			BaseURL:   srv.URL,
			APIToken:  "test-token",
			ChannelID: 42,
		},
	}

	c := NewOpsVistaraCollector(repo, cfg, nil, "test-instance")
	if err := c.collectOnce(context.Background()); err != nil {
		t.Fatalf("collectOnce returned error: %v", err)
	}
	if len(repo.inserted) != 1 || repo.inserted[0] != 7_000_000 {
		t.Fatalf("inserted = %v, want [7000000]", repo.inserted)
	}
}
