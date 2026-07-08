package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	opsVistaraCollectorInterval      = 60 * time.Second
	opsVistaraCollectorTimeout       = 15 * time.Second
	opsVistaraCollectorLeaderLockKey = "ops:vistara:collector:leader"
	opsVistaraCollectorLeaderLockTTL = 90 * time.Second
)

var opsVistaraCollectorAdvisoryLockID = hashAdvisoryLockID(opsVistaraCollectorLeaderLockKey)

type OpsVistaraCollector struct {
	opsRepo     OpsRepository
	cfg         *config.Config
	redisClient *redis.Client
	instanceID  string
	httpClient  *http.Client

	stopCh    chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
}

func NewOpsVistaraCollector(opsRepo OpsRepository, cfg *config.Config, redisClient *redis.Client, instanceID string) *OpsVistaraCollector {
	return &OpsVistaraCollector{
		opsRepo:     opsRepo,
		cfg:         cfg,
		redisClient: redisClient,
		instanceID:  instanceID,
		httpClient:  &http.Client{Timeout: opsVistaraCollectorTimeout},
	}
}

func (c *OpsVistaraCollector) Start() {
	if c == nil {
		return
	}
	c.startOnce.Do(func() {
		if c.stopCh == nil {
			c.stopCh = make(chan struct{})
		}
		go c.run()
	})
}

func (c *OpsVistaraCollector) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		if c.stopCh != nil {
			close(c.stopCh)
		}
	})
}

func (c *OpsVistaraCollector) run() {
	ticker := time.NewTicker(opsVistaraCollectorInterval)
	defer ticker.Stop()
	for {
		if err := c.collectOnce(context.Background()); err != nil {
			log.Printf("[OpsVistaraCollector] collect failed: %v", err)
		}
		select {
		case <-ticker.C:
		case <-c.stopCh:
			return
		}
	}
}

func (c *OpsVistaraCollector) collectOnce(ctx context.Context) error {
	if c == nil || c.opsRepo == nil || c.cfg == nil {
		return nil
	}
	if c.cfg.Vistara.BaseURL == "" || c.cfg.Vistara.APIToken == "" || c.cfg.Vistara.ChannelID == 0 {
		return nil // not configured; no-op
	}

	usedQuota, err := c.fetchUsedQuota(ctx)
	if err != nil {
		return err
	}
	if usedQuota == nil {
		return nil
	}
	return c.opsRepo.InsertVistaraQuotaSample(ctx, *usedQuota)
}

func (c *OpsVistaraCollector) fetchUsedQuota(ctx context.Context) (*int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.Vistara.BaseURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Vistara.APIToken)
	req.Header.Set("new-api-user", "1")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vistara api status %d", resp.StatusCode)
	}

	var parsed struct {
		Data struct {
			Items []struct {
				ID        int64 `json:"id"`
				UsedQuota int64 `json:"used_quota"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	for _, item := range parsed.Data.Items {
		if item.ID == c.cfg.Vistara.ChannelID {
			q := item.UsedQuota
			return &q, nil
		}
	}
	return nil, nil
}
