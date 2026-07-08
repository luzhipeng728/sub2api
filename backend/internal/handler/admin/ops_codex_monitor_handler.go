package admin

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/gin-gonic/gin"
)

// parseCodexWindow parses a "1h"/"6h"/"24h" style window param into a
// time.Duration, defaulting to 1h.
func parseCodexWindow(raw string) (time.Duration, bool) {
	switch raw {
	case "", "1h":
		return time.Hour, true
	case "6h":
		return 6 * time.Hour, true
	case "24h":
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}

// GetCodexOverview returns aggregate Codex (OpenAI) traffic: total traffic,
// success count, accounts used, RPM, blocked-reason breakdown, vistara cost.
// GET /api/v1/admin/ops/codex/overview?window=1h|6h|24h
func (h *OpsHandler) GetCodexOverview(c *gin.Context) {
	if h.opsService == nil {
		response.Error(c, http.StatusServiceUnavailable, "Ops service not available")
		return
	}
	if err := h.opsService.RequireMonitoringEnabled(c.Request.Context()); err != nil {
		response.ErrorFrom(c, err)
		return
	}

	window, ok := parseCodexWindow(c.Query("window"))
	if !ok {
		response.BadRequest(c, "Invalid window")
		return
	}

	until := time.Now().UTC()
	since := until.Add(-window)

	overview, err := h.opsService.GetCodexOverview(c.Request.Context(), since, until)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, overview)
}

// GetCodexAccounts returns per-account Codex status + traffic.
// GET /api/v1/admin/ops/codex/accounts?window=1h|6h|24h
func (h *OpsHandler) GetCodexAccounts(c *gin.Context) {
	if h.opsService == nil {
		response.Error(c, http.StatusServiceUnavailable, "Ops service not available")
		return
	}
	if err := h.opsService.RequireMonitoringEnabled(c.Request.Context()); err != nil {
		response.ErrorFrom(c, err)
		return
	}

	window, ok := parseCodexWindow(c.Query("window"))
	if !ok {
		response.BadRequest(c, "Invalid window")
		return
	}

	until := time.Now().UTC()
	since := until.Add(-window)

	accounts, err := h.opsService.GetCodexAccounts(c.Request.Context(), since, until)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"accounts": accounts})
}

// GetCodexRuntimeSeries returns memory/goroutine/WS-pool time series for the
// memory-leak diagnostic chart.
// GET /api/v1/admin/ops/codex/runtime-series?hours=6
func (h *OpsHandler) GetCodexRuntimeSeries(c *gin.Context) {
	if h.opsService == nil {
		response.Error(c, http.StatusServiceUnavailable, "Ops service not available")
		return
	}
	if err := h.opsService.RequireMonitoringEnabled(c.Request.Context()); err != nil {
		response.ErrorFrom(c, err)
		return
	}

	hoursRaw := c.DefaultQuery("hours", "6")
	hours, err := strconv.ParseFloat(hoursRaw, 64)
	if err != nil || hours <= 0 || hours > 168 {
		response.BadRequest(c, "Invalid hours")
		return
	}

	since := time.Now().UTC().Add(-time.Duration(hours * float64(time.Hour)))

	series, err := h.opsService.GetCodexRuntimeSeries(c.Request.Context(), since)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"series": series})
}
