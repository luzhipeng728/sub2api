package admin

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGetCodexOverviewReturns503WhenOpsServiceNil(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewOpsHandler(nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/v1/admin/ops/codex/overview", nil)

	h.GetCodexOverview(c)

	if w.Code != 503 {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}
