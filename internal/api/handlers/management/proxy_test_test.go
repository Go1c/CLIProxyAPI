package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestProxyTestRejectsInvalidURLWithoutExposingCredentials(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v0/management/proxy/test", strings.NewReader(`{"proxy_url":"socks5:user:pass@host:443","provider":"codex"}`))
	request.Header.Set("Content-Type", "application/json")
	ctx.Request = request

	handler.TestProxy(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "user") || strings.Contains(recorder.Body.String(), "pass") {
		t.Fatalf("response exposed proxy credentials: %s", recorder.Body.String())
	}
}

func TestProxyTestAllowsEmptyDirectWhenNotRequired(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v0/management/proxy/test", strings.NewReader(`{"proxy_url":"","provider":"codex"}`))
	request.Header.Set("Content-Type", "application/json")
	ctx.Request = request

	handler.TestProxy(ctx)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "proxy_test_direct_allowed") {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
}
