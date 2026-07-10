package management

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
)

type proxyTestRequest struct {
	ProxyURL *string `json:"proxy_url"`
	Provider string  `json:"provider"`
	AuthFile string  `json:"auth_file"`
}

func validateCodexProxyCandidate(cfg *config.Config, raw string, explicit bool) error {
	raw = strings.TrimSpace(raw)
	if explicit && cfg != nil && cfg.CodexProxyRequired && raw == "" {
		return proxyutil.NewError(proxyutil.CodeRequired, proxyutil.StageConfig, false, "", "Codex proxy is required; proxy_url cannot be empty", nil)
	}
	if raw == "" {
		return nil
	}
	setting, errParse := proxyutil.Parse(raw)
	if errParse != nil {
		return errParse
	}
	if cfg != nil && cfg.CodexProxyRequired && setting.Mode != proxyutil.ModeProxy {
		return proxyutil.NewError(proxyutil.CodeRequired, proxyutil.StageConfig, false, "", "Codex proxy is required; direct proxy mode is not allowed", nil)
	}
	return nil
}

func writeProxyValidationError(c *gin.Context, err error) {
	if proxyErr, ok := proxyutil.AsError(err); ok {
		c.JSON(proxyTestHTTPStatus(proxyErr.Code, false), gin.H{
			"ok":         false,
			"code":       proxyErr.Code,
			"stage":      proxyErr.Stage,
			"message":    proxyErr.Message,
			"proxy_hash": proxyErr.ProxyHash,
		})
		return
	}
	c.JSON(http.StatusBadRequest, gin.H{"ok": false, "code": proxyutil.CodeConfigInvalid, "stage": proxyutil.StageConfig, "message": fmt.Sprint(err)})
}

func (h *Handler) TestProxy(c *gin.Context) {
	var req proxyTestRequest
	if errBind := c.ShouldBindJSON(&req); errBind != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "code": "proxy_test_request_invalid", "message": "invalid request body"})
		return
	}

	provider := strings.ToLower(strings.TrimSpace(req.Provider))
	proxyURL := ""
	if req.ProxyURL != nil {
		proxyURL = strings.TrimSpace(*req.ProxyURL)
	}
	var auth *coreauth.Auth
	if strings.TrimSpace(req.AuthFile) != "" && h.authManager != nil {
		auth = findAuthByNameOrID(h.authManager, req.AuthFile)
		if auth != nil {
			if provider == "" {
				provider = strings.ToLower(strings.TrimSpace(auth.Provider))
			}
			if req.ProxyURL == nil {
				proxyURL = strings.TrimSpace(auth.ProxyURL)
			}
		}
	}
	if provider == "" {
		provider = "codex"
	}
	if provider != "codex" {
		c.JSON(http.StatusOK, gin.H{"ok": true, "code": "proxy_test_not_applicable", "proxy_mode": "not_applicable"})
		return
	}

	h.mu.Lock()
	cfg := h.cfg
	h.mu.Unlock()
	result := helps.RunCodexProxyTest(c.Request.Context(), cfg, proxyURL)
	status := proxyTestHTTPStatus(result.Code, result.OK)
	c.JSON(status, result)
}

func findAuthByNameOrID(manager *coreauth.Manager, name string) *coreauth.Auth {
	if manager == nil {
		return nil
	}
	name = strings.TrimSpace(name)
	if auth, ok := manager.GetByID(name); ok {
		return auth
	}
	for _, auth := range manager.List() {
		if auth != nil && strings.EqualFold(strings.TrimSpace(auth.FileName), name) {
			return auth
		}
	}
	return nil
}

func proxyTestHTTPStatus(code string, ok bool) int {
	if ok {
		return http.StatusOK
	}
	switch code {
	case proxyutil.CodeConfigInvalid:
		return http.StatusBadRequest
	case proxyutil.CodeRequired:
		return http.StatusPreconditionFailed
	case proxyutil.CodeConnectTimeout, proxyutil.CodeTLSTimeout, proxyutil.CodeUpstreamHeaderTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}
