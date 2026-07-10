package helps

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
)

const codexProxyTestTarget = "https://chatgpt.com/backend-api/codex/models"

type CodexProxyTestTimings struct {
	ProxyConnect int64 `json:"proxy_connect"`
	TLSHandshake int64 `json:"tls_handshake"`
	FirstByte    int64 `json:"first_byte"`
	Total        int64 `json:"total"`
}

type CodexProxyTestResult struct {
	OK            bool                  `json:"ok"`
	Code          string                `json:"code"`
	Stage         string                `json:"stage,omitempty"`
	Message       string                `json:"message,omitempty"`
	Proxy         string                `json:"proxy,omitempty"`
	ProxyHash     string                `json:"proxy_hash,omitempty"`
	ProxyMode     string                `json:"proxy_mode"`
	TargetStatus  int                   `json:"target_status,omitempty"`
	CloudflarePOP string                `json:"cloudflare_pop,omitempty"`
	TimingsMS     CodexProxyTestTimings `json:"timings_ms"`
}

func RunCodexProxyTest(ctx context.Context, cfg *config.Config, proxyURL string) CodexProxyTestResult {
	if ctx == nil {
		ctx = context.Background()
	}
	effectiveProxyURL := strings.TrimSpace(proxyURL)
	if effectiveProxyURL == "" && cfg != nil {
		effectiveProxyURL = strings.TrimSpace(cfg.ProxyURL)
	}
	setting, errParse := validateCodexProxySetting(effectiveProxyURL, cfg != nil && cfg.CodexProxyRequired)
	if errParse != nil {
		return codexProxyTestFailure(errParse, effectiveProxyURL)
	}
	if setting.Mode != proxyutil.ModeProxy {
		return CodexProxyTestResult{
			OK:        true,
			Code:      "proxy_test_direct_allowed",
			ProxyMode: "direct",
		}
	}

	proxyHash := proxyutil.Hash(effectiveProxyURL)
	result := CodexProxyTestResult{
		Proxy:     proxyutil.Redact(effectiveProxyURL),
		ProxyHash: proxyHash,
		ProxyMode: "proxy",
	}
	timings := &CodexProxyTimings{}
	auth := &cliproxyauth.Auth{ProxyURL: effectiveProxyURL}
	startedAt := time.Now()
	client, errClient := newCodexRustlsHTTPClient(ctx, cfg, auth, timings)
	if errClient != nil {
		return codexProxyTestFailure(errClient, effectiveProxyURL)
	}
	req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, codexProxyTestTarget, nil)
	if errRequest != nil {
		return CodexProxyTestResult{OK: false, Code: "proxy_test_request_invalid", Stage: proxyutil.StageConfig, Message: "failed to create proxy test request", Proxy: result.Proxy, ProxyHash: proxyHash, ProxyMode: "proxy"}
	}
	response, errDo := client.Do(req)
	firstByte := time.Since(startedAt)
	proxyConnect, tlsHandshake := timings.Snapshot()
	result.TimingsMS = CodexProxyTestTimings{
		ProxyConnect: proxyConnect.Milliseconds(),
		TLSHandshake: tlsHandshake.Milliseconds(),
		FirstByte:    firstByte.Milliseconds(),
		Total:        time.Since(startedAt).Milliseconds(),
	}
	if errDo != nil {
		failure := codexProxyTestFailure(errDo, effectiveProxyURL)
		failure.TimingsMS = result.TimingsMS
		return failure
	}
	defer func() { _ = response.Body.Close() }()
	result.TargetStatus = response.StatusCode
	result.CloudflarePOP = cloudflarePOP(response.Header)
	result.TimingsMS.Total = time.Since(startedAt).Milliseconds()
	if response.StatusCode == http.StatusProxyAuthRequired {
		failure := codexProxyTestFailure(proxyutil.NewError(proxyutil.CodeAuthFailed, proxyutil.StageProxyConnect, true, proxyHash, "proxy authentication failed", nil), effectiveProxyURL)
		failure.TargetStatus = response.StatusCode
		failure.TimingsMS = result.TimingsMS
		return failure
	}
	if response.StatusCode >= http.StatusInternalServerError {
		result.Code = "proxy_test_upstream_5xx"
		result.Stage = proxyutil.StageUpstreamHeader
		result.Message = fmt.Sprintf("proxy reached upstream but target returned HTTP %d", response.StatusCode)
		return result
	}

	result.OK = true
	result.Code = "proxy_test_ok"
	proxyutil.MarkVerified(proxyHash, result.CloudflarePOP, time.Since(startedAt), time.Now())
	return result
}

func codexProxyTestFailure(err error, proxyURL string) CodexProxyTestResult {
	result := CodexProxyTestResult{
		OK:        false,
		Code:      "proxy_test_failed",
		Message:   "proxy test failed",
		Proxy:     proxyutil.Redact(proxyURL),
		ProxyHash: proxyutil.Hash(proxyURL),
		ProxyMode: "proxy",
	}
	if strings.TrimSpace(proxyURL) == "" {
		result.Proxy = ""
		result.ProxyMode = "direct"
	}
	if proxyErr, ok := proxyutil.AsError(err); ok {
		result.Code = proxyErr.Code
		result.Stage = proxyErr.Stage
		result.Message = proxyErr.Message
		result.ProxyHash = proxyErr.ProxyHash
		if result.ProxyHash == "" {
			result.ProxyHash = proxyutil.Hash(proxyURL)
		}
	}
	return result
}

func cloudflarePOP(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if pop := strings.TrimSpace(headers.Get("CF-POP")); pop != "" {
		return pop
	}
	ray := strings.TrimSpace(headers.Get("CF-RAY"))
	if idx := strings.LastIndex(ray, "-"); idx >= 0 && idx+1 < len(ray) {
		return strings.TrimSpace(ray[idx+1:])
	}
	return ""
}
