package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func managementRegister(raw []byte) ([]byte, error) {
	var req rpcManagementRegistrationRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	runtime.setManagementPaths(req.BasePath, req.ResourceBasePath)
	cfg := currentConfig()
	if !cfg.ManagementEnabled {
		return okEnvelope(rpcManagementRegistrationResponse{})
	}
	return okEnvelope(rpcManagementRegistrationResponse{
		Routes: []pluginapi.ManagementRoute{
			{Method: http.MethodGet, Path: "/api/status", Description: "Codex HTTP/2 keepalive JSON status."},
			{Method: http.MethodPost, Path: "/api/close-idle", Description: "Close idle HTTP/2 connections for every pool."},
		},
		Resources: []pluginapi.ResourceRoute{
			{Path: "/status", Menu: "Codex HTTP/2 Keepalive", Description: "View pool status and close idle connections."},
		},
	})
}

func managementHandle(raw []byte) ([]byte, error) {
	var req rpcManagementRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	cfg := currentConfig()
	if !cfg.ManagementEnabled {
		return errorEnvelope("management_disabled", "management routes are disabled", false, http.StatusNotFound), nil
	}
	basePath, resourceBasePath := runtime.managementPaths()
	if basePath == "" || resourceBasePath == "" {
		return errorEnvelope("not_found", "management route not found", false, http.StatusNotFound), nil
	}
	switch {
	case strings.EqualFold(req.Method, http.MethodGet) && strings.HasPrefix(req.Path, resourceBasePath) && strings.HasSuffix(req.Path, "/status"):
		return okEnvelope(managementStatusHTML(cfg, resourceBasePath))
	case strings.EqualFold(req.Method, http.MethodGet) && strings.HasPrefix(req.Path, basePath) && strings.HasSuffix(req.Path, "/api/status"):
		return okEnvelope(managementStatusJSON(cfg))
	case strings.EqualFold(req.Method, http.MethodPost) && strings.HasPrefix(req.Path, basePath) && strings.HasSuffix(req.Path, "/api/close-idle"):
		runtime.pools.CloseIdleConnections()
		return okEnvelope(poolCloseResponse{
			ClosedIdlePools: len(runtime.pools.snapshot()),
			GeneratedAt:     timeNowRFC3339(),
			Pools:           runtime.pools.snapshot(),
		})
	default:
		return errorEnvelope("not_found", "management route not found", false, http.StatusNotFound), nil
	}
}

func managementStatusJSON(cfg pluginConfig) poolStatusResponse {
	return poolStatusResponse{
		GeneratedAt:        timeNowRFC3339(),
		Enabled:            cfg.Enabled,
		ManagementEnabled:  cfg.ManagementEnabled,
		BaseURL:            cfg.BaseURL,
		ModelPrefixes:      append([]string(nil), cfg.ModelPrefixes...),
		ReadIdleTimeout:    cfg.ReadIdleTimeout.String(),
		PingTimeout:        cfg.PingTimeout.String(),
		MaxIdleConnections: cfg.MaxIdleConnections,
		RetryNetworkErrors: cfg.RetryNetworkErrors,
		Pools:              runtime.pools.snapshot(),
	}
}

func managementStatusHTML(cfg pluginConfig, resourceBasePath string) pluginapi.ManagementResponse {
	status := managementStatusJSON(cfg)
	var buf bytes.Buffer
	buf.WriteString("<!doctype html><html><head><meta charset=\"utf-8\">")
	buf.WriteString("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">")
	buf.WriteString("<title>Codex HTTP/2 Keepalive</title>")
	buf.WriteString("<style>")
	buf.WriteString("body{font-family:system-ui,sans-serif;margin:24px;color:#111}table{border-collapse:collapse;width:100%;margin-top:16px}th,td{border-bottom:1px solid #ddd;padding:8px 10px;text-align:left;vertical-align:top}th{background:#f7f7f7}.muted{color:#666}.toolbar{display:flex;gap:12px;align-items:center;flex-wrap:wrap}button{padding:8px 12px;border:1px solid #ccc;background:#fff;cursor:pointer}")
	buf.WriteString("</style></head><body>")
	buf.WriteString("<div class=\"toolbar\"><strong>Codex HTTP/2 Keepalive</strong>")
	buf.WriteString("<span class=\"muted\">")
	buf.WriteString(html.EscapeString(status.GeneratedAt))
	buf.WriteString("</span>")
	buf.WriteString("<form method=\"post\" action=\"/v0/management/api/close-idle\"><button type=\"submit\">Close idle connections</button></form>")
	buf.WriteString("</div>")
	buf.WriteString("<p class=\"muted\">Resource path: ")
	buf.WriteString(html.EscapeString(resourceBasePath))
	buf.WriteString("</p>")
	buf.WriteString("<table><thead><tr><th>Upstream</th><th>Proxy</th><th>HTTP/2</th><th>Requests</th><th>Success</th><th>Failure</th><th>Active</th><th>Created</th><th>Removed</th><th>Last success</th><th>Last error</th><th>Last error at</th></tr></thead><tbody>")
	for _, pool := range status.Pools {
		buf.WriteString("<tr>")
		writeHTMLCell(&buf, pool.UpstreamHost)
		writeHTMLCell(&buf, pool.Proxy)
		writeHTMLCell(&buf, pool.HTTP2Status+" "+pool.ReadIdleTimeout+"/"+pool.PingTimeout)
		writeHTMLCell(&buf, fmt.Sprint(pool.Requests))
		writeHTMLCell(&buf, fmt.Sprint(pool.Successes))
		writeHTMLCell(&buf, fmt.Sprint(pool.Failures))
		writeHTMLCell(&buf, fmt.Sprint(pool.ActiveStreams))
		writeHTMLCell(&buf, fmt.Sprint(pool.ConnectionsCreated))
		writeHTMLCell(&buf, fmt.Sprint(pool.ConnectionsRemoved))
		writeHTMLCell(&buf, pool.LastSuccess)
		writeHTMLCell(&buf, pool.LastError)
		writeHTMLCell(&buf, pool.LastErrorAt)
		buf.WriteString("</tr>")
	}
	buf.WriteString("</tbody></table>")
	buf.WriteString("</body></html>")
	return pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
		Body:       buf.Bytes(),
	}
}

func writeHTMLCell(buf *bytes.Buffer, value string) {
	buf.WriteString("<td>")
	buf.WriteString(html.EscapeString(value))
	buf.WriteString("</td>")
}

func timeNowRFC3339() string {
	return strings.TrimSpace(time.Now().UTC().Format(time.RFC3339Nano))
}
