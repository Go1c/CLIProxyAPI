package main

import (
	"encoding/json"
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
	runtime.setManagementBasePath(req.BasePath)
	cfg := currentConfig()
	if !cfg.ManagementEnabled {
		return okEnvelope(rpcManagementRegistrationResponse{})
	}
	return okEnvelope(rpcManagementRegistrationResponse{
		Routes: []pluginapi.ManagementRoute{
			{Method: http.MethodGet, Path: "/plugins/" + pluginIdentifier + "/status", Description: "Codex HTTP/2 keepalive JSON status."},
			{Method: http.MethodPost, Path: "/plugins/" + pluginIdentifier + "/close-idle", Description: "Close idle HTTP/2 connections for every pool."},
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
	basePath := runtime.managementPath()
	if basePath == "" {
		return errorEnvelope("not_found", "management route not found", false, http.StatusNotFound), nil
	}
	statusPath := strings.TrimRight(basePath, "/") + "/plugins/" + pluginIdentifier + "/status"
	closeIdlePath := strings.TrimRight(basePath, "/") + "/plugins/" + pluginIdentifier + "/close-idle"
	switch {
	case strings.EqualFold(req.Method, http.MethodGet) && req.Path == statusPath:
		return okEnvelope(managementStatusJSON(cfg))
	case strings.EqualFold(req.Method, http.MethodPost) && req.Path == closeIdlePath:
		processed := runtime.pools.CloseIdleConnections()
		pools := runtime.pools.snapshot()
		return okEnvelope(poolCloseResponse{
			PoolsProcessed: processed,
			GeneratedAt:    timeNowRFC3339(),
			Pools:          pools,
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

func timeNowRFC3339() string {
	return strings.TrimSpace(time.Now().UTC().Format(time.RFC3339Nano))
}
