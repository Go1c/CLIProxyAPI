package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestManagementRegisterHandleStatusAndResource(t *testing.T) {
	withTestRuntime(t)
	cfg := defaultPluginConfig()
	cfg.ManagementEnabled = true
	cfg.ReadIdleTimeout = 15 * time.Second
	cfg.PingTimeout = 15 * time.Second
	runtime.updateConfig(cfg)
	if _, err := runtime.pools.acquire(cfg.BaseURL, "", cfg); err != nil {
		t.Fatalf("acquire pool: %v", err)
	}

	regRaw, err := json.Marshal(rpcManagementRegistrationRequest{
		ManagementRegistrationRequest: pluginapi.ManagementRegistrationRequest{
			BasePath:         "/v0/management",
			ResourceBasePath: "/v0/resource/plugins/" + pluginIdentifier,
		},
	})
	if err != nil {
		t.Fatalf("marshal register request: %v", err)
	}
	reg := decodeEnvelope[rpcManagementRegistrationResponse](t, mustCallManagementRegister(t, regRaw))
	if len(reg.Routes) != 2 || len(reg.Resources) != 0 {
		t.Fatalf("management registration = %#v, want 2 authenticated routes and no public resources", reg)
	}

	statusRaw, err := json.Marshal(rpcManagementRequest{
		ManagementRequest: pluginapi.ManagementRequest{
			Method: http.MethodGet,
			Path:   "/v0/management/plugins/" + pluginIdentifier + "/status",
		},
	})
	if err != nil {
		t.Fatalf("marshal status request: %v", err)
	}
	status := decodeEnvelope[poolStatusResponse](t, mustCallManagementHandle(t, statusRaw))
	if !status.ManagementEnabled || !status.Enabled {
		t.Fatalf("status = %#v, want enabled management", status)
	}
	if len(status.Pools) != 1 {
		t.Fatalf("status pools = %d, want 1", len(status.Pools))
	}
	if status.Pools[0].ReadIdleTimeout != cfg.ReadIdleTimeout.String() || status.Pools[0].PingTimeout != cfg.PingTimeout.String() {
		t.Fatalf("status pool = %#v, want configured timeouts", status.Pools[0])
	}

	closeRaw, err := json.Marshal(rpcManagementRequest{
		ManagementRequest: pluginapi.ManagementRequest{
			Method: http.MethodPost,
			Path:   "/v0/management/plugins/" + pluginIdentifier + "/close-idle",
		},
	})
	if err != nil {
		t.Fatalf("marshal close request: %v", err)
	}
	closeResp := decodeEnvelope[poolCloseResponse](t, mustCallManagementHandle(t, closeRaw))
	if closeResp.PoolsProcessed != 1 {
		t.Fatalf("PoolsProcessed = %d, want 1", closeResp.PoolsProcessed)
	}
}

func mustCallManagementRegister(t *testing.T, raw []byte) []byte {
	t.Helper()
	out, err := managementRegister(raw)
	if err != nil {
		t.Fatalf("managementRegister() error = %v", err)
	}
	return out
}

func mustCallManagementHandle(t *testing.T, raw []byte) []byte {
	t.Helper()
	out, err := managementHandle(raw)
	if err != nil {
		t.Fatalf("managementHandle() error = %v", err)
	}
	return out
}
