package main

import (
	"encoding/json"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestPluginRegistrationDeclaresCodexOAuthExecutor(t *testing.T) {
	reg := pluginRegistration()
	if reg.SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", reg.SchemaVersion)
	}
	if reg.Metadata.Name != pluginIdentifier {
		t.Fatalf("Metadata.Name = %q, want %q", reg.Metadata.Name, pluginIdentifier)
	}
	if reg.Capabilities.ExecutorModelScope != string(pluginapi.ExecutorModelScopeOAuth) {
		t.Fatalf("ExecutorModelScope = %q, want oauth", reg.Capabilities.ExecutorModelScope)
	}
	if !reg.Capabilities.ModelRouter || !reg.Capabilities.Executor || !reg.Capabilities.ManagementAPI {
		t.Fatalf("Capabilities = %#v, want router+executor+management", reg.Capabilities)
	}
	if got := reg.Capabilities.ExecutorInputFormats; len(got) != 1 || got[0] != "codex" {
		t.Fatalf("ExecutorInputFormats = %#v, want [codex]", got)
	}
	if got := reg.Capabilities.ExecutorOutputFormats; len(got) != 1 || got[0] != "codex" {
		t.Fatalf("ExecutorOutputFormats = %#v, want [codex]", got)
	}
	wantFields := map[string]struct{}{
		"enabled":              {},
		"model_prefixes":       {},
		"base_url":             {},
		"read_idle_timeout":    {},
		"ping_timeout":         {},
		"max_idle_connections": {},
		"retry_network_errors": {},
		"management_enabled":   {},
	}
	if len(reg.Metadata.ConfigFields) != len(wantFields) {
		t.Fatalf("ConfigFields = %d, want %d", len(reg.Metadata.ConfigFields), len(wantFields))
	}
	for _, field := range reg.Metadata.ConfigFields {
		if _, ok := wantFields[field.Name]; !ok {
			t.Fatalf("unexpected config field %q", field.Name)
		}
		delete(wantFields, field.Name)
	}
	if len(wantFields) != 0 {
		t.Fatalf("missing config fields: %#v", wantFields)
	}
}

func TestExecutorIdentifierUsesCodexOAuthProvider(t *testing.T) {
	raw, errHandle := handleMethod(pluginabi.MethodExecutorIdentifier, nil)
	if errHandle != nil {
		t.Fatalf("handleMethod() error = %v", errHandle)
	}
	identifier := decodeEnvelope[map[string]string](t, raw)["identifier"]
	if identifier != executorProvider {
		t.Fatalf("identifier = %q, want %q", identifier, executorProvider)
	}
	if identifier == pluginIdentifier {
		t.Fatalf("executor provider must differ from plugin id %q", pluginIdentifier)
	}
}

func TestRouteModelMatchesConfiguredCodexPrefixes(t *testing.T) {
	withTestRuntime(t)
	cfg := defaultPluginConfig()
	runtime.updateConfig(cfg)

	rawReq, errMarshal := json.Marshal(rpcModelRouteRequest{
		ModelRouteRequest: pluginapi.ModelRouteRequest{RequestedModel: "gpt-5.4", SourceFormat: "openai"},
	})
	if errMarshal != nil {
		t.Fatalf("marshal request: %v", errMarshal)
	}
	resp := decodeEnvelope[pluginapi.ModelRouteResponse](t, mustCallRouteModel(t, rawReq))
	if !resp.Handled || resp.TargetKind != pluginapi.ModelRouteTargetSelf {
		t.Fatalf("route response = %#v, want self handled", resp)
	}

	rawReq, errMarshal = json.Marshal(rpcModelRouteRequest{
		ModelRouteRequest: pluginapi.ModelRouteRequest{RequestedModel: "gemini-2.0", SourceFormat: "openai"},
	})
	if errMarshal != nil {
		t.Fatalf("marshal request: %v", errMarshal)
	}
	resp = decodeEnvelope[pluginapi.ModelRouteResponse](t, mustCallRouteModel(t, rawReq))
	if resp.Handled {
		t.Fatalf("route response = %#v, want unhandled", resp)
	}
}

func TestRouteModelDisabledPluginDoesNotHandle(t *testing.T) {
	withTestRuntime(t)
	cfg := defaultPluginConfig()
	cfg.Enabled = false
	runtime.updateConfig(cfg)

	rawReq, errMarshal := json.Marshal(rpcModelRouteRequest{
		ModelRouteRequest: pluginapi.ModelRouteRequest{RequestedModel: "gpt-5.4"},
	})
	if errMarshal != nil {
		t.Fatalf("marshal request: %v", errMarshal)
	}
	resp := decodeEnvelope[pluginapi.ModelRouteResponse](t, mustCallRouteModel(t, rawReq))
	if resp.Handled {
		t.Fatalf("route response = %#v, want unhandled when disabled", resp)
	}
}

func TestResolveCodexAuthSettingsUsesAuthValuesBeforeConfig(t *testing.T) {
	req := rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			AuthID: "auth-1",
			StorageJSON: []byte(`{
				"access_token":"token-storage",
				"account_id":"acct-storage",
				"proxy_url":"socks5://user:pass@proxy.example:1080",
				"base_url":"https://auth.example/backend-api/codex/responses"
			}`),
		},
	}
	cfg := defaultPluginConfig()
	cfg.BaseURL = "https://auth.example/backend-api/codex"

	settings, err := resolveCodexAuthSettings(req, cfg)
	if err != nil {
		t.Fatalf("resolveCodexAuthSettings() error = %v", err)
	}
	if settings.BaseURL != "https://auth.example/backend-api/codex/responses" {
		t.Fatalf("BaseURL = %q, want auth override", settings.BaseURL)
	}
	if settings.AccessToken != "token-storage" || settings.AccountID != "acct-storage" {
		t.Fatalf("auth values = %#v, want storage values", settings)
	}
	if settings.ProxyURL != "socks5://user:pass@proxy.example:1080" {
		t.Fatalf("ProxyURL = %q, want storage value", settings.ProxyURL)
	}
}

func TestResolveCodexAuthSettingsRejectsUnconfiguredOrigin(t *testing.T) {
	req := rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{
		StorageJSON: []byte(`{"access_token":"token","base_url":"https://attacker.example/responses"}`),
	}}
	_, err := resolveCodexAuthSettings(req, defaultPluginConfig())
	if err == nil || pluginErrorCode(err) != "invalid_base_url" {
		t.Fatalf("resolveCodexAuthSettings() error = %v, want invalid_base_url", err)
	}
}

func TestResolveCodexAuthSettingsReturnsAuthNotFoundWithoutToken(t *testing.T) {
	req := rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			StorageJSON: []byte(`{}`),
		},
	}

	_, err := resolveCodexAuthSettings(req, defaultPluginConfig())
	if err == nil {
		t.Fatal("resolveCodexAuthSettings() error = nil, want auth_not_found")
	}
	if pluginErrorCode(err) != "auth_not_found" {
		t.Fatalf("pluginErrorCode = %q, want auth_not_found", pluginErrorCode(err))
	}
}

func mustCallRouteModel(t *testing.T, raw []byte) []byte {
	t.Helper()
	out, err := routeModel(raw)
	if err != nil {
		t.Fatalf("routeModel() error = %v", err)
	}
	return out
}
