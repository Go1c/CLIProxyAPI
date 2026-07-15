package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static const cliproxy_host_api* stored_host;

static void store_host_api(const cliproxy_host_api* host) {
	stored_host = host;
}

static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) {
		return 1;
	}
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}

static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) {
		stored_host->free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

const (
	pluginIdentifier         = "codex-http2-keepalive"
	defaultPluginVersion     = "0.1.0"
	defaultCodexResponsesURL = "https://chatgpt.com/backend-api/codex/responses"
	defaultPluginPrompt      = "Help me use Codex HTTP/2 keepalive."
)

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	Retryable  bool   `json:"retryable,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

type rawPluginConfig struct {
	Enabled            *bool    `yaml:"enabled"`
	ModelPrefixes      []string `yaml:"model_prefixes"`
	BaseURL            string   `yaml:"base_url"`
	ReadIdleTimeout    string   `yaml:"read_idle_timeout"`
	PingTimeout        string   `yaml:"ping_timeout"`
	MaxIdleConnections *int     `yaml:"max_idle_connections"`
	RetryNetworkErrors *bool    `yaml:"retry_network_errors"`
	ManagementEnabled  *bool    `yaml:"management_enabled"`
}

type pluginConfig struct {
	Enabled            bool
	ModelPrefixes      []string
	BaseURL            string
	ReadIdleTimeout    time.Duration
	PingTimeout        time.Duration
	MaxIdleConnections int
	RetryNetworkErrors bool
	ManagementEnabled  bool
}

type pluginState struct {
	mu                 sync.RWMutex
	config             pluginConfig
	managementBasePath string
	resourceBasePath   string
	shutdown           bool
	pools              *codexPoolManager
}

type rpcLifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type registrationCapability struct {
	ModelRouter           bool     `json:"model_router"`
	Executor              bool     `json:"executor"`
	ExecutorModelScope    string   `json:"executor_model_scope"`
	ExecutorInputFormats  []string `json:"executor_input_formats"`
	ExecutorOutputFormats []string `json:"executor_output_formats"`
	ManagementAPI         bool     `json:"management_api"`
}

type rpcExecutorRequest struct {
	pluginapi.ExecutorRequest
	StreamID       string `json:"stream_id,omitempty"`
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcModelRouteRequest struct {
	pluginapi.ModelRouteRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcManagementRequest struct {
	pluginapi.ManagementRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcManagementRegistrationRequest struct {
	pluginapi.ManagementRegistrationRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcManagementRegistrationResponse struct {
	Routes    []pluginapi.ManagementRoute `json:"routes,omitempty"`
	Resources []pluginapi.ResourceRoute   `json:"resources,omitempty"`
}

type rpcEmptyResponse struct{}

type rpcExecutorStreamResponse struct {
	Headers http.Header                     `json:"headers,omitempty"`
	Chunks  []pluginapi.ExecutorStreamChunk `json:"chunks,omitempty"`
}

type rpcHostLogRequest struct {
	HostCallbackID string         `json:"host_callback_id,omitempty"`
	Level          string         `json:"level,omitempty"`
	Message        string         `json:"message,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}

type rpcStreamEmitRequest struct {
	StreamID string `json:"stream_id"`
	Payload  []byte `json:"payload,omitempty"`
	Error    string `json:"error,omitempty"`
}

type rpcStreamCloseRequest struct {
	StreamID string `json:"stream_id"`
	Error    string `json:"error,omitempty"`
}

type pluginRuntime struct {
	mu sync.RWMutex
	pluginConfig
	pools              *codexPoolManager
	managementBasePath string
	resourceBasePath   string
	shutdown           bool
}

var runtime = newPluginRuntime()

var hostCall = callHost

func newPluginRuntime() *pluginRuntime {
	return &pluginRuntime{
		pluginConfig: defaultPluginConfig(),
		pools:        newCodexPoolManager(),
	}
}

func defaultPluginConfig() pluginConfig {
	return pluginConfig{
		Enabled:            true,
		ModelPrefixes:      []string{"gpt-", "codex-"},
		BaseURL:            defaultCodexResponsesURL,
		ReadIdleTimeout:    15 * time.Second,
		PingTimeout:        15 * time.Second,
		MaxIdleConnections: 16,
		RetryNetworkErrors: true,
		ManagementEnabled:  true,
	}
}

func normalizePluginConfig(cfg pluginConfig) pluginConfig {
	base := defaultPluginConfig()
	if !cfg.Enabled {
		base.Enabled = false
	}
	if len(cfg.ModelPrefixes) > 0 {
		prefixes := make([]string, 0, len(cfg.ModelPrefixes))
		for _, prefix := range cfg.ModelPrefixes {
			prefix = strings.ToLower(strings.TrimSpace(prefix))
			if prefix != "" {
				prefixes = append(prefixes, prefix)
			}
		}
		if len(prefixes) > 0 {
			base.ModelPrefixes = prefixes
		}
	}
	if baseURL := strings.TrimSpace(cfg.BaseURL); baseURL != "" {
		base.BaseURL = baseURL
	}
	if cfg.ReadIdleTimeout > 0 {
		base.ReadIdleTimeout = cfg.ReadIdleTimeout
	}
	if cfg.PingTimeout > 0 {
		base.PingTimeout = cfg.PingTimeout
	}
	if cfg.MaxIdleConnections > 0 {
		base.MaxIdleConnections = cfg.MaxIdleConnections
	}
	if !cfg.RetryNetworkErrors {
		base.RetryNetworkErrors = false
	}
	if !cfg.ManagementEnabled {
		base.ManagementEnabled = false
	}
	return base
}

func decodePluginConfig(raw []byte) (pluginConfig, error) {
	cfg := defaultPluginConfig()
	if len(raw) == 0 {
		return cfg, nil
	}
	var decoded rawPluginConfig
	if err := yaml.Unmarshal(raw, &decoded); err != nil {
		return pluginConfig{}, err
	}
	if decoded.Enabled != nil {
		cfg.Enabled = *decoded.Enabled
	}
	cfg.ModelPrefixes = decoded.ModelPrefixes
	cfg.BaseURL = decoded.BaseURL
	if decoded.ReadIdleTimeout != "" {
		dur, err := time.ParseDuration(decoded.ReadIdleTimeout)
		if err != nil {
			return pluginConfig{}, fmt.Errorf("parse read_idle_timeout: %w", err)
		}
		cfg.ReadIdleTimeout = dur
	}
	if decoded.PingTimeout != "" {
		dur, err := time.ParseDuration(decoded.PingTimeout)
		if err != nil {
			return pluginConfig{}, fmt.Errorf("parse ping_timeout: %w", err)
		}
		cfg.PingTimeout = dur
	}
	if decoded.MaxIdleConnections != nil {
		cfg.MaxIdleConnections = *decoded.MaxIdleConnections
	}
	if decoded.RetryNetworkErrors != nil {
		cfg.RetryNetworkErrors = *decoded.RetryNetworkErrors
	}
	if decoded.ManagementEnabled != nil {
		cfg.ManagementEnabled = *decoded.ManagementEnabled
	}
	return normalizePluginConfig(cfg), nil
}

func (r *pluginRuntime) snapshot() pluginConfig {
	if r == nil {
		return defaultPluginConfig()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pluginConfig
}

func (r *pluginRuntime) updateConfig(cfg pluginConfig) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.pluginConfig = normalizePluginConfig(cfg)
	r.mu.Unlock()
	if r.pools != nil {
		r.pools.CloseIdleConnections()
	}
}

func (r *pluginRuntime) setManagementPaths(basePath, resourceBasePath string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.managementBasePath = strings.TrimSpace(basePath)
	r.resourceBasePath = strings.TrimSpace(resourceBasePath)
	r.mu.Unlock()
}

func (r *pluginRuntime) managementPaths() (string, string) {
	if r == nil {
		return "", ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.managementBasePath, r.resourceBasePath
}

func (r *pluginRuntime) setShutdown() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.shutdown = true
	r.mu.Unlock()
}

func (r *pluginRuntime) isShutdown() bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.shutdown
}

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	C.store_host_api(host)
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required", false, http.StatusBadRequest))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleMethod(C.GoString(method), requestBytes)
	if errHandle != nil {
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error(), false, http.StatusInternalServerError))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	runtime.setShutdown()
	if runtime.pools != nil {
		runtime.pools.Shutdown()
	}
}

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		if err := configure(request); err != nil {
			return nil, err
		}
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodPluginShutdown:
		cliproxyPluginShutdown()
		return okEnvelope(rpcEmptyResponse{})
	case pluginabi.MethodModelRoute:
		return routeModel(request)
	case pluginabi.MethodExecutorIdentifier:
		return okEnvelope(map[string]string{"identifier": pluginIdentifier})
	case pluginabi.MethodExecutorExecute:
		return execute(request)
	case pluginabi.MethodExecutorExecuteStream:
		return executeStream(request)
	case pluginabi.MethodExecutorCountTokens:
		return okEnvelope(pluginapi.ExecutorResponse{Payload: []byte(`{"input_tokens":0,"output_tokens":0,"total_tokens":0}`)})
	case pluginabi.MethodManagementRegister:
		return managementRegister(request)
	case pluginabi.MethodManagementHandle:
		return managementHandle(request)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method, false, http.StatusBadRequest), nil
	}
}

func configure(raw []byte) error {
	var req rpcLifecycleRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return err
		}
	}
	cfg, err := decodePluginConfig(req.ConfigYAML)
	if err != nil {
		return err
	}
	runtime.updateConfig(cfg)
	return nil
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             pluginIdentifier,
			Version:          defaultPluginVersion,
			Author:           "Local developer",
			GitHubRepository: "https://github.com/router-for-me/CLIProxyAPI",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Enable or disable the Codex HTTP/2 keepalive plugin."},
				{Name: "model_prefixes", Type: pluginapi.ConfigFieldTypeArray, Description: "Model prefixes routed to the plugin. Defaults to gpt- and codex-."},
				{Name: "base_url", Type: pluginapi.ConfigFieldTypeString, Description: "Upstream Codex Responses endpoint. Defaults to https://chatgpt.com/backend-api/codex/responses."},
				{Name: "read_idle_timeout", Type: pluginapi.ConfigFieldTypeString, Description: "HTTP/2 read idle timeout, for example 15s."},
				{Name: "ping_timeout", Type: pluginapi.ConfigFieldTypeString, Description: "HTTP/2 PING timeout, for example 15s."},
				{Name: "max_idle_connections", Type: pluginapi.ConfigFieldTypeInteger, Description: "Maximum number of idle pool entries retained by the plugin."},
				{Name: "retry_network_errors", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Retry once after a retryable network or dead-connection failure."},
				{Name: "management_enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Expose the management status and idle-close routes."},
			},
		},
		Capabilities: registrationCapability{
			ModelRouter:           true,
			Executor:              true,
			ExecutorModelScope:    string(pluginapi.ExecutorModelScopeOAuth),
			ExecutorInputFormats:  []string{"codex"},
			ExecutorOutputFormats: []string{"codex"},
			ManagementAPI:         true,
		},
	}
}

func routeModel(raw []byte) ([]byte, error) {
	var req rpcModelRouteRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	cfg := runtime.snapshot()
	if !cfg.Enabled {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	if !modelMatchesPrefix(req.RequestedModel, cfg.ModelPrefixes) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	if !sourceFormatLooksCodexOrOpenAI(req.SourceFormat) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	return okEnvelope(pluginapi.ModelRouteResponse{
		Handled:    true,
		TargetKind: pluginapi.ModelRouteTargetSelf,
		Reason:     "codex_http2_keepalive",
	})
}

func modelMatchesPrefix(model string, prefixes []string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		return false
	}
	if len(prefixes) == 0 {
		prefixes = []string{"gpt-", "codex-"}
	}
	for _, prefix := range prefixes {
		prefix = strings.ToLower(strings.TrimSpace(prefix))
		if prefix != "" && strings.HasPrefix(model, prefix) {
			return true
		}
	}
	return false
}

func sourceFormatLooksCodexOrOpenAI(format string) bool {
	format = strings.ToLower(strings.TrimSpace(format))
	return format == "" || strings.HasPrefix(format, "openai") || strings.HasPrefix(format, "codex")
}

func okEnvelope(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return marshalEnvelope(json.RawMessage(raw))
}

func errorEnvelope(code, message string, retryable bool, status int) []byte {
	raw, _ := json.Marshal(envelope{
		OK: false,
		Error: &envelopeError{
			Code:       code,
			Message:    message,
			Retryable:  retryable,
			HTTPStatus: status,
		},
	})
	return raw
}

func marshalEnvelope(result json.RawMessage) ([]byte, error) {
	if result == nil {
		result = json.RawMessage(`{}`)
	}
	return json.Marshal(envelope{OK: true, Result: result})
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

func callHost(method string, payload any) (json.RawMessage, error) {
	rawPayload, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return nil, fmt.Errorf("marshal host callback %s: %w", method, errMarshal)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	var response C.cliproxy_buffer
	var requestPtr *C.uint8_t
	if len(rawPayload) > 0 {
		cPayload := C.CBytes(rawPayload)
		if cPayload == nil {
			return nil, fmt.Errorf("allocate host callback %s", method)
		}
		defer C.free(cPayload)
		requestPtr = (*C.uint8_t)(cPayload)
	}
	callCode := C.call_host_api(cMethod, requestPtr, C.size_t(len(rawPayload)), &response)
	var rawResponse []byte
	if response.ptr != nil && response.len > 0 {
		rawResponse = C.GoBytes(response.ptr, C.int(response.len))
	}
	if response.ptr != nil {
		C.free_host_buffer(response.ptr, response.len)
	}
	if len(rawResponse) == 0 {
		return nil, fmt.Errorf("host callback %s returned no response, code=%d", method, int(callCode))
	}

	var env envelope
	if errUnmarshal := json.Unmarshal(rawResponse, &env); errUnmarshal != nil {
		return nil, fmt.Errorf("decode host envelope %s: %w", method, errUnmarshal)
	}
	if !env.OK {
		if env.Error != nil {
			return nil, fmt.Errorf("%s: %s", env.Error.Code, env.Error.Message)
		}
		return nil, fmt.Errorf("host callback %s failed", method)
	}
	if callCode != 0 {
		return nil, fmt.Errorf("host callback %s returned code=%d", method, int(callCode))
	}
	return append(json.RawMessage(nil), env.Result...), nil
}

func cloneHeaders(src http.Header) http.Header {
	if len(src) == 0 {
		return make(http.Header)
	}
	return src.Clone()
}

func currentConfig() pluginConfig {
	return runtime.snapshot()
}
