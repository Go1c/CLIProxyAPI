package pluginhost

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// executorPluginReady reports whether the named plugin can actually execute a
// request right now: it must declare an executor capability, resolve a
// non-empty provider identifier, and declare formats compatible with the
// current request.
// Routing pre-checks use this so that targets which would fail at execution are
// treated as unhandled and fall through to lower-priority routers instead of
// returning handled then 500ing.
func (h *Host) executorPluginReady(pluginID string, routeReq pluginapi.ModelRouteRequest) bool {
	if h == nil {
		return false
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return false
	}
	for _, record := range h.activeRecords() {
		if record.id != pluginID || h.isPluginFused(record.id) {
			continue
		}
		executor := record.plugin.Capabilities.Executor
		if executor == nil {
			return false
		}
		provider, okProvider := h.executorProvider(record, executor)
		if !okProvider {
			return false
		}
		adapter := newExecutorAdapterRegistration(h, record, provider, executor).adapter
		return adapter.supportsExecutorFormats(
			coreexecutor.Request{Model: routeReq.RequestedModel, Payload: routeReq.Body},
			coreexecutor.Options{
				Stream:          routeReq.Stream,
				OriginalRequest: routeReq.Body,
				SourceFormat:    sdktranslator.FromString(routeReq.SourceFormat),
				ResponseFormat:  sdktranslator.FromString(routeReq.SourceFormat),
				Headers:         cloneHeader(routeReq.Headers),
				Query:           cloneValues(routeReq.Query),
				Metadata:        cloneInterceptorMetadata(routeReq.Metadata),
			},
		)
	}
	return false
}

func (a *executorAdapter) supportsExecutorFormats(req coreexecutor.Request, opts coreexecutor.Options) bool {
	if a == nil {
		return false
	}
	inputRequested := executorInputFormat(req, opts)
	requestedFormat := executorRequestedFormat(req, opts)
	inputFormat, errInput := a.selectExecutorInputFormat(inputRequested)
	if errInput != nil {
		return false
	}
	_, errOutput := a.selectExecutorOutputFormat(requestedFormat, inputFormat)
	return errOutput == nil
}

// PluginExecutorRequestToFormat reports the executor input format selected for a direct plugin executor route.
func (h *Host) PluginExecutorRequestToFormat(pluginID string, req coreexecutor.Request, opts coreexecutor.Options) sdktranslator.Format {
	_, _, adapter, errAdapter := h.executorAdapterRecordForPlugin(pluginID)
	if errAdapter != nil {
		return ""
	}
	return adapter.RequestToFormat(req, opts)
}

// ExecutePluginExecutor executes a request with the named plugin executor without changing the requested model.
func (h *Host) ExecutePluginExecutor(ctx context.Context, pluginID string, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	record, provider, adapter, errAdapter := h.executorAdapterRecordForPlugin(pluginID)
	if errAdapter != nil {
		return coreexecutor.Response{}, errAdapter
	}
	auth, errAuth := h.executorAuthForRecord(ctx, record, provider, req, opts)
	if errAuth != nil {
		return coreexecutor.Response{}, errAuth
	}
	return adapter.Execute(ctx, auth, req, opts)
}

// ExecutePluginExecutorStream executes a streaming request with the named plugin executor without changing the requested model.
func (h *Host) ExecutePluginExecutorStream(ctx context.Context, pluginID string, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	record, provider, adapter, errAdapter := h.executorAdapterRecordForPlugin(pluginID)
	if errAdapter != nil {
		return nil, errAdapter
	}
	auth, errAuth := h.executorAuthForRecord(ctx, record, provider, req, opts)
	if errAuth != nil {
		return nil, errAuth
	}
	return adapter.ExecuteStream(ctx, auth, req, opts)
}

// CountPluginExecutor executes a count-tokens request with the named plugin executor without changing the requested model.
func (h *Host) CountPluginExecutor(ctx context.Context, pluginID string, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	record, provider, adapter, errAdapter := h.executorAdapterRecordForPlugin(pluginID)
	if errAdapter != nil {
		return coreexecutor.Response{}, errAdapter
	}
	auth, errAuth := h.executorAuthForRecord(ctx, record, provider, req, opts)
	if errAuth != nil {
		return coreexecutor.Response{}, errAuth
	}
	return adapter.CountTokens(ctx, auth, req, opts)
}

func (h *Host) executorAdapterRecordForPlugin(pluginID string) (capabilityRecord, string, *executorAdapter, error) {
	if h == nil {
		return capabilityRecord{}, "", nil, fmt.Errorf("plugin host is unavailable")
	}
	pluginID = strings.TrimSpace(pluginID)
	if pluginID == "" {
		return capabilityRecord{}, "", nil, fmt.Errorf("target executor plugin id is required")
	}
	for _, record := range h.activeRecords() {
		if record.id != pluginID {
			continue
		}
		if h.isPluginFused(record.id) {
			return capabilityRecord{}, "", nil, fmt.Errorf("plugin executor %s is unavailable", pluginID)
		}
		executor := record.plugin.Capabilities.Executor
		if executor == nil {
			return capabilityRecord{}, "", nil, fmt.Errorf("plugin %s does not declare an executor", pluginID)
		}
		provider, okProvider := h.callExecutorIdentifier(record.id, executor)
		if !okProvider {
			return capabilityRecord{}, "", nil, fmt.Errorf("plugin executor %s has no provider identifier", pluginID)
		}
		provider = strings.ToLower(strings.TrimSpace(provider))
		registration := newExecutorAdapterRegistration(h, record, provider, executor)
		return record, provider, registration.adapter, nil
	}
	return capabilityRecord{}, "", nil, fmt.Errorf("plugin executor %s not found", pluginID)
}

func (h *Host) executorAuthForRecord(ctx context.Context, record capabilityRecord, provider string, req coreexecutor.Request, opts coreexecutor.Options) (*coreauth.Auth, error) {
	if h == nil {
		return nil, fmt.Errorf("plugin host is unavailable")
	}
	executor := record.plugin.Capabilities.Executor
	if executor == nil {
		return nil, fmt.Errorf("plugin %s does not declare an executor", record.id)
	}
	if normalizedExecutorModelScope(record.plugin.Capabilities) != pluginapi.ExecutorModelScopeOAuth {
		return nil, nil
	}
	if h.authManager == nil {
		return nil, &coreauth.Error{Code: "auth_not_found", Message: "no oauth auth available", HTTPStatus: http.StatusServiceUnavailable}
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	selected, errSelect := h.authManager.SelectAuthByKind(ctx, provider, strings.TrimSpace(req.Model), coreauth.AuthKindOAuth, opts)
	if errSelect != nil {
		return nil, errSelect
	}
	if selected == nil {
		return nil, &coreauth.Error{Code: "auth_not_found", Message: "no oauth auth available", HTTPStatus: http.StatusServiceUnavailable}
	}
	return selected, nil
}
