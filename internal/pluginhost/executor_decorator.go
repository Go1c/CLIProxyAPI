package pluginhost

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// ExecutorRequestDecoration describes a prepared plugin executor request just
// before it crosses the plugin boundary.
type ExecutorRequestDecoration struct {
	Model         string
	SourceFormat  sdktranslator.Format
	SourcePayload []byte
	Payload       []byte
	Headers       http.Header
}

// ExecutorRequestDecorator adjusts a prepared plugin executor request for one
// provider after translation and credential selection.
type ExecutorRequestDecorator func(context.Context, *coreauth.Auth, ExecutorRequestDecoration) (ExecutorRequestDecoration, error)

// RegisterExecutorRequestDecorator registers or removes a provider-specific
// request decorator. Passing nil removes the current decorator.
func (h *Host) RegisterExecutorRequestDecorator(provider string, decorator ExecutorRequestDecorator) {
	if h == nil {
		return
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.executorDecorators == nil {
		h.executorDecorators = make(map[string]ExecutorRequestDecorator)
	}
	if decorator == nil {
		delete(h.executorDecorators, provider)
		return
	}
	h.executorDecorators[provider] = decorator
}

func (h *Host) executorRequestDecorator(provider string) ExecutorRequestDecorator {
	if h == nil {
		return nil
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.executorDecorators[provider]
}

func (a *executorAdapter) decorateExecutorCall(ctx context.Context, auth *coreauth.Auth, sourceReq coreexecutor.Request, prepared preparedExecutorCall) (preparedExecutorCall, error) {
	decorator := a.host.executorRequestDecorator(a.provider)
	if decorator == nil {
		return prepared, nil
	}
	decoration, errDecorate := decorator(ctx, auth, ExecutorRequestDecoration{
		Model:         sourceReq.Model,
		SourceFormat:  prepared.inputRequested,
		SourcePayload: bytes.Clone(sourceReq.Payload),
		Payload:       bytes.Clone(prepared.req.Payload),
		Headers:       cloneHeader(prepared.opts.Headers),
	})
	if errDecorate != nil {
		return preparedExecutorCall{}, fmt.Errorf("decorate plugin executor %s request: %w", a.Identifier(), errDecorate)
	}
	prepared.req.Payload = bytes.Clone(decoration.Payload)
	prepared.opts.Headers = cloneHeader(decoration.Headers)
	return prepared, nil
}
