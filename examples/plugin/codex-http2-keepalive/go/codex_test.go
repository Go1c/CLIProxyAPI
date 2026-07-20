package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

func TestPrepareCodexRequestBodyStripsUnsupportedResponsesFields(t *testing.T) {
	// Client-origin fields that ChatGPT Codex internal upstream rejects (production 400:
	// Unsupported parameter: safety_identifier) plus the rest of the core strip set.
	payload := []byte(`{
		"model": "gpt-5-codex-client",
		"input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],
		"prompt_cache_key": "cache-keep",
		"safety_identifier": "sid-from-cursor",
		"prompt_cache_retention": "24h",
		"stream_options": {"include_usage": true},
		"previous_response_id": "resp_should_drop",
		"user": "user-1",
		"metadata": {"source": "test"},
		"context_management": {"compaction": {"type":"auto"}},
		"temperature": 0.2,
		"top_p": 0.9,
		"max_output_tokens": 128,
		"max_completion_tokens": 256,
		"truncation": "auto",
		"stream": false
	}`)
	out, err := prepareCodexRequestBody(rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{Payload: payload},
	}, "gpt-5.3-codex")
	if err != nil {
		t.Fatalf("prepareCodexRequestBody() error = %v", err)
	}

	if got := gjson.GetBytes(out, "model").String(); got != "gpt-5.3-codex" {
		t.Fatalf("model = %q, want gpt-5.3-codex", got)
	}
	if !gjson.GetBytes(out, "stream").Bool() {
		t.Fatalf("stream = false, want true; body=%s", out)
	}
	if got := gjson.GetBytes(out, "prompt_cache_key").String(); got != "cache-keep" {
		t.Fatalf("prompt_cache_key = %q, want preserved cache-keep", got)
	}
	if !gjson.GetBytes(out, "input").Exists() {
		t.Fatalf("input must be preserved; body=%s", out)
	}

	for _, field := range []string{
		"safety_identifier",
		"prompt_cache_retention",
		"stream_options",
		"previous_response_id",
		"user",
		"context_management",
		"temperature",
		"top_p",
		"max_output_tokens",
		"max_completion_tokens",
		"truncation",
	} {
		if gjson.GetBytes(out, field).Exists() {
			t.Fatalf("unsupported field %q leaked to upstream body: %s", field, out)
		}
	}
	// metadata is not stripped by codex_executor HTTP path; leave it unless translator
	// path already removed it. Core HTTP strip set does not delete metadata.
	if !gjson.GetBytes(out, "metadata").Exists() {
		t.Fatalf("metadata should remain (not in core executor strip set); body=%s", out)
	}
}

func TestPrepareCodexRequestBodyServiceTier(t *testing.T) {
	dropped, err := prepareCodexRequestBody(rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{Payload: []byte(`{"model":"gpt-5.3-codex","service_tier":"default","input":[]}`)},
	}, "")
	if err != nil {
		t.Fatalf("prepareCodexRequestBody() error = %v", err)
	}
	if gjson.GetBytes(dropped, "service_tier").Exists() {
		t.Fatalf("non-priority service_tier leaked to upstream body: %s", dropped)
	}

	kept, err := prepareCodexRequestBody(rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{Payload: []byte(`{"model":"gpt-5.3-codex","service_tier":"priority","input":[]}`)},
	}, "")
	if err != nil {
		t.Fatalf("prepareCodexRequestBody() error = %v", err)
	}
	if got := gjson.GetBytes(kept, "service_tier").String(); got != "priority" {
		t.Fatalf("service_tier = %q, want preserved priority; body=%s", got, kept)
	}
}

func TestPrepareCodexRequestBodyParallelToolCalls(t *testing.T) {
	withoutTools, err := prepareCodexRequestBody(rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{Payload: []byte(`{"model":"gpt-5.3-codex","parallel_tool_calls":true,"input":[]}`)},
	}, "")
	if err != nil {
		t.Fatalf("prepareCodexRequestBody() error = %v", err)
	}
	if gjson.GetBytes(withoutTools, "parallel_tool_calls").Exists() {
		t.Fatalf("parallel_tool_calls without tools leaked to upstream body: %s", withoutTools)
	}

	emptyTools, err := prepareCodexRequestBody(rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{Payload: []byte(`{"model":"gpt-5.3-codex","parallel_tool_calls":true,"tools":[],"input":[]}`)},
	}, "")
	if err != nil {
		t.Fatalf("prepareCodexRequestBody() error = %v", err)
	}
	if gjson.GetBytes(emptyTools, "parallel_tool_calls").Exists() {
		t.Fatalf("parallel_tool_calls with empty tools leaked to upstream body: %s", emptyTools)
	}

	withTools, err := prepareCodexRequestBody(rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{Payload: []byte(`{"model":"gpt-5.3-codex","parallel_tool_calls":true,"tools":[{"type":"function","name":"shell"}],"input":[]}`)},
	}, "")
	if err != nil {
		t.Fatalf("prepareCodexRequestBody() error = %v", err)
	}
	if !gjson.GetBytes(withTools, "parallel_tool_calls").Bool() {
		t.Fatalf("parallel_tool_calls with tools must be preserved; body=%s", withTools)
	}
}

func TestPrepareCodexRequestBodyEmptyBody(t *testing.T) {
	_, err := prepareCodexRequestBody(rpcExecutorRequest{}, "")
	if err == nil {
		t.Fatal("prepareCodexRequestBody() error = nil, want empty body error")
	}
}

func TestProcessCodexResponseAggregatesCompletedBody(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"message","content":[{"type":"output_text","text":"hello"}]}}`,
		``,
		`data: {"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":3,"output_tokens":4,"reasoning_tokens":5,"total_tokens":12}}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	result, err := processCodexResponse(context.Background(), resp, time.Now().Add(-2*time.Second), rpcExecutorRequest{}, codexAuthSettings{}, false, nil)
	if err != nil {
		t.Fatalf("processCodexResponse() error = %v", err)
	}
	if result.usage.TotalTokens != 12 || result.usage.InputTokens != 3 || result.usage.OutputTokens != 4 {
		t.Fatalf("usage = %#v, want completed usage", result.usage)
	}
	if !strings.Contains(string(result.finalBody), "hello") {
		t.Fatalf("final body = %s, want patched output item", result.finalBody)
	}
	if !strings.Contains(string(result.finalBody), `"total_tokens":12`) {
		t.Fatalf("final body = %s, want usage", result.finalBody)
	}
}

func TestProcessCodexResponseReturnsNonSSEBody(t *testing.T) {
	body := `{"id":"resp-json","object":"response","output":[{"type":"message","content":[{"type":"output_text","text":"plain"}]}]}`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	result, err := processCodexResponse(context.Background(), resp, time.Now(), rpcExecutorRequest{}, codexAuthSettings{}, false, nil)
	if err != nil {
		t.Fatalf("processCodexResponse() error = %v", err)
	}
	if string(result.finalBody) != body {
		t.Fatalf("finalBody = %s, want full non-SSE body", result.finalBody)
	}
}

func TestRequestContextFollowsHostCancellation(t *testing.T) {
	release := make(chan struct{})
	withHostCallStub(t, func(method string, payload any) (json.RawMessage, error) {
		if method != pluginabi.MethodHostContextWait {
			t.Fatalf("host method = %q, want context wait", method)
		}
		req := payload.(rpcHostContextWaitRequest)
		if req.HostCallbackID != "callback-1" {
			t.Fatalf("HostCallbackID = %q, want callback-1", req.HostCallbackID)
		}
		<-release
		return json.RawMessage(`{"canceled":true}`), nil
	})
	ctx, cancel := requestContext("callback-1")
	defer cancel()
	close(release)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("request context was not canceled by host callback")
	}
}

func TestCountTokensReturnsNonZeroInputUsage(t *testing.T) {
	rawReq, errMarshal := json.Marshal(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{
		Model:   "gpt-5.4",
		Payload: []byte(`{"instructions":"Be concise","input":[{"type":"message","content":[{"type":"input_text","text":"hello world"}]}]}`),
	}})
	if errMarshal != nil {
		t.Fatalf("marshal request: %v", errMarshal)
	}
	rawResp, errCount := countTokens(rawReq)
	if errCount != nil {
		t.Fatalf("countTokens() error = %v", errCount)
	}
	resp := decodeEnvelope[pluginapi.ExecutorResponse](t, rawResp)
	if count := gjson.GetBytes(resp.Payload, "response.usage.input_tokens").Int(); count <= 0 {
		t.Fatalf("input_tokens = %d, want > 0; payload=%s", count, resp.Payload)
	}
}

func TestHeaderSanitizersDoNotForwardCredentialsOrCookies(t *testing.T) {
	forwarded := sanitizeForwardHeaders(http.Header{
		"User-Agent":    []string{"client"},
		"Authorization": []string{"Bearer client-token"},
		"Cookie":        []string{"session=secret"},
		"X-Smuggled":    []string{"value"},
	})
	if forwarded.Get("User-Agent") != "client" || forwarded.Get("Authorization") != "" || forwarded.Get("Cookie") != "" || forwarded.Get("X-Smuggled") != "" {
		t.Fatalf("forwarded headers = %#v", forwarded)
	}
	response := sanitizeResponseHeaders(http.Header{
		"Content-Type":   []string{"text/event-stream"},
		"Set-Cookie":     []string{"backend=secret"},
		"Server":         []string{"internal"},
		"X-Request-Id":   []string{"request-1"},
		"X-Ratelimit-By": []string{"1"},
	}, false)
	if response.Get("Set-Cookie") != "" || response.Get("Server") != "" || response.Get("X-Request-Id") != "request-1" {
		t.Fatalf("response headers = %#v", response)
	}
}

func TestSanitizeForwardHeadersPreservesCodexSessionHeaderVariants(t *testing.T) {
	for _, key := range []string{"session_id", "Session_id", "Session-Id", "conversation_id", "Conversation-Id"} {
		t.Run(key, func(t *testing.T) {
			forwarded := sanitizeForwardHeaders(http.Header{key: []string{"affinity-1"}})
			if got := forwarded[key]; len(got) != 1 || got[0] != "affinity-1" {
				t.Fatalf("forwarded[%q] = %#v, want [affinity-1]", key, got)
			}
		})
	}
}

func TestCodexExecutorRequestHeaderKeySurvivesJSONRoundTrip(t *testing.T) {
	original := rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{
		Headers: http.Header{"session_id": []string{"cache-1"}},
	}}
	raw, errMarshal := json.Marshal(original)
	if errMarshal != nil {
		t.Fatalf("json.Marshal() error = %v", errMarshal)
	}
	var decoded rpcExecutorRequest
	if errUnmarshal := json.Unmarshal(raw, &decoded); errUnmarshal != nil {
		t.Fatalf("json.Unmarshal() error = %v", errUnmarshal)
	}
	forwarded := sanitizeForwardHeaders(decoded.Headers)
	if got := forwarded["session_id"]; len(got) != 1 || got[0] != "cache-1" {
		t.Fatalf("session_id after round trip = %#v, want [cache-1]", got)
	}
}

func TestProcessCodexResponseStreamsPatchedEvents(t *testing.T) {
	var emitted [][]byte
	withHostCallStub(t, func(method string, payload any) (json.RawMessage, error) {
		if method == pluginabi.MethodHostStreamEmit {
			req := payload.(rpcStreamEmitRequest)
			emitted = append(emitted, append([]byte(nil), req.Payload...))
		}
		return nil, nil
	})

	body := strings.Join([]string{
		`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"message","content":[{"type":"output_text","text":"hello"}]}}`,
		``,
		`data: {"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":3,"output_tokens":4,"reasoning_tokens":5,"total_tokens":12}}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	result, err := processCodexResponse(context.Background(), resp, time.Now().Add(-2*time.Second), rpcExecutorRequest{StreamID: "stream-1"}, codexAuthSettings{}, true, nil)
	if err != nil {
		t.Fatalf("processCodexResponse() error = %v", err)
	}
	if !result.emitted {
		t.Fatal("result.emitted = false, want true")
	}
	if len(emitted) < 2 {
		t.Fatalf("emitted chunks = %d, want at least 2", len(emitted))
	}
	if !strings.Contains(string(emitted[len(emitted)-1]), "response.completed") || !strings.Contains(string(emitted[len(emitted)-1]), "hello") {
		t.Fatalf("final emitted chunk = %s, want patched completed event", emitted[len(emitted)-1])
	}
}

func TestProcessCodexResponseReturnsRetryableErrorOnFailedEvent(t *testing.T) {
	body := `data: {"type":"response.failed","response":{"error":{"message":"boom"}}}
`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	_, err := processCodexResponse(context.Background(), resp, time.Now(), rpcExecutorRequest{}, codexAuthSettings{}, false, nil)
	if err == nil {
		t.Fatal("processCodexResponse() error = nil, want codex_response_error")
	}
	if pluginErrorCode(err) != "codex_response_error" {
		t.Fatalf("pluginErrorCode = %q, want codex_response_error", pluginErrorCode(err))
	}
	if pluginErrorStatus(err) != http.StatusBadGateway {
		t.Fatalf("pluginErrorStatus = %d, want %d", pluginErrorStatus(err), http.StatusBadGateway)
	}
}

func TestProcessCodexResponsePreservesUsageLimitStatusAndRetryAfter(t *testing.T) {
	resetAt := time.Now().Add(5 * time.Minute).Unix()
	body := fmt.Sprintf(`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":%d}}`, resetAt)
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	_, err := processCodexResponse(context.Background(), resp, time.Now(), rpcExecutorRequest{}, codexAuthSettings{}, false, nil)
	if err == nil {
		t.Fatal("processCodexResponse() error = nil, want upstream_status")
	}
	if pluginErrorCode(err) != "upstream_status" {
		t.Fatalf("pluginErrorCode = %q, want upstream_status", pluginErrorCode(err))
	}
	if pluginErrorStatus(err) != http.StatusTooManyRequests {
		t.Fatalf("pluginErrorStatus = %d, want %d", pluginErrorStatus(err), http.StatusTooManyRequests)
	}
	if err.Error() != body {
		t.Fatalf("error message = %q, want pass-through body %q", err.Error(), body)
	}
	if !isRetryableError(err) {
		t.Fatal("isRetryableError() = false, want true for usage limit")
	}
	retryAfter := pluginErrorRetryAfterSeconds(err)
	if retryAfter == nil {
		t.Fatal("pluginErrorRetryAfterSeconds() = nil, want resets_at based retry")
	}
	if *retryAfter < 4*60 || *retryAfter > 6*60 {
		t.Fatalf("pluginErrorRetryAfterSeconds() = %v, want ~5 minutes", *retryAfter)
	}
	envelopeRaw := errorEnvelopeWithRetryAfter(pluginErrorCode(err), err.Error(), isRetryableError(err), pluginErrorStatus(err), retryAfter)
	var env envelope
	if errDecode := json.Unmarshal(envelopeRaw, &env); errDecode != nil {
		t.Fatalf("unmarshal error envelope: %v", errDecode)
	}
	if env.Error == nil || env.Error.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("envelope error = %#v, want HTTPStatus 429", env.Error)
	}
	if env.Error.Message != body {
		t.Fatalf("envelope message = %q, want pass-through body", env.Error.Message)
	}
	if env.Error.RetryAfterSeconds == nil || *env.Error.RetryAfterSeconds < 4*60 {
		t.Fatalf("envelope retry_after_seconds = %#v, want ~5 minutes", env.Error.RetryAfterSeconds)
	}
}

func TestProcessCodexResponseMapsSSEUsageLimitTo429(t *testing.T) {
	body := `data: {"type":"response.failed","response":{"error":{"type":"usage_limit_reached","message":"usage limit reached","resets_in_seconds":120}}}
`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	_, err := processCodexResponse(context.Background(), resp, time.Now(), rpcExecutorRequest{}, codexAuthSettings{}, false, nil)
	if err == nil {
		t.Fatal("processCodexResponse() error = nil, want codex_response_error")
	}
	if pluginErrorStatus(err) != http.StatusTooManyRequests {
		t.Fatalf("pluginErrorStatus = %d, want %d", pluginErrorStatus(err), http.StatusTooManyRequests)
	}
	if !strings.Contains(err.Error(), `"type":"usage_limit_reached"`) {
		t.Fatalf("error message = %q, want usage_limit_reached body", err.Error())
	}
	retryAfter := pluginErrorRetryAfterSeconds(err)
	if retryAfter == nil || *retryAfter != 120 {
		t.Fatalf("pluginErrorRetryAfterSeconds() = %v, want 120", retryAfter)
	}
}

func TestPassThroughErrorBodyAndRetryableClassification(t *testing.T) {
	if got := passThroughErrorBody([]byte("  hello  ")); got != "hello" {
		t.Fatalf("passThroughErrorBody() = %q, want %q", got, "hello")
	}
	long := strings.Repeat("a", maxErrorBodyBytes+10)
	if got := passThroughErrorBody([]byte(long)); !strings.HasSuffix(got, "...") || len(got) != maxErrorBodyBytes+3 {
		t.Fatalf("passThroughErrorBody(long) length = %d, want capped", len(got))
	}
	if got := isRetryableNetworkError(io.ErrUnexpectedEOF); !got {
		t.Fatal("isRetryableNetworkError(io.ErrUnexpectedEOF) = false, want true")
	}
	if got := isRetryableNetworkError(context.Canceled); got {
		t.Fatal("isRetryableNetworkError(context.Canceled) = true, want false")
	}
}

func TestLogPluginEventDropsNonErrorLevels(t *testing.T) {
	var called bool
	withHostCallStub(t, func(method string, payload any) (json.RawMessage, error) {
		if method == pluginabi.MethodHostLog {
			called = true
		}
		return nil, nil
	})
	if err := logPluginEvent("cb", "info", "should not log", map[string]any{"x": 1}); err != nil {
		t.Fatalf("logPluginEvent(info) error = %v", err)
	}
	if called {
		t.Fatal("info log reached host, want error-only logging")
	}
	if err := logPluginEvent("cb", "error", "codex request failed", map[string]any{"error": "boom"}); err != nil {
		t.Fatalf("logPluginEvent(error) error = %v", err)
	}
	if !called {
		t.Fatal("error log did not reach host")
	}
}
