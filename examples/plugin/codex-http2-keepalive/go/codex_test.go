package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

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

func TestSummarizeBodyAndRetryableClassification(t *testing.T) {
	if got := summarizeBody([]byte("  hello \n world  ")); got != "hello world" {
		t.Fatalf("summarizeBody() = %q, want %q", got, "hello world")
	}
	if got := isRetryableNetworkError(io.ErrUnexpectedEOF); !got {
		t.Fatal("isRetryableNetworkError(io.ErrUnexpectedEOF) = false, want true")
	}
	if got := isRetryableNetworkError(context.Canceled); got {
		t.Fatal("isRetryableNetworkError(context.Canceled) = true, want false")
	}
}
