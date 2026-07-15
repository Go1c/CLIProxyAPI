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
