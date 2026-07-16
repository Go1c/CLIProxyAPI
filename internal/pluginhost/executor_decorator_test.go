package pluginhost

import (
	"context"
	"errors"
	"net/http"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestExecutorRequestDecoratorAppliesToExecuteAndStreamOnly(t *testing.T) {
	host := newHostWithRecords(normalizeTestCapabilityRecord(capabilityRecord{id: "decorated-executor"}))
	decorateCalls := 0
	host.RegisterExecutorRequestDecorator("plugin-provider", func(ctx context.Context, auth *coreauth.Auth, decoration ExecutorRequestDecoration) (ExecutorRequestDecoration, error) {
		decorateCalls++
		if auth == nil || auth.ID != "auth-1" || decoration.Model != "model-1" || decoration.SourceFormat != sdktranslator.FormatOpenAI {
			t.Fatalf("decoration input = %#v, auth=%#v", decoration, auth)
		}
		if string(decoration.SourcePayload) != "source-payload" || string(decoration.Payload) != "source-payload" {
			t.Fatalf("decoration payloads = %q/%q", decoration.SourcePayload, decoration.Payload)
		}
		if decoration.Headers == nil {
			decoration.Headers = make(http.Header)
		}
		decoration.Headers["Session_id"] = []string{"cache-1"}
		decoration.Payload = []byte("decorated-payload")
		return decoration, nil
	})

	streamChunks := make(chan pluginapi.ExecutorStreamChunk)
	close(streamChunks)
	executor := &fakeExecutor{
		execute: func(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
			if string(req.Payload) != "decorated-payload" || req.Headers.Get("Session_id") != "cache-1" {
				t.Fatalf("Execute request = %#v", req)
			}
			return pluginapi.ExecutorResponse{Payload: []byte("ok")}, nil
		},
		executeStream: func(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorStreamResponse, error) {
			if string(req.Payload) != "decorated-payload" || req.Headers.Get("Session_id") != "cache-1" {
				t.Fatalf("ExecuteStream request = %#v", req)
			}
			return pluginapi.ExecutorStreamResponse{Chunks: streamChunks}, nil
		},
		countTokens: func(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
			if string(req.Payload) != "source-payload" || req.Headers.Get("Session_id") != "" {
				t.Fatalf("CountTokens request was decorated: %#v", req)
			}
			return pluginapi.ExecutorResponse{Payload: []byte(`{"total_tokens":1}`)}, nil
		},
	}
	adapter := newExecutorAdapterForRecordForTest(host, capabilityRecord{id: "decorated-executor"}, executor,
		[]sdktranslator.Format{sdktranslator.FormatOpenAI}, []sdktranslator.Format{sdktranslator.FormatOpenAI})
	auth := &coreauth.Auth{ID: "auth-1", Provider: "plugin-provider"}
	req := coreexecutor.Request{Model: "model-1", Payload: []byte("source-payload")}
	opts := coreexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI}

	if _, errExecute := adapter.Execute(context.Background(), auth, req, opts); errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if _, errStream := adapter.ExecuteStream(context.Background(), auth, req, opts); errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	if _, errCount := adapter.CountTokens(context.Background(), auth, req, opts); errCount != nil {
		t.Fatalf("CountTokens() error = %v", errCount)
	}
	if decorateCalls != 2 {
		t.Fatalf("decorator calls = %d, want 2", decorateCalls)
	}
	if opts.Headers != nil {
		t.Fatalf("input options headers were mutated: %#v", opts.Headers)
	}
}

func TestExecutorRequestDecoratorErrorStopsExecution(t *testing.T) {
	host := newHostWithRecords(normalizeTestCapabilityRecord(capabilityRecord{id: "decorator-error"}))
	wantErr := errors.New("cache assembly failed")
	host.RegisterExecutorRequestDecorator("plugin-provider", func(context.Context, *coreauth.Auth, ExecutorRequestDecoration) (ExecutorRequestDecoration, error) {
		return ExecutorRequestDecoration{}, wantErr
	})
	called := false
	executor := &fakeExecutor{execute: func(context.Context, pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
		called = true
		return pluginapi.ExecutorResponse{}, nil
	}}
	adapter := newExecutorAdapterForRecordForTest(host, capabilityRecord{id: "decorator-error"}, executor,
		[]sdktranslator.Format{sdktranslator.FormatOpenAI}, []sdktranslator.Format{sdktranslator.FormatOpenAI})

	_, errExecute := adapter.Execute(context.Background(), &coreauth.Auth{}, coreexecutor.Request{Payload: []byte(`{}`)}, coreexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI})
	if !errors.Is(errExecute, wantErr) {
		t.Fatalf("Execute() error = %v, want %v", errExecute, wantErr)
	}
	if called {
		t.Fatal("plugin executor was called after decorator error")
	}
}

func TestExecutorRequestDecoratorOnlyAppliesToRegisteredProvider(t *testing.T) {
	host := newHostWithRecords(normalizeTestCapabilityRecord(capabilityRecord{id: "provider-scope"}))
	decorateCalls := 0
	host.RegisterExecutorRequestDecorator("other-provider", func(context.Context, *coreauth.Auth, ExecutorRequestDecoration) (ExecutorRequestDecoration, error) {
		decorateCalls++
		return ExecutorRequestDecoration{}, nil
	})
	executor := &fakeExecutor{execute: func(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
		if string(req.Payload) != "original" {
			t.Fatalf("payload = %q, want original", req.Payload)
		}
		return pluginapi.ExecutorResponse{Payload: []byte("ok")}, nil
	}}
	adapter := newExecutorAdapterForRecordForTest(host, capabilityRecord{id: "provider-scope"}, executor,
		[]sdktranslator.Format{sdktranslator.FormatOpenAI}, []sdktranslator.Format{sdktranslator.FormatOpenAI})

	if _, errExecute := adapter.Execute(context.Background(), nil, coreexecutor.Request{Payload: []byte("original")}, coreexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI}); errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if decorateCalls != 0 {
		t.Fatalf("decorator calls = %d, want 0", decorateCalls)
	}
}
