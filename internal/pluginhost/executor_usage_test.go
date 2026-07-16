package pluginhost

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestCodexPluginExecutorPublishesNonStreamUsageWithUserIdentity(t *testing.T) {
	authID := "codex-plugin-usage-nonstream"
	records := registerCodexPluginUsageCollector(t, authID)
	executor := &fakeExecutor{execute: func(context.Context, pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
		return pluginapi.ExecutorResponse{Payload: []byte(`{
			"id":"resp-1",
			"usage":{
				"input_tokens":100,
				"output_tokens":10,
				"total_tokens":110,
				"input_tokens_details":{"cached_tokens":80}
			}
		}`)}, nil
	}}
	adapter := newCodexUsageTestAdapter(executor, "nonstream")
	ctx := codexPluginUsageTestContext("cpa-user-nonstream")
	auth := &coreauth.Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"email": "oauth-user@example.com"}}

	if _, errExecute := adapter.Execute(ctx, auth, coreexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4"}`)}, coreexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex}); errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	record := awaitCodexPluginUsageRecord(t, records)
	assertCodexPluginUsageIdentity(t, record, authID, "cpa-user-nonstream")
	if record.Detail.InputTokens != 100 || record.Detail.OutputTokens != 10 || record.Detail.TotalTokens != 110 || record.Detail.CacheReadTokens != 80 {
		t.Fatalf("usage detail = %#v", record.Detail)
	}
}

func TestCodexPluginExecutorPublishesStreamUsageWithUserIdentity(t *testing.T) {
	authID := "codex-plugin-usage-stream"
	records := registerCodexPluginUsageCollector(t, authID)
	chunks := make(chan pluginapi.ExecutorStreamChunk, 1)
	chunks <- pluginapi.ExecutorStreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":200,\"output_tokens\":20,\"total_tokens\":220,\"input_tokens_details\":{\"cached_tokens\":160}}}}\n\n")}
	close(chunks)
	executor := &fakeExecutor{executeStream: func(context.Context, pluginapi.ExecutorRequest) (pluginapi.ExecutorStreamResponse, error) {
		return pluginapi.ExecutorStreamResponse{Chunks: chunks}, nil
	}}
	adapter := newCodexUsageTestAdapter(executor, "stream")
	ctx := codexPluginUsageTestContext("cpa-user-stream")
	auth := &coreauth.Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"email": "oauth-user@example.com"}}

	stream, errStream := adapter.ExecuteStream(ctx, auth, coreexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4"}`)}, coreexecutor.Options{Stream: true, SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	for range stream.Chunks {
	}
	record := awaitCodexPluginUsageRecord(t, records)
	assertCodexPluginUsageIdentity(t, record, authID, "cpa-user-stream")
	if record.Detail.InputTokens != 200 || record.Detail.OutputTokens != 20 || record.Detail.TotalTokens != 220 || record.Detail.CacheReadTokens != 160 {
		t.Fatalf("usage detail = %#v", record.Detail)
	}
}

func newCodexUsageTestAdapter(executor pluginapi.ProviderExecutor, suffix string) *executorAdapter {
	record := normalizeTestCapabilityRecord(capabilityRecord{id: "codex-usage-" + suffix})
	host := newHostWithRecords(record)
	return &executorAdapter{
		host:          host,
		pluginID:      record.id,
		path:          record.path,
		version:       record.version,
		provider:      "codex",
		executor:      executor,
		inputFormats:  []sdktranslator.Format{sdktranslator.FormatCodex},
		outputFormats: []sdktranslator.Format{sdktranslator.FormatCodex},
	}
}

func codexPluginUsageTestContext(apiKey string) context.Context {
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Set("userApiKey", apiKey)
	return context.WithValue(context.Background(), "gin", ginCtx)
}

func registerCodexPluginUsageCollector(t *testing.T, authID string) <-chan coreusage.Record {
	t.Helper()
	records := make(chan coreusage.Record, 1)
	coreusage.RegisterNamedPlugin("test:codex-plugin-usage:"+authID, coreUsagePluginFunc(func(ctx context.Context, record coreusage.Record) {
		if record.AuthID != authID {
			return
		}
		select {
		case records <- record:
		default:
		}
	}))
	return records
}

func awaitCodexPluginUsageRecord(t *testing.T, records <-chan coreusage.Record) coreusage.Record {
	t.Helper()
	select {
	case record := <-records:
		return record
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Codex plugin usage record")
		return coreusage.Record{}
	}
}

func assertCodexPluginUsageIdentity(t *testing.T, record coreusage.Record, authID, apiKey string) {
	t.Helper()
	if record.Provider != "codex" || record.AuthID != authID || record.APIKey != apiKey || record.Source != "oauth-user@example.com" {
		t.Fatalf("usage identity = provider:%q auth:%q api_key:%q source:%q", record.Provider, record.AuthID, record.APIKey, record.Source)
	}
}
