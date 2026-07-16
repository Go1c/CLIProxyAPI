package helps

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestApplyCodexCacheAssemblyOpenAIUsesStableAPIKey(t *testing.T) {
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Set("userApiKey", "assembly-api-key")
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	originalHeaders := http.Header{
		"Session-Id": []string{"old-hyphen"},
		"session_id": []string{"old-underscore"},
		"X-Keep":     []string{"yes"},
	}

	assembly, errAssembly := ApplyCodexCacheAssembly(ctx, sdktranslator.FormatOpenAI, "gpt-5.4", []byte(`{"model":"gpt-5.4"}`), []byte(`{"model":"gpt-5.4","stream":true}`), originalHeaders)
	if errAssembly != nil {
		t.Fatalf("ApplyCodexCacheAssembly() error = %v", errAssembly)
	}
	expectedKey := uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:codex:prompt-cache:assembly-api-key")).String()
	if got := gjson.GetBytes(assembly.Body, "prompt_cache_key").String(); got != expectedKey {
		t.Fatalf("prompt_cache_key = %q, want %q", got, expectedKey)
	}
	if got := assembly.Headers["Session_id"]; len(got) != 1 || got[0] != expectedKey {
		t.Fatalf("Session_id = %#v, want [%q]", got, expectedKey)
	}
	if got := assembly.Headers.Get("Session-Id"); got != "" {
		t.Fatalf("Session-Id = %q, want empty", got)
	}
	if got := assembly.Headers.Get("X-Keep"); got != "yes" {
		t.Fatalf("X-Keep = %q, want yes", got)
	}
	if got := originalHeaders.Get("Session-Id"); got != "old-hyphen" {
		t.Fatalf("input headers were mutated: Session-Id = %q", got)
	}
	if !assembly.CacheKeyPresent || assembly.Source != CodexCacheSourceAPIKey || assembly.SessionHeaderKey != "Session_id" {
		t.Fatalf("assembly metadata = %#v", assembly)
	}
}

func TestApplyCodexCacheAssemblyOpenAIResponsesUsesClientKey(t *testing.T) {
	assembly, errAssembly := ApplyCodexCacheAssembly(context.Background(), sdktranslator.FormatOpenAIResponse, "gpt-5.4", []byte(`{"model":"gpt-5.4","prompt_cache_key":"client-cache"}`), []byte(`{"model":"gpt-5.4"}`), nil)
	if errAssembly != nil {
		t.Fatalf("ApplyCodexCacheAssembly() error = %v", errAssembly)
	}
	if got := gjson.GetBytes(assembly.Body, "prompt_cache_key").String(); got != "client-cache" {
		t.Fatalf("prompt_cache_key = %q, want client-cache", got)
	}
	if got := assembly.Headers["Session_id"]; len(got) != 1 || got[0] != "client-cache" {
		t.Fatalf("Session_id = %#v, want [client-cache]", got)
	}
	if assembly.Source != CodexCacheSourceClientBody {
		t.Fatalf("Source = %q, want %q", assembly.Source, CodexCacheSourceClientBody)
	}
}

func TestApplyCodexCacheAssemblyClaudeSessionIsStable(t *testing.T) {
	payload := []byte(`{"model":"gpt-5.4","metadata":{"user_id":"{\"session_id\":\"assembly-claude-session\"}"}}`)
	first, errFirst := ApplyCodexCacheAssembly(context.Background(), sdktranslator.FormatClaude, "gpt-5.4-assembly", payload, []byte(`{"model":"gpt-5.4"}`), nil)
	if errFirst != nil {
		t.Fatalf("first ApplyCodexCacheAssembly() error = %v", errFirst)
	}
	second, errSecond := ApplyCodexCacheAssembly(context.Background(), sdktranslator.FormatClaude, "gpt-5.4-assembly", payload, []byte(`{"model":"gpt-5.4"}`), nil)
	if errSecond != nil {
		t.Fatalf("second ApplyCodexCacheAssembly() error = %v", errSecond)
	}
	firstKey := gjson.GetBytes(first.Body, "prompt_cache_key").String()
	secondKey := gjson.GetBytes(second.Body, "prompt_cache_key").String()
	if firstKey == "" || secondKey != firstKey {
		t.Fatalf("Claude session keys = %q/%q, want same non-empty key", firstKey, secondKey)
	}
	if first.Source != CodexCacheSourceClaudeSession {
		t.Fatalf("Source = %q, want %q", first.Source, CodexCacheSourceClaudeSession)
	}
}

func TestApplyCodexCacheAssemblyCodexSourceDoesNotSynthesizeKey(t *testing.T) {
	assembly, errAssembly := ApplyCodexCacheAssembly(context.Background(), sdktranslator.FormatCodex, "gpt-5.4", []byte(`{"model":"gpt-5.4"}`), []byte(`{"model":"gpt-5.4"}`), nil)
	if errAssembly != nil {
		t.Fatalf("ApplyCodexCacheAssembly() error = %v", errAssembly)
	}
	if got := gjson.GetBytes(assembly.Body, "prompt_cache_key").String(); got != "" {
		t.Fatalf("prompt_cache_key = %q, want empty", got)
	}
	if assembly.CacheKeyPresent || assembly.Source != CodexCacheSourceNone || len(assembly.Headers) != 0 {
		t.Fatalf("assembly = %#v, want no cache affinity", assembly)
	}
}

func TestCodexIdentityConfuseEnabledRequiresStableRouting(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  *config.Config
		want bool
	}{
		{name: "disabled", cfg: &config.Config{}, want: false},
		{name: "round robin", cfg: &config.Config{Codex: config.CodexConfig{IdentityConfuse: true}}, want: false},
		{name: "fill first", cfg: &config.Config{Routing: config.RoutingConfig{Strategy: "fill-first"}, Codex: config.CodexConfig{IdentityConfuse: true}}, want: true},
		{name: "session affinity", cfg: &config.Config{Routing: config.RoutingConfig{SessionAffinity: true}, Codex: config.CodexConfig{IdentityConfuse: true}}, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := CodexIdentityConfuseEnabled(tt.cfg); got != tt.want {
				t.Fatalf("CodexIdentityConfuseEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}
