package cliproxy

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/pluginhost"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexPluginExecutorRequestDecoratorInjectsNativeCacheAffinity(t *testing.T) {
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Set("userApiKey", "plugin-decorator-api-key")
	ctx := context.WithValue(context.Background(), "gin", ginCtx)

	decoration, errDecorate := codexPluginExecutorRequestDecorator(ctx, nil, pluginhost.ExecutorRequestDecoration{
		Model:         "gpt-5.4",
		SourceFormat:  sdktranslator.FormatOpenAI,
		SourcePayload: []byte(`{"model":"gpt-5.4"}`),
		Payload:       []byte(`{"model":"gpt-5.4","stream":true}`),
	})
	if errDecorate != nil {
		t.Fatalf("codexPluginExecutorRequestDecorator() error = %v", errDecorate)
	}
	expectedKey := uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:codex:prompt-cache:plugin-decorator-api-key")).String()
	if got := gjson.GetBytes(decoration.Payload, "prompt_cache_key").String(); got != expectedKey {
		t.Fatalf("prompt_cache_key = %q, want %q", got, expectedKey)
	}
	if got := decoration.Headers["Session_id"]; len(got) != 1 || got[0] != expectedKey {
		t.Fatalf("Session_id = %#v, want [%q]", got, expectedKey)
	}
}
