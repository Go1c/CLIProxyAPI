package helps

import (
	"bytes"
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CodexCacheSource identifies how a Codex prompt cache key was resolved.
type CodexCacheSource string

const (
	CodexCacheSourceNone          CodexCacheSource = "none"
	CodexCacheSourceClaudeSession CodexCacheSource = "claude-session"
	CodexCacheSourceAPIKey        CodexCacheSource = "api-key"
	CodexCacheSourceClientBody    CodexCacheSource = "client-body"
)

// CodexCacheAssembly contains the prepared Codex body, headers, and safe
// observability metadata for prompt cache affinity.
type CodexCacheAssembly struct {
	Body             []byte
	Headers          http.Header
	CacheKeyPresent  bool
	Source           CodexCacheSource
	SessionHeaderKey string
}

// ApplyCodexCacheAssembly derives the same stable prompt_cache_key used by the
// native Codex HTTP executor, injects it into the translated Codex body, and
// aligns the Session_id header for executor paths that bypass native assembly.
func ApplyCodexCacheAssembly(ctx context.Context, sourceFormat sdktranslator.Format, modelName string, sourcePayload, translatedBody []byte, headers http.Header) (CodexCacheAssembly, error) {
	assembly := CodexCacheAssembly{
		Body:             bytes.Clone(translatedBody),
		Headers:          cloneCodexCacheHeaders(headers),
		CacheKeyPresent:  strings.TrimSpace(gjson.GetBytes(translatedBody, "prompt_cache_key").String()) != "",
		Source:           CodexCacheSourceNone,
		SessionHeaderKey: codexCacheSessionHeaderKey(headers),
	}

	cacheID := ""
	cacheSource := CodexCacheSourceNone
	switch {
	case codexSourceFormatEqual(sourceFormat, sdktranslator.FormatClaude):
		cached, ok, errCache := ClaudeCodePromptCache(ctx, modelName, sourcePayload, headers)
		if errCache != nil {
			return CodexCacheAssembly{}, errCache
		}
		if ok {
			cacheID = cached.ID
			cacheSource = CodexCacheSourceClaudeSession
		}
	case codexSourceFormatEqual(sourceFormat, sdktranslator.FormatOpenAIResponse):
		if promptCacheKey := gjson.GetBytes(sourcePayload, "prompt_cache_key"); promptCacheKey.Exists() {
			cacheID = promptCacheKey.String()
			if cacheID != "" {
				cacheSource = CodexCacheSourceClientBody
			}
		}
	case codexSourceFormatEqual(sourceFormat, sdktranslator.FormatOpenAI):
		if apiKey := strings.TrimSpace(APIKeyFromContext(ctx)); apiKey != "" {
			cacheID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:codex:prompt-cache:"+apiKey)).String()
			cacheSource = CodexCacheSourceAPIKey
		}
	}

	if cacheID == "" {
		return assembly, nil
	}
	assembly.Body, _ = sjson.SetBytes(assembly.Body, "prompt_cache_key", cacheID)
	assembly.SessionHeaderKey = setCodexCacheSessionHeader(assembly.Headers, "Session_id", cacheID)
	assembly.CacheKeyPresent = true
	assembly.Source = cacheSource
	return assembly, nil
}

// CodexIdentityConfuseEnabled reports whether IdentityConfuse is active for
// the current routing configuration.
func CodexIdentityConfuseEnabled(cfg *config.Config) bool {
	if cfg == nil || !cfg.Codex.IdentityConfuse {
		return false
	}
	strategy := strings.ToLower(strings.TrimSpace(cfg.Routing.Strategy))
	return cfg.Routing.SessionAffinity || strategy == "fill-first" || strategy == "fillfirst" || strategy == "ff"
}

func cloneCodexCacheHeaders(headers http.Header) http.Header {
	if len(headers) == 0 {
		return make(http.Header)
	}
	return headers.Clone()
}

// SetCodexCacheSessionHeader replaces any existing Session_id-style header with key/value.
func SetCodexCacheSessionHeader(headers http.Header, key, value string) string {
	return setCodexCacheSessionHeader(headers, key, value)
}

func setCodexCacheSessionHeader(headers http.Header, key, value string) string {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if headers == nil || key == "" || value == "" {
		return ""
	}
	for existingKey := range headers {
		normalized := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(existingKey)), "_", "-")
		if normalized == "session-id" {
			delete(headers, existingKey)
		}
	}
	headers[key] = []string{value}
	return key
}

func codexCacheSessionHeaderKey(headers http.Header) string {
	for key := range headers {
		normalized := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "_", "-")
		if normalized == "session-id" {
			return key
		}
	}
	return ""
}

func codexSourceFormatEqual(from, want sdktranslator.Format) bool {
	return strings.EqualFold(strings.TrimSpace(from.String()), want.String())
}
