package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/tiktoken-go/tokenizer"
)

type codexAuthSettings struct {
	AccessToken string
	AccountID   string
	ProxyURL    string
	BaseURL     string
	AuthID      string
}

type codexUsageSummary struct {
	InputTokens     int64 `json:"input_tokens,omitempty"`
	OutputTokens    int64 `json:"output_tokens,omitempty"`
	ReasoningTokens int64 `json:"reasoning_tokens,omitempty"`
	TotalTokens     int64 `json:"total_tokens,omitempty"`
}

type codexProcessState struct {
	sawSSE          bool
	completed       bool
	emitted         bool
	firstDataAt     time.Duration
	usage           codexUsageSummary
	finalBody       []byte
	outputItemsByID map[int64][]byte
	outputFallback  [][]byte
}

type codexExecutionOutcome struct {
	AuthID          string
	Model           string
	UpstreamURL     string
	ProxyURL        string
	PoolKey         string
	Usage           codexUsageSummary
	TTFT            time.Duration
	Duration        time.Duration
	StatusCode      int
	Attempts        int
	Stream          bool
	CacheKeyPresent bool
}

func execute(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	cfg := currentConfig()
	ctx, cancel := requestContext(req.HostCallbackID)
	defer cancel()
	resp, outcome, err := runCodexExecution(ctx, req, cfg, false)
	if err != nil {
		return errorEnvelope(pluginErrorCode(err), err.Error(), isRetryableError(err), pluginErrorStatus(err)), nil
	}
	logCodexCompletion(req.HostCallbackID, outcome, nil)
	return okEnvelope(resp)
}

func executeStream(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.StreamID) == "" {
		return errorEnvelope("invalid_request", "stream_id is required for executor.execute_stream", false, http.StatusBadRequest), nil
	}
	cfg := currentConfig()
	ctx, cancel := requestContext(req.HostCallbackID)
	go func() {
		defer cancel()
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = closePluginStream(req.StreamID, fmt.Sprintf("stream panic: %v", recovered))
			}
		}()
		resp, outcome, errRun := runCodexExecution(ctx, req, cfg, true)
		_ = resp
		if errRun != nil {
			logCodexFailure(req.HostCallbackID, outcome, errRun)
			_ = closePluginStream(req.StreamID, errRun.Error())
			return
		}
		logCodexCompletion(req.HostCallbackID, outcome, nil)
		_ = closePluginStream(req.StreamID, "")
	}()
	return okEnvelope(rpcExecutorStreamResponse{
		Headers: http.Header{"Content-Type": []string{"text/event-stream"}},
	})
}

func requestContext(hostCallbackID string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	if strings.TrimSpace(hostCallbackID) == "" {
		return ctx, cancel
	}
	go func() {
		raw, errWait := hostCall(pluginabi.MethodHostContextWait, rpcHostContextWaitRequest{HostCallbackID: hostCallbackID})
		if errWait != nil {
			return
		}
		var resp rpcHostContextWaitResponse
		if errDecode := json.Unmarshal(raw, &resp); errDecode == nil && resp.Canceled {
			cancel()
		}
	}()
	return ctx, cancel
}

func countTokens(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	body := req.Payload
	if len(body) == 0 {
		body = req.OriginalRequest
	}
	model := resolveCodexModel(req)
	if model != "" {
		updated, errSet := sjson.SetBytes(body, "model", model)
		if errSet != nil {
			return errorEnvelope("token_count_failed", errSet.Error(), false, http.StatusBadRequest), nil
		}
		body = updated
	}
	body, _ = sjson.SetBytes(body, "stream", false)
	enc, errTokenizer := tokenizerForCodexModel(model)
	if errTokenizer != nil {
		return errorEnvelope("token_count_failed", errTokenizer.Error(), false, http.StatusInternalServerError), nil
	}
	count, errCount := countCodexInputTokens(enc, body)
	if errCount != nil {
		return errorEnvelope("token_count_failed", errCount.Error(), false, http.StatusInternalServerError), nil
	}
	payload := []byte(fmt.Sprintf(`{"response":{"usage":{"input_tokens":%d,"output_tokens":0,"total_tokens":%d}}}`, count, count))
	return okEnvelope(pluginapi.ExecutorResponse{Payload: payload})
}

func tokenizerForCodexModel(model string) (tokenizer.Codec, error) {
	sanitized := strings.ToLower(strings.TrimSpace(model))
	switch {
	case sanitized == "":
		return tokenizer.Get(tokenizer.Cl100kBase)
	case strings.HasPrefix(sanitized, "gpt-5"):
		return tokenizer.ForModel(tokenizer.GPT5)
	case strings.HasPrefix(sanitized, "gpt-4.1"):
		return tokenizer.ForModel(tokenizer.GPT41)
	case strings.HasPrefix(sanitized, "gpt-4o"):
		return tokenizer.ForModel(tokenizer.GPT4o)
	case strings.HasPrefix(sanitized, "gpt-4"):
		return tokenizer.ForModel(tokenizer.GPT4)
	case strings.HasPrefix(sanitized, "gpt-3.5"), strings.HasPrefix(sanitized, "gpt-3"):
		return tokenizer.ForModel(tokenizer.GPT35Turbo)
	default:
		return tokenizer.Get(tokenizer.Cl100kBase)
	}
}

func countCodexInputTokens(enc tokenizer.Codec, body []byte) (int64, error) {
	if enc == nil {
		return 0, fmt.Errorf("encoder is nil")
	}
	if len(body) == 0 {
		return 0, nil
	}
	root := gjson.ParseBytes(body)
	segments := make([]string, 0, 16)
	if instructions := strings.TrimSpace(root.Get("instructions").String()); instructions != "" {
		segments = append(segments, instructions)
	}
	for _, item := range root.Get("input").Array() {
		switch item.Get("type").String() {
		case "message":
			for _, part := range item.Get("content").Array() {
				if text := strings.TrimSpace(part.Get("text").String()); text != "" {
					segments = append(segments, text)
				}
			}
		case "function_call":
			segments = appendNonEmpty(segments, item.Get("name").String(), item.Get("arguments").String())
		case "function_call_output":
			segments = appendNonEmpty(segments, item.Get("output").String())
		default:
			segments = appendNonEmpty(segments, item.Get("text").String())
		}
	}
	for _, tool := range root.Get("tools").Array() {
		segments = appendNonEmpty(segments, tool.Get("name").String(), tool.Get("description").String())
		if parameters := tool.Get("parameters"); parameters.Exists() {
			value := parameters.Raw
			if parameters.Type == gjson.String {
				value = parameters.String()
			}
			segments = appendNonEmpty(segments, value)
		}
	}
	if format := root.Get("text.format"); format.Exists() {
		segments = appendNonEmpty(segments, format.Get("name").String())
		if schema := format.Get("schema"); schema.Exists() {
			value := schema.Raw
			if schema.Type == gjson.String {
				value = schema.String()
			}
			segments = appendNonEmpty(segments, value)
		}
	}
	text := strings.Join(segments, "\n")
	if text == "" {
		return 0, nil
	}
	count, errCount := enc.Count(text)
	return int64(count), errCount
}

func appendNonEmpty(dst []string, values ...string) []string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			dst = append(dst, value)
		}
	}
	return dst
}

func runCodexExecution(ctx context.Context, req rpcExecutorRequest, cfg pluginConfig, stream bool) (pluginapi.ExecutorResponse, codexExecutionOutcome, error) {
	settings, errSettings := resolveCodexAuthSettings(req, cfg)
	if errSettings != nil {
		return pluginapi.ExecutorResponse{}, codexExecutionOutcome{}, errSettings
	}
	model := resolveCodexModel(req)
	baseURL, errBase := normalizeResponsesEndpoint(settings.BaseURL)
	if errBase != nil {
		return pluginapi.ExecutorResponse{}, codexExecutionOutcome{}, errBase
	}
	pool, errPool := runtime.pools.acquireForRequest(baseURL, settings.ProxyURL, cfg)
	if errPool != nil {
		return pluginapi.ExecutorResponse{}, codexExecutionOutcome{}, errPool
	}
	defer pool.recordRequestDone()
	requestBody, errBody := prepareCodexRequestBody(req, model)
	if errBody != nil {
		pool.recordFailure(errBody)
		return pluginapi.ExecutorResponse{}, codexExecutionOutcome{AuthID: settings.AuthID, Model: model, UpstreamURL: baseURL, ProxyURL: settings.ProxyURL, PoolKey: pool.key, Stream: stream}, errBody
	}
	headers := buildCodexRequestHeaders(req, settings)
	outcome := codexExecutionOutcome{
		AuthID:          settings.AuthID,
		Model:           model,
		UpstreamURL:     baseURL,
		ProxyURL:        settings.ProxyURL,
		PoolKey:         pool.key,
		Stream:          stream,
		CacheKeyPresent: strings.TrimSpace(gjson.GetBytes(requestBody, "prompt_cache_key").String()) != "",
	}
	logCodexStart(req.HostCallbackID, outcome)
	var attempt int
	var lastErr error
	for attempt = 0; attempt < 2; attempt++ {
		requestStart := time.Now()
		httpReq, errRequest := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(requestBody))
		if errRequest != nil {
			pool.recordFailure(errRequest)
			return pluginapi.ExecutorResponse{}, outcome, errRequest
		}
		httpReq.Header = cloneHeaders(headers)
		resp, errDo := pool.clientDo(httpReq)
		if errDo != nil {
			lastErr = errDo
			pool.recordFailure(errDo)
			if attempt == 0 && cfg.RetryNetworkErrors && isRetryableNetworkError(errDo) {
				pool.CloseIdleConnections()
				continue
			}
			return pluginapi.ExecutorResponse{}, outcome, errDo
		}
		result, errProcess := processCodexResponse(ctx, resp, requestStart, req, settings, stream, pool)
		if errClose := resp.Body.Close(); errClose != nil {
			_ = logPluginEvent(req.HostCallbackID, "warn", "failed to close upstream body", map[string]any{
				"plugin_id": pluginIdentifier,
				"error":     errClose.Error(),
				"pool":      pool.key,
			})
		}
		if errProcess != nil {
			lastErr = errProcess
			pool.recordFailure(errProcess)
			if attempt == 0 && cfg.RetryNetworkErrors && isRetryableNetworkError(errProcess) && !result.emitted {
				pool.CloseIdleConnections()
				continue
			}
			outcome.Attempts = attempt + 1
			outcome.Duration = time.Since(requestStart)
			return pluginapi.ExecutorResponse{}, outcome, errProcess
		}
		pool.recordSuccess()
		outcome.Attempts = attempt + 1
		outcome.Duration = time.Since(requestStart)
		outcome.TTFT = result.ttft
		outcome.Usage = result.usage
		outcome.StatusCode = result.statusCode
		if stream {
			return pluginapi.ExecutorResponse{Headers: result.headers}, outcome, nil
		}
		return pluginapi.ExecutorResponse{Payload: result.finalBody, Headers: result.headers}, outcome, nil
	}
	return pluginapi.ExecutorResponse{}, outcome, lastErr
}

type codexProcessResult struct {
	finalBody  []byte
	headers    http.Header
	usage      codexUsageSummary
	ttft       time.Duration
	statusCode int
	emitted    bool
}

func processCodexResponse(ctx context.Context, resp *http.Response, requestStarted time.Time, req rpcExecutorRequest, settings codexAuthSettings, stream bool, pool *codexUpstreamPool) (codexProcessResult, error) {
	result := codexProcessResult{
		headers:    sanitizeResponseHeaders(resp.Header, stream),
		statusCode: resp.StatusCode,
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, errRead := io.ReadAll(resp.Body)
		if errRead != nil {
			return result, errRead
		}
		return result, newPluginError("upstream_status", http.StatusBadGateway, false, fmt.Sprintf("upstream returned status %d: %s", resp.StatusCode, summarizeBody(body)), nil)
	}

	reader := bufio.NewReader(resp.Body)
	var raw bytes.Buffer
	state := codexProcessState{
		outputItemsByID: make(map[int64][]byte),
	}
	for {
		line, errRead := reader.ReadBytes('\n')
		if len(line) > 0 {
			// raw only feeds the non-SSE fallback below; once SSE is detected,
			// buffering the stream would hold the full transcript in memory.
			if !state.sawSSE {
				raw.Write(line)
			}
			outLine, done, errHandle := processCodexLine(&state, line, requestStarted, stream)
			if stream && len(outLine) > 0 {
				if errEmit := emitPluginStreamChunk(req.StreamID, outLine); errEmit != nil {
					return result, errEmit
				}
				result.emitted = true
			}
			if errHandle != nil {
				return result, errHandle
			}
			if done {
				break
			}
		}
		if errRead == io.EOF {
			break
		}
		if errRead != nil {
			return result, errRead
		}
	}

	if !state.sawSSE {
		result.finalBody = bytes.Clone(raw.Bytes())
		if stream && len(result.finalBody) > 0 && !result.emitted {
			if errEmit := emitPluginStreamChunk(req.StreamID, result.finalBody); errEmit != nil {
				return result, errEmit
			}
			result.emitted = true
		}
		return result, nil
	}
	if !state.completed {
		return result, newPluginError("codex_response_incomplete", http.StatusBadGateway, true, "stream closed before response.completed", nil)
	}
	result.finalBody = bytes.Clone(state.finalBody)
	result.usage = state.usage
	result.ttft = state.firstDataAt
	return result, nil
}

func processCodexLine(state *codexProcessState, line []byte, requestStarted time.Time, streaming bool) ([]byte, bool, error) {
	if state == nil {
		return line, false, nil
	}
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return line, false, nil
	}
	if !bytes.HasPrefix(trimmed, []byte("data:")) {
		return line, false, nil
	}
	state.sawSSE = true
	if state.firstDataAt == 0 {
		state.firstDataAt = time.Since(requestStarted)
	}
	data := bytes.TrimSpace(trimmed[5:])
	eventType := gjson.GetBytes(data, "type").String()
	switch eventType {
	case "response.output_item.done":
		collectCodexOutputItemDone(data, state.outputItemsByID, &state.outputFallback)
	case "response.completed":
		patched := patchCodexCompletedOutput(data, state.outputItemsByID, state.outputFallback)
		if usage, ok := parseCodexUsage(patched); ok {
			state.usage = usage
		}
		state.completed = true
		state.finalBody = codexCompletedResponseBody(patched)
		if streaming {
			return rebuildSSELine(line, patched), true, nil
		}
		return nil, true, nil
	case "response.failed", "error":
		body := codexErrorBody(data)
		if streaming {
			return rebuildSSELine(line, data), false, newPluginError("codex_response_error", http.StatusBadGateway, false, body, nil)
		}
		return nil, false, newPluginError("codex_response_error", http.StatusBadGateway, false, body, nil)
	}
	if streaming {
		return line, false, nil
	}
	return nil, false, nil
}

func resolveCodexAuthSettings(req rpcExecutorRequest, cfg pluginConfig) (codexAuthSettings, error) {
	settings := codexAuthSettings{
		AuthID: req.AuthID,
	}
	storage := decodeStringMap(req.StorageJSON)
	configuredBaseURL := strings.TrimSpace(cfg.BaseURL)
	if configuredBaseURL == "" {
		configuredBaseURL = defaultCodexResponsesURL
	}
	settings.BaseURL = configuredBaseURL
	if authBaseURL := firstStringFromSources(storage, req.AuthMetadata, req.AuthAttributes, "base_url"); authBaseURL != "" {
		allowed, errAllowed := sameCodexOrigin(authBaseURL, configuredBaseURL)
		if errAllowed != nil {
			return settings, newPluginError("invalid_base_url", http.StatusBadRequest, false, errAllowed.Error(), errAllowed)
		}
		if !allowed {
			return settings, newPluginError("invalid_base_url", http.StatusBadRequest, false, "oauth base_url must match the configured Codex origin", nil)
		}
		settings.BaseURL = authBaseURL
	}
	settings.AccessToken = firstStringFromSources(storage, req.AuthMetadata, req.AuthAttributes, "access_token")
	settings.AccountID = firstStringFromSources(storage, req.AuthMetadata, req.AuthAttributes, "account_id")
	settings.ProxyURL = firstStringFromSources(storage, req.AuthMetadata, req.AuthAttributes, "proxy_url")
	if settings.AccessToken == "" {
		return settings, newPluginError("auth_not_found", http.StatusServiceUnavailable, false, "no oauth access token available", nil)
	}
	return settings, nil
}

func resolveCodexModel(req rpcExecutorRequest) string {
	if model := strings.TrimSpace(req.Model); model != "" {
		return model
	}
	if model := strings.TrimSpace(firstStringFromJSON(req.Payload, "model")); model != "" {
		return model
	}
	if model := strings.TrimSpace(firstStringFromJSON(req.OriginalRequest, "model")); model != "" {
		return model
	}
	return ""
}

// unsupportedCodexResponsesFields are top-level Responses parameters rejected by the
// ChatGPT Codex internal upstream (chatgpt.com/backend-api/codex/responses).
// Keep aligned with internal/runtime/executor/codex_executor.go and
// internal/translator/codex/openai/responses/codex_openai-responses_request.go.
var unsupportedCodexResponsesFields = []string{
	// codex_executor strips these on the HTTP responses path.
	"previous_response_id",
	"prompt_cache_retention",
	"safety_identifier",
	"stream_options",
	// translator strips these for Codex internal compatibility.
	"max_output_tokens",
	"max_completion_tokens",
	"temperature",
	"top_p",
	"truncation",
	"user",
	"context_management",
}

func prepareCodexRequestBody(req rpcExecutorRequest, model string) ([]byte, error) {
	body := req.Payload
	if len(body) == 0 {
		body = req.OriginalRequest
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("codex request body is empty")
	}
	body = stripUnsupportedCodexResponsesFields(body)
	if model != "" {
		updated, errSet := sjson.SetBytes(body, "model", model)
		if errSet != nil {
			return nil, errSet
		}
		body = updated
	}
	updated, errSet := sjson.SetBytes(body, "stream", true)
	if errSet != nil {
		return nil, errSet
	}
	return updated, nil
}

func stripUnsupportedCodexResponsesFields(body []byte) []byte {
	for _, field := range unsupportedCodexResponsesFields {
		body, _ = sjson.DeleteBytes(body, field)
	}
	// Codex internal upstream accepts service_tier only for priority processing.
	if tier := gjson.GetBytes(body, "service_tier"); tier.Exists() && tier.String() != "priority" {
		body, _ = sjson.DeleteBytes(body, "service_tier")
	}
	return normalizeCodexParallelToolCallsForTools(body)
}

// normalizeCodexParallelToolCallsForTools mirrors the codex_executor helper of the
// same name: upstream rejects parallel_tool_calls when the request carries no tools.
func normalizeCodexParallelToolCallsForTools(body []byte) []byte {
	if !gjson.GetBytes(body, "parallel_tool_calls").Exists() {
		return body
	}
	tools := gjson.GetBytes(body, "tools")
	if tools.Exists() && tools.IsArray() && len(tools.Array()) > 0 {
		return body
	}
	body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	return body
}

func buildCodexRequestHeaders(req rpcExecutorRequest, settings codexAuthSettings) http.Header {
	headers := sanitizeForwardHeaders(req.Headers)
	headers.Set("Content-Type", "application/json")
	headers.Set("Authorization", "Bearer "+settings.AccessToken)
	headers.Set("Connection", "Keep-Alive")
	// Codex Responses returns SSE upstream even when the plugin aggregates it for a non-streaming client.
	headers.Set("Accept", "text/event-stream")
	if headers.Get("User-Agent") == "" {
		headers.Set("User-Agent", defaultCodexUserAgent)
	}
	if headers.Get("Originator") == "" {
		headers.Set("Originator", defaultCodexOriginator)
	}
	if headers.Get("Version") == "" {
		headers.Set("Version", defaultCodexVersion)
	}
	if headers.Get("X-Codex-Beta-Features") == "" {
		headers.Set("X-Codex-Beta-Features", defaultCodexBetaFeatures)
	}
	if settings.AccountID != "" {
		headers.Set("Chatgpt-Account-Id", settings.AccountID)
	}
	applyAuthHeaderOverrides(headers, req.AuthAttributes)
	return headers
}

func sanitizeForwardHeaders(src http.Header) http.Header {
	dst := make(http.Header)
	for key, values := range src {
		normalized := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "_", "-")
		if _, ok := codexForwardHeaderAllowlist[normalized]; !ok {
			continue
		}
		dst[key] = append([]string(nil), values...)
	}
	return dst
}

var codexForwardHeaderAllowlist = map[string]struct{}{
	"user-agent":            {},
	"originator":            {},
	"version":               {},
	"x-codex-beta-features": {},
	"x-codex-turn-metadata": {},
	"x-client-request-id":   {},
	"session-id":            {},
	"conversation-id":       {},
	"thread-id":             {},
	"x-codex-window-id":     {},
}

func sanitizeResponseHeaders(src http.Header, stream bool) http.Header {
	dst := make(http.Header)
	for key, values := range src {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if normalized == "content-type" || normalized == "retry-after" || normalized == "x-request-id" ||
			strings.HasPrefix(normalized, "x-ratelimit-") || strings.HasPrefix(normalized, "openai-") {
			dst[key] = append([]string(nil), values...)
		}
	}
	if stream {
		dst.Set("Content-Type", "text/event-stream")
	} else {
		dst.Set("Content-Type", "application/json")
	}
	return dst
}

func applyAuthHeaderOverrides(headers http.Header, attrs map[string]string) {
	for key, value := range attrs {
		if !strings.HasPrefix(key, "header:") {
			continue
		}
		name := strings.TrimSpace(strings.TrimPrefix(key, "header:"))
		value = strings.TrimSpace(value)
		if name == "" || value == "" || strings.EqualFold(name, "Host") || strings.EqualFold(name, "Content-Length") {
			continue
		}
		headers.Set(name, value)
	}
}

func collectCodexOutputItemDone(eventData []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback *[][]byte) {
	itemResult := gjson.GetBytes(eventData, "item")
	if !itemResult.Exists() || itemResult.Type != gjson.JSON {
		return
	}
	outputIndexResult := gjson.GetBytes(eventData, "output_index")
	if outputIndexResult.Exists() {
		outputItemsByIndex[outputIndexResult.Int()] = []byte(itemResult.Raw)
		return
	}
	*outputItemsFallback = append(*outputItemsFallback, []byte(itemResult.Raw))
}

func patchCodexCompletedOutput(eventData []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	outputResult := gjson.GetBytes(eventData, "response.output")
	shouldPatch := (!outputResult.Exists() || !outputResult.IsArray() || len(outputResult.Array()) == 0) && (len(outputItemsByIndex) > 0 || len(outputItemsFallback) > 0)
	if !shouldPatch {
		return eventData
	}
	indexes := make([]int64, 0, len(outputItemsByIndex))
	for idx := range outputItemsByIndex {
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	patched := append([]byte(nil), eventData...)
	patched, _ = sjson.SetRawBytes(patched, "response.output", []byte(`[]`))
	for _, idx := range indexes {
		patched, _ = sjson.SetRawBytes(patched, "response.output.-1", outputItemsByIndex[idx])
	}
	for _, item := range outputItemsFallback {
		patched, _ = sjson.SetRawBytes(patched, "response.output.-1", item)
	}
	return patched
}

func codexCompletedResponseBody(eventData []byte) []byte {
	response := gjson.GetBytes(eventData, "response")
	if !response.Exists() || response.Type != gjson.JSON {
		return eventData
	}
	return []byte(response.Raw)
}

func parseCodexUsage(eventData []byte) (codexUsageSummary, bool) {
	usage := gjson.GetBytes(eventData, "response.usage")
	if !usage.Exists() || usage.Type != gjson.JSON {
		return codexUsageSummary{}, false
	}
	return codexUsageSummary{
		InputTokens:     usage.Get("input_tokens").Int(),
		OutputTokens:    usage.Get("output_tokens").Int(),
		ReasoningTokens: usage.Get("reasoning_tokens").Int(),
		TotalTokens:     usage.Get("total_tokens").Int(),
	}, true
}

func codexErrorBody(eventData []byte) string {
	candidates := []string{
		gjson.GetBytes(eventData, "response.error.message").String(),
		gjson.GetBytes(eventData, "error.message").String(),
		gjson.GetBytes(eventData, "message").String(),
	}
	for _, candidate := range candidates {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	if raw := strings.TrimSpace(string(eventData)); raw != "" {
		return raw
	}
	return "codex response error"
}

func summarizeBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return ""
	}
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 240 {
		text = text[:240] + "..."
	}
	return text
}

func rebuildSSELine(original []byte, patched []byte) []byte {
	suffix := []byte{}
	switch {
	case bytes.HasSuffix(original, []byte("\r\n")):
		suffix = []byte("\r\n")
	case bytes.HasSuffix(original, []byte("\n")):
		suffix = []byte("\n")
	}
	out := append([]byte("data: "), patched...)
	return append(out, suffix...)
}

func firstStringFromSources(storageJSON map[string]any, metadata map[string]any, attrs map[string]string, key string) string {
	if value := firstStringFromAnyMap(storageJSON, key); value != "" {
		return value
	}
	if value := firstStringFromAnyMap(metadata, key); value != "" {
		return value
	}
	if attrs != nil {
		if value := strings.TrimSpace(attrs[key]); value != "" {
			return value
		}
	}
	return ""
}

func decodeStringMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil
	}
	return parsed
}

func firstStringFromJSON(raw []byte, key string) string {
	if len(raw) == 0 {
		return ""
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return ""
	}
	return firstStringFromAnyMap(parsed, key)
}

func firstStringFromAnyMap(src map[string]any, key string) string {
	if len(src) == 0 {
		return ""
	}
	value, ok := src[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []byte:
		return strings.TrimSpace(string(typed))
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func pluginErrorCode(err error) string {
	var pe *pluginError
	if errors.As(err, &pe) {
		return pe.code
	}
	return "plugin_error"
}

func pluginErrorStatus(err error) int {
	var pe *pluginError
	if errors.As(err, &pe) && pe.statusCode > 0 {
		return pe.statusCode
	}
	return http.StatusInternalServerError
}

func isRetryableError(err error) bool {
	var pe *pluginError
	if errors.As(err, &pe) {
		return pe.retryable
	}
	return isRetryableNetworkError(err)
}

func logCodexStart(callbackID string, outcome codexExecutionOutcome) {
	_ = logPluginEvent(callbackID, "info", "codex request start", map[string]any{
		"plugin_id":         pluginIdentifier,
		"auth_id":           outcome.AuthID,
		"base_url":          outcome.UpstreamURL,
		"proxy":             proxyutil.Redact(outcome.ProxyURL),
		"pool_key":          outcome.PoolKey,
		"stream":            outcome.Stream,
		"model":             outcome.Model,
		"cache_key_present": outcome.CacheKeyPresent,
	})
}

func logCodexCompletion(callbackID string, outcome codexExecutionOutcome, err error) {
	fields := map[string]any{
		"plugin_id":   pluginIdentifier,
		"auth_id":     outcome.AuthID,
		"base_url":    outcome.UpstreamURL,
		"proxy":       proxyutil.Redact(outcome.ProxyURL),
		"pool_key":    outcome.PoolKey,
		"stream":      outcome.Stream,
		"model":       outcome.Model,
		"attempts":    outcome.Attempts,
		"status":      outcome.StatusCode,
		"ttft_ms":     outcome.TTFT.Milliseconds(),
		"duration_ms": outcome.Duration.Milliseconds(),
	}
	if outcome.Usage.InputTokens > 0 || outcome.Usage.OutputTokens > 0 || outcome.Usage.TotalTokens > 0 {
		fields["input_tokens"] = outcome.Usage.InputTokens
		fields["output_tokens"] = outcome.Usage.OutputTokens
		fields["reasoning_tokens"] = outcome.Usage.ReasoningTokens
		fields["total_tokens"] = outcome.Usage.TotalTokens
	}
	if err != nil {
		fields["error"] = err.Error()
		_ = logPluginEvent(callbackID, "error", "codex request failed", fields)
		return
	}
	_ = logPluginEvent(callbackID, "info", "codex request completed", fields)
}

func logCodexFailure(callbackID string, outcome codexExecutionOutcome, err error) {
	if err == nil {
		return
	}
	_ = logPluginEvent(callbackID, "error", "codex request failed", map[string]any{
		"plugin_id": pluginIdentifier,
		"auth_id":   outcome.AuthID,
		"base_url":  outcome.UpstreamURL,
		"proxy":     proxyutil.Redact(outcome.ProxyURL),
		"pool_key":  outcome.PoolKey,
		"stream":    outcome.Stream,
		"model":     outcome.Model,
		"attempts":  outcome.Attempts,
		"error":     err.Error(),
	})
}

func logPluginEvent(callbackID string, level, message string, fields map[string]any) error {
	if fields == nil {
		fields = make(map[string]any)
	}
	sanitized := sanitizeLogFields(fields)
	_, err := hostCall(pluginabi.MethodHostLog, rpcHostLogRequest{
		HostCallbackID: callbackID,
		Level:          level,
		Message:        message,
		Fields:         sanitized,
	})
	return err
}

func sanitizeLogFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string]any, len(fields))
	for key, value := range fields {
		lower := strings.ToLower(strings.TrimSpace(key))
		switch lower {
		case "authorization", "proxy-authorization", "access_token", "proxy_password":
			continue
		}
		out[key] = value
	}
	return out
}

func emitPluginStreamChunk(streamID string, payload []byte) error {
	if strings.TrimSpace(streamID) == "" {
		return fmt.Errorf("plugin stream id is required")
	}
	_, err := hostCall(pluginabi.MethodHostStreamEmit, rpcStreamEmitRequest{StreamID: streamID, Payload: payload})
	return err
}

func closePluginStream(streamID, errMsg string) error {
	if strings.TrimSpace(streamID) == "" {
		return nil
	}
	_, err := hostCall(pluginabi.MethodHostStreamClose, rpcStreamCloseRequest{
		StreamID: streamID,
		Error:    strings.TrimSpace(errMsg),
	})
	return err
}

type pluginError struct {
	code       string
	statusCode int
	retryable  bool
	message    string
	cause      error
}

func (e *pluginError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.message) != "" {
		return e.message
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.code
}

func newPluginError(code string, status int, retryable bool, message string, cause error) error {
	return &pluginError{code: code, statusCode: status, retryable: retryable, message: message, cause: cause}
}

func isRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return isRetryableNetworkError(urlErr.Err)
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EPIPE, syscall.ECONNRESET, syscall.ECONNREFUSED, syscall.ECONNABORTED, syscall.ETIMEDOUT, syscall.EHOSTUNREACH, syscall.ENETDOWN, syscall.ENETUNREACH:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "broken pipe"):
		return true
	case strings.Contains(msg, "connection reset by peer"):
		return true
	case strings.Contains(msg, "unexpected eof"):
		return true
	case strings.Contains(msg, "use of closed network connection"):
		return true
	case strings.Contains(msg, "server closed idle connection"):
		return true
	case strings.Contains(msg, "http2") && strings.Contains(msg, "connection lost"):
		return true
	case strings.Contains(msg, "i/o timeout"):
		return true
	}
	return false
}
