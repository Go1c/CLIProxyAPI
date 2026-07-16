package pluginhost

import (
	"bytes"
	"context"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	usage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

func (a *executorAdapter) codexUsageReporter(ctx context.Context, auth *coreauth.Auth, model string) *helps.UsageReporter {
	if a == nil || !strings.EqualFold(strings.TrimSpace(a.provider), "codex") {
		return nil
	}
	return helps.NewExecutorUsageReporter(ctx, a, model, auth)
}

func publishCodexPluginNonStreamUsage(ctx context.Context, reporter *helps.UsageReporter, payload []byte) {
	if reporter == nil {
		return
	}
	reporter.Publish(ctx, helps.ParseOpenAIUsage(payload))
	reporter.EnsurePublished(ctx)
}

func observeCodexPluginStreamUsage(ctx context.Context, reporter *helps.UsageReporter, in <-chan pluginapi.ExecutorStreamChunk) <-chan pluginapi.ExecutorStreamChunk {
	if reporter == nil {
		return in
	}
	if in == nil {
		reporter.EnsurePublished(ctx)
		return nil
	}
	out := make(chan pluginapi.ExecutorStreamChunk)
	go func() {
		defer close(out)
		var done <-chan struct{}
		if ctx != nil {
			done = ctx.Done()
		}
		for {
			select {
			case <-done:
				reporter.PublishFailure(ctx, ctx.Err())
				return
			case chunk, ok := <-in:
				if !ok {
					reporter.EnsurePublished(ctx)
					return
				}
				if chunk.Err != nil {
					reporter.PublishFailure(ctx, chunk.Err)
				} else if len(chunk.Payload) > 0 {
					reporter.MarkFirstResponseByte()
					if detail, okUsage := parseCodexPluginStreamUsage(chunk.Payload); okUsage {
						reporter.Publish(ctx, detail)
					}
				}
				select {
				case out <- chunk:
				case <-done:
					reporter.PublishFailure(ctx, ctx.Err())
					return
				}
			}
		}
	}()
	return out
}

func parseCodexPluginStreamUsage(payload []byte) (detail usage.Detail, ok bool) {
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return usage.Detail{}, false
	}
	for _, line := range bytes.Split(trimmedPayload, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if detail, okUsage := helps.ParseCodexUsage(data); okUsage {
			return detail, true
		}
	}
	if detail, okUsage := helps.ParseCodexUsage(trimmedPayload); okUsage {
		return detail, true
	}
	if gjson.GetBytes(trimmedPayload, "usage").Exists() {
		return helps.ParseOpenAIUsage(trimmedPayload), true
	}
	return usage.Detail{}, false
}
