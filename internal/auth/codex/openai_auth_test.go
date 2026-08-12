package codex

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"golang.org/x/sync/singleflight"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewCodexAuthDoesNotSetRequestTimeout(t *testing.T) {
	if got := NewCodexAuth(nil).httpClient.Timeout; got != 0 {
		t.Fatalf("HTTP client timeout = %s, want zero", got)
	}
}

func TestRefreshTokens_UsesIndependentTimeout(t *testing.T) {
	resetCodexRefreshGroupForTest()
	defer resetCodexRefreshGroupForTest()

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	cancelCaller()
	var requestDeadline time.Time
	auth := &CodexAuth{
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				var ok bool
				requestDeadline, ok = req.Context().Deadline()
				if !ok {
					t.Fatal("refresh request has no deadline")
				}
				if errContext := req.Context().Err(); errContext != nil {
					t.Fatalf("refresh request context is already done: %v", errContext)
				}
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(strings.NewReader(`{"error":"probe"}`)),
					Header:     make(http.Header),
					Request:    req,
				}, nil
			}),
		},
	}

	_, err := auth.RefreshTokens(callerCtx, "independent-timeout-token")
	if err == nil {
		t.Fatal("expected refresh error")
	}
	if requestDeadline.IsZero() || !requestDeadline.After(time.Now()) {
		t.Fatalf("refresh deadline = %v, want a future deadline", requestDeadline)
	}
}

func resetCodexRefreshGroupForTest() {
	codexRefreshGroup = singleflight.Group{}
}

func TestRefreshTokensWithRetry_NonRetryableOnlyAttemptsOnce(t *testing.T) {
	var calls int32
	auth := &CodexAuth{
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				atomic.AddInt32(&calls, 1)
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(strings.NewReader(`{"error":"invalid_grant","code":"refresh_token_reused"}`)),
					Header:     make(http.Header),
					Request:    req,
				}, nil
			}),
		},
	}

	_, err := auth.RefreshTokensWithRetry(context.Background(), "dummy_refresh_token", 3)
	if err == nil {
		t.Fatalf("expected error for non-retryable refresh failure")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "refresh_token_reused") {
		t.Fatalf("expected refresh_token_reused in error, got: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected 1 refresh attempt, got %d", got)
	}
}

func TestRefreshTokens_DeduplicatesConcurrentRefreshAcrossInstances(t *testing.T) {
	resetCodexRefreshGroupForTest()
	t.Cleanup(resetCodexRefreshGroupForTest)

	var calls int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		once.Do(func() { close(started) })
		<-release
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"access_token":"new-access",
				"refresh_token":"new-refresh",
				"token_type":"Bearer",
				"expires_in":3600
			}`)),
			Header:  make(http.Header),
			Request: req,
		}, nil
	})
	authA := &CodexAuth{httpClient: &http.Client{Transport: transport}}
	authB := &CodexAuth{httpClient: &http.Client{Transport: transport}}

	results := make(chan *CodexTokenData, 2)
	errs := make(chan error, 2)
	runRefresh := func(auth *CodexAuth, launched chan<- struct{}) {
		if launched != nil {
			close(launched)
		}
		tokenData, errRefresh := auth.RefreshTokens(context.Background(), "shared-refresh-token")
		results <- tokenData
		errs <- errRefresh
	}

	go runRefresh(authA, nil)
	<-started

	secondLaunched := make(chan struct{})
	go runRefresh(authB, secondLaunched)
	<-secondLaunched
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected concurrent refresh to share a single upstream call, got %d", got)
	}
	close(release)

	for i := 0; i < 2; i++ {
		if errRefresh := <-errs; errRefresh != nil {
			t.Fatalf("expected refresh to succeed, got %v", errRefresh)
		}
		tokenData := <-results
		if tokenData == nil || tokenData.AccessToken != "new-access" || tokenData.RefreshToken != "new-refresh" {
			t.Fatalf("unexpected token data: %#v", tokenData)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected both refresh callers to share a single upstream call, got %d", got)
	}
}

func TestNewCodexAuthWithProxyURL_OverrideDirectDisablesProxy(t *testing.T) {
	cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "http://proxy.example.com:8080"}}
	auth := NewCodexAuthWithProxyURL(cfg, "direct")

	info, ok := helps.CodexFingerprintTransportInfoOf(auth.httpClient.Transport)
	if !ok {
		t.Fatalf("expected Codex fingerprint transport, got %T", auth.httpClient.Transport)
	}
	if !info.Enabled {
		t.Fatal("expected OAuth fingerprint transport to be enabled")
	}
	// "direct" must not inherit env/global proxy and should not hash a proxy endpoint.
	if info.InheritEnvProxy {
		t.Fatal("expected direct override to disable env proxy inheritance")
	}
	if info.ProxyHash != "" {
		t.Fatalf("proxy hash = %q, want empty for direct", info.ProxyHash)
	}
}

func TestNewCodexAuthWithProxyURL_OverrideProxyTakesPrecedence(t *testing.T) {
	cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "http://global.example.com:8080"}}
	auth := NewCodexAuthWithProxyURL(cfg, "http://override.example.com:8081")

	info, ok := helps.CodexFingerprintTransportInfoOf(auth.httpClient.Transport)
	if !ok {
		t.Fatalf("expected Codex fingerprint transport, got %T", auth.httpClient.Transport)
	}
	if !info.Enabled {
		t.Fatal("expected OAuth fingerprint transport to be enabled")
	}
	if info.InheritEnvProxy {
		t.Fatal("expected explicit proxy override to disable env proxy inheritance")
	}
	wantHash := ""
	// ProxyHash is a credential-free endpoint hash; just require non-empty for proxy mode.
	if info.ProxyHash == "" {
		t.Fatal("proxy hash empty, want non-empty for explicit proxy override")
	}
	_ = wantHash
}

func TestNewCodexAuthUsesFingerprintTransport(t *testing.T) {
	auth := NewCodexAuth(nil)
	if auth == nil || auth.httpClient == nil {
		t.Fatal("expected CodexAuth with http client")
	}
	if auth.httpClient.Timeout != 0 {
		t.Fatalf("Timeout = %v, want 0", auth.httpClient.Timeout)
	}
	info, ok := helps.CodexFingerprintTransportInfoOf(auth.httpClient.Transport)
	if !ok {
		t.Fatalf("expected Codex fingerprint transport, got %T", auth.httpClient.Transport)
	}
	if !info.Enabled {
		t.Fatal("expected fingerprint transport enabled for OAuth")
	}
}
