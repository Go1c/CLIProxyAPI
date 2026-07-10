package auth

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
)

type proxyFailoverExecutor struct {
	calls []string
}

func (e *proxyFailoverExecutor) Identifier() string { return "codex" }

func (e *proxyFailoverExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls = append(e.calls, auth.ID)
	if authProxyHash(auth) == proxyutil.Hash("socks5://proxy-a.example.com:443") {
		return cliproxyexecutor.Response{}, proxyutil.NewError(proxyutil.CodeConnectTimeout, proxyutil.StageProxyConnect, true, authProxyHash(auth), "proxy connection timed out", context.DeadlineExceeded)
	}
	return cliproxyexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}

func (e *proxyFailoverExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, &Error{Code: "not_implemented", Message: "not implemented"}
}

func (e *proxyFailoverExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *proxyFailoverExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *proxyFailoverExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, &Error{Code: "not_implemented", Message: "not implemented"}
}

func TestManagerProxyFailoverSelectsDifferentProxyHash(t *testing.T) {
	proxyutil.ResetHealthForTesting()
	t.Cleanup(proxyutil.ResetHealthForTesting)
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetConfig(&config.Config{})
	executor := &proxyFailoverExecutor{}
	manager.RegisterExecutor(executor)
	registerProxyFailoverAuth(t, manager, "auth-a", "socks5://user-a:pass-a@proxy-a.example.com:443")
	registerProxyFailoverAuth(t, manager, "auth-b", "socks5://user-b:pass-b@proxy-a.example.com:443")
	registerProxyFailoverAuth(t, manager, "auth-c", "socks5://user-c:pass-c@proxy-c.example.com:443")

	if _, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}); errExecute != nil {
		t.Fatalf("Execute returned error: %v", errExecute)
	}
	if len(executor.calls) != 2 || executor.calls[0] != "auth-a" || executor.calls[1] != "auth-c" {
		t.Fatalf("calls = %#v, want auth-a then auth-c", executor.calls)
	}
}

func TestManagerProxyFailoverDoesNotRetrySameProxyHash(t *testing.T) {
	proxyutil.ResetHealthForTesting()
	t.Cleanup(proxyutil.ResetHealthForTesting)
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetConfig(&config.Config{})
	executor := &proxyFailoverExecutor{}
	manager.RegisterExecutor(executor)
	registerProxyFailoverAuth(t, manager, "auth-a", "socks5://user-a:pass-a@proxy-a.example.com:443")
	registerProxyFailoverAuth(t, manager, "auth-b", "socks5://user-b:pass-b@proxy-a.example.com:443")

	_, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	proxyErr, ok := proxyutil.AsError(errExecute)
	if !ok || proxyErr.Code != proxyutil.CodeConnectTimeout {
		t.Fatalf("error = %v, want %s", errExecute, proxyutil.CodeConnectTimeout)
	}
	if len(executor.calls) != 1 || executor.calls[0] != "auth-a" {
		t.Fatalf("calls = %#v, same proxy must not be retried", executor.calls)
	}
}

func TestManagerProxyFailureDoesNotEnterOuterRetryLoop(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	errProxy := proxyutil.NewError(
		proxyutil.CodeConnectTimeout,
		proxyutil.StageProxyConnect,
		true,
		"proxy-a",
		"proxy connection timed out",
		context.DeadlineExceeded,
	)

	if wait, retry := manager.shouldRetryAfterError(errProxy, 0, []string{"codex"}, "gpt-5", time.Minute); retry || wait != 0 {
		t.Fatalf("proxy failure outer retry = %v after %v, want disabled", retry, wait)
	}
}

func TestManagerExcludesInvalidRequiredProxyCredential(t *testing.T) {
	proxyutil.ResetHealthForTesting()
	t.Cleanup(proxyutil.ResetHealthForTesting)
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetConfig(&config.Config{SDKConfig: config.SDKConfig{CodexProxyRequired: true}})
	executor := &proxyFailoverExecutor{}
	manager.RegisterExecutor(executor)
	registerProxyFailoverAuth(t, manager, "auth-direct", "")

	if _, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatal("Execute error = nil, want unavailable credential")
	}
	if len(executor.calls) != 0 {
		t.Fatalf("invalid credential reached executor: %#v", executor.calls)
	}
}

func registerProxyFailoverAuth(t *testing.T, manager *Manager, id, proxyURL string) {
	t.Helper()
	_, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: id, Provider: "codex", ProxyURL: proxyURL})
	if errRegister != nil {
		t.Fatalf("register %s: %v", id, errRegister)
	}
}
