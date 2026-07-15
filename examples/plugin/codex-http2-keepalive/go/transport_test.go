package main

import (
	"strings"
	"testing"
	"time"
)

func TestCodexPoolManagerReusesPoolsAndAppliesTimeouts(t *testing.T) {
	manager := newCodexPoolManager()
	cfg := defaultPluginConfig()
	cfg.ReadIdleTimeout = 11 * time.Second
	cfg.PingTimeout = 13 * time.Second
	cfg.MaxIdleConnections = 4

	pool1, err := manager.acquire(defaultCodexResponsesURL, "", cfg)
	if err != nil {
		t.Fatalf("acquire pool1: %v", err)
	}
	pool2, err := manager.acquire(defaultCodexResponsesURL, "", cfg)
	if err != nil {
		t.Fatalf("acquire pool2: %v", err)
	}
	if pool1 != pool2 {
		t.Fatal("expected identical pool for same upstream and proxy")
	}
	if pool1.transport.ReadIdleTimeout != cfg.ReadIdleTimeout {
		t.Fatalf("ReadIdleTimeout = %s, want %s", pool1.transport.ReadIdleTimeout, cfg.ReadIdleTimeout)
	}
	if pool1.transport.PingTimeout != cfg.PingTimeout {
		t.Fatalf("PingTimeout = %s, want %s", pool1.transport.PingTimeout, cfg.PingTimeout)
	}

	pool3, err := manager.acquire(defaultCodexResponsesURL, "socks5://user:pass@proxy.example:1080", cfg)
	if err != nil {
		t.Fatalf("acquire pool3: %v", err)
	}
	if pool3 == pool1 {
		t.Fatal("expected different pool for different proxy settings")
	}
	if got := len(manager.snapshot()); got != 2 {
		t.Fatalf("snapshot size = %d, want 2", got)
	}
}

func TestNewCodexTransportDialerAcceptsSupportedProxySchemes(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantProxy bool
	}{
		{name: "direct-empty", raw: "", wantProxy: false},
		{name: "direct-word", raw: "direct", wantProxy: false},
		{name: "none-word", raw: "none", wantProxy: false},
		{name: "http", raw: "http://user:pass@proxy.example:8080", wantProxy: true},
		{name: "https", raw: "https://user:pass@proxy.example:8443", wantProxy: true},
		{name: "socks5", raw: "socks5://user:pass@proxy.example:1080", wantProxy: true},
		{name: "socks5h", raw: "socks5h://user:pass@proxy.example:1080", wantProxy: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer, err := newCodexTransportDialer(tt.raw)
			if err != nil {
				t.Fatalf("newCodexTransportDialer() error = %v", err)
			}
			if tt.wantProxy && dialer.proxyDialer == nil {
				t.Fatalf("proxyDialer = nil for %q, want proxy dialer", tt.raw)
			}
			if !tt.wantProxy && dialer.proxyDialer != nil {
				t.Fatalf("proxyDialer = %#v for %q, want nil", dialer.proxyDialer, tt.raw)
			}
		})
	}
}

func TestCodexPoolManagerShutdownClosesPools(t *testing.T) {
	manager := newCodexPoolManager()
	cfg := defaultPluginConfig()

	pool, err := manager.acquire(defaultCodexResponsesURL, "", cfg)
	if err != nil {
		t.Fatalf("acquire pool: %v", err)
	}
	manager.Shutdown()
	if !pool.shutdown.Load() {
		t.Fatal("pool shutdown flag = false, want true")
	}
	if got := len(manager.snapshot()); got != 0 {
		t.Fatalf("snapshot size after shutdown = %d, want 0", got)
	}
}

func TestMaskProxyURLRedactsCredentials(t *testing.T) {
	got := maskProxyURL("socks5://user:secret@proxy.example:1080/path?token=abc")
	if got == "" || got == "invalid-proxy" {
		t.Fatalf("maskProxyURL() = %q, want masked URL", got)
	}
	if strings.Contains(got, "user:secret") || strings.Contains(got, "token=abc") {
		t.Fatalf("maskProxyURL() = %q, want redacted output", got)
	}
}
