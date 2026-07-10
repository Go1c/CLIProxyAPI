package config

import (
	"testing"
	"time"
)

func TestCodexProxyTimeoutDefaultsAndOverrides(t *testing.T) {
	defaults := (&Config{}).CodexProxyTimeouts()
	if defaults.ProxyConnect != 10*time.Second || defaults.TLSHandshake != 15*time.Second || defaults.ResponseHeader != 90*time.Second || defaults.FirstByte != 120*time.Second {
		t.Fatalf("defaults = %#v", defaults)
	}
	overrides := (&Config{Codex: CodexConfig{ProxyConnectTimeoutSeconds: 1, TLSHandshakeTimeoutSeconds: 2, ResponseHeaderTimeoutSeconds: 3, FirstByteTimeoutSeconds: 4}}).CodexProxyTimeouts()
	if overrides.ProxyConnect != time.Second || overrides.TLSHandshake != 2*time.Second || overrides.ResponseHeader != 3*time.Second || overrides.FirstByte != 4*time.Second {
		t.Fatalf("overrides = %#v", overrides)
	}
}
