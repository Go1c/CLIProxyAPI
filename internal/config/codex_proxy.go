package config

import "time"

const (
	DefaultCodexProxyConnectTimeout   = 10 * time.Second
	DefaultCodexTLSHandshakeTimeout   = 15 * time.Second
	DefaultCodexResponseHeaderTimeout = 90 * time.Second
	DefaultCodexFirstByteTimeout      = 120 * time.Second
)

type CodexProxyTimeouts struct {
	ProxyConnect   time.Duration
	TLSHandshake   time.Duration
	ResponseHeader time.Duration
	FirstByte      time.Duration
}

func (c *Config) CodexProxyTimeouts() CodexProxyTimeouts {
	timeouts := CodexProxyTimeouts{
		ProxyConnect:   DefaultCodexProxyConnectTimeout,
		TLSHandshake:   DefaultCodexTLSHandshakeTimeout,
		ResponseHeader: DefaultCodexResponseHeaderTimeout,
		FirstByte:      DefaultCodexFirstByteTimeout,
	}
	if c == nil {
		return timeouts
	}
	if c.Codex.ProxyConnectTimeoutSeconds > 0 {
		timeouts.ProxyConnect = time.Duration(c.Codex.ProxyConnectTimeoutSeconds) * time.Second
	}
	if c.Codex.TLSHandshakeTimeoutSeconds > 0 {
		timeouts.TLSHandshake = time.Duration(c.Codex.TLSHandshakeTimeoutSeconds) * time.Second
	}
	if c.Codex.ResponseHeaderTimeoutSeconds > 0 {
		timeouts.ResponseHeader = time.Duration(c.Codex.ResponseHeaderTimeoutSeconds) * time.Second
	}
	if c.Codex.FirstByteTimeoutSeconds > 0 {
		timeouts.FirstByte = time.Duration(c.Codex.FirstByteTimeoutSeconds) * time.Second
	}
	return timeouts
}
