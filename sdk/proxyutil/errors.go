package proxyutil

import (
	"errors"
	"net/http"
)

const (
	CodeConfigInvalid         = "proxy_config_invalid"
	CodeRequired              = "proxy_required"
	CodeAuthFailed            = "proxy_auth_failed"
	CodeConnectTimeout        = "proxy_connect_timeout"
	CodeConnectFailed         = "proxy_connect_failed"
	CodeTLSTimeout            = "proxy_tls_timeout"
	CodeTLSFailed             = "proxy_tls_failed"
	CodeUpstreamHeaderTimeout = "upstream_header_timeout"
)

const (
	StageConfig         = "config"
	StageProxyConnect   = "proxy_connect"
	StageTLSHandshake   = "tls_handshake"
	StageUpstreamHeader = "upstream_header"
)

// Error is a credential-safe network error used by proxy-aware transports.
// Cause is intentionally excluded from JSON because upstream errors may echo
// credentials embedded in a URL.
type Error struct {
	Code      string `json:"code"`
	Stage     string `json:"stage"`
	Retryable bool   `json:"retryable"`
	ProxyHash string `json:"proxy_hash,omitempty"`
	Message   string `json:"message"`
	Cause     error  `json:"-"`
}

func NewError(code, stage string, retryable bool, proxyHash, message string, cause error) *Error {
	if message == "" {
		message = defaultErrorMessage(code)
	}
	return &Error{
		Code:      code,
		Stage:     stage,
		Retryable: retryable,
		ProxyHash: proxyHash,
		Message:   message,
		Cause:     cause,
	}
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *Error) StatusCode() int {
	if e == nil {
		return 0
	}
	switch e.Code {
	case CodeConfigInvalid:
		return http.StatusBadRequest
	case CodeRequired:
		return http.StatusServiceUnavailable
	case CodeAuthFailed:
		return http.StatusBadGateway
	case CodeConnectTimeout, CodeTLSTimeout, CodeUpstreamHeaderTimeout:
		return http.StatusGatewayTimeout
	case CodeConnectFailed, CodeTLSFailed:
		return http.StatusBadGateway
	default:
		return http.StatusBadGateway
	}
}

func AsError(err error) (*Error, bool) {
	var proxyErr *Error
	if !errors.As(err, &proxyErr) || proxyErr == nil {
		return nil, false
	}
	return proxyErr, true
}

func defaultErrorMessage(code string) string {
	switch code {
	case CodeConfigInvalid:
		return "proxy URL is invalid"
	case CodeRequired:
		return "a valid Codex proxy is required"
	case CodeAuthFailed:
		return "proxy authentication failed"
	case CodeConnectTimeout:
		return "proxy connection timed out"
	case CodeConnectFailed:
		return "proxy connection failed"
	case CodeTLSTimeout:
		return "TLS handshake timed out"
	case CodeTLSFailed:
		return "TLS handshake failed"
	case CodeUpstreamHeaderTimeout:
		return "upstream response headers timed out"
	default:
		return "proxy request failed"
	}
}
