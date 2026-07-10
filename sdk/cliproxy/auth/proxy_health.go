package auth

import (
	"errors"
	"strings"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
)

const (
	proxyModeAttribute      = "proxy_mode"
	proxyValidAttribute     = "proxy_valid"
	proxyEndpointAttribute  = "proxy_endpoint"
	proxyHashAttribute      = "proxy_hash"
	proxyErrorCodeAttribute = "proxy_error_code"
	proxyAttemptPrefix      = "proxy-hash:"
)

type ProxyRuntimeStatus struct {
	Mode               string
	Valid              bool
	Verified           bool
	Endpoint           string
	Hash               string
	CloudflarePOP      string
	CircuitState       string
	AuthCircuitState   string
	LastProbeAt        time.Time
	LastProbeLatencyMS int64
	LastErrorCode      string
}

func (m *Manager) normalizeCodexProxyRuntime(auth *Auth, cfg *internalconfig.Config) {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	for _, key := range []string{proxyModeAttribute, proxyValidAttribute, proxyEndpointAttribute, proxyHashAttribute, proxyErrorCodeAttribute} {
		delete(auth.Attributes, key)
	}
	raw := strings.TrimSpace(auth.ProxyURL)
	if raw == "" && cfg != nil {
		raw = strings.TrimSpace(cfg.ProxyURL)
	}
	setting, errParse := proxyutil.Parse(raw)
	if errParse != nil {
		auth.Attributes[proxyModeAttribute] = "invalid"
		auth.Attributes[proxyValidAttribute] = "false"
		auth.Attributes[proxyEndpointAttribute] = proxyutil.Redact(raw)
		auth.Attributes[proxyErrorCodeAttribute] = proxyutil.CodeConfigInvalid
		return
	}
	if cfg != nil && cfg.CodexProxyRequired && setting.Mode != proxyutil.ModeProxy {
		auth.Attributes[proxyModeAttribute] = "invalid"
		auth.Attributes[proxyValidAttribute] = "false"
		auth.Attributes[proxyErrorCodeAttribute] = proxyutil.CodeRequired
		return
	}
	auth.Attributes[proxyValidAttribute] = "true"
	switch setting.Mode {
	case proxyutil.ModeProxy:
		auth.Attributes[proxyModeAttribute] = "proxy"
		auth.Attributes[proxyEndpointAttribute] = proxyutil.Redact(raw)
		auth.Attributes[proxyHashAttribute] = proxyutil.Hash(raw)
	default:
		auth.Attributes[proxyModeAttribute] = "direct"
	}
}

func authProxyHash(auth *Auth) string {
	if auth == nil || auth.Attributes == nil {
		return ""
	}
	return strings.TrimSpace(auth.Attributes[proxyHashAttribute])
}

func authProxyConfigInvalid(auth *Auth) bool {
	return auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") && strings.EqualFold(strings.TrimSpace(authAttributeValue(auth, proxyValidAttribute)), "false")
}

func authAttributeValue(auth *Auth, key string) string {
	if auth == nil || auth.Attributes == nil {
		return ""
	}
	return auth.Attributes[key]
}

func authAttempted(tried map[string]struct{}, auth *Auth) bool {
	if auth == nil || len(tried) == 0 {
		return false
	}
	if _, ok := tried[auth.ID]; ok {
		return true
	}
	proxyHash := authProxyHash(auth)
	if proxyHash == "" {
		return false
	}
	_, ok := tried[proxyAttemptPrefix+proxyHash]
	return ok
}

func markProxyAttempted(tried map[string]struct{}, auth *Auth, err error) bool {
	proxyErr, ok := proxyutil.AsError(err)
	if !ok || !proxyErr.Retryable || tried == nil || auth == nil {
		return false
	}
	proxyHash := proxyErr.ProxyHash
	if proxyHash == "" {
		proxyHash = authProxyHash(auth)
	}
	if proxyHash == "" {
		return false
	}
	tried[proxyAttemptPrefix+proxyHash] = struct{}{}
	return true
}

func resultErrorFromExecution(err error) *Error {
	if err == nil {
		return nil
	}
	if proxyErr, ok := proxyutil.AsError(err); ok {
		return &Error{
			Code:      proxyErr.Code,
			Message:   proxyErr.Message,
			Retryable: proxyErr.Retryable,
		}
	}
	resultErr := &Error{Message: err.Error()}
	if statusErr, ok := errors.AsType[interface {
		error
		StatusCode() int
	}](err); ok && statusErr != nil {
		resultErr.HTTPStatus = statusErr.StatusCode()
	}
	return resultErr
}

func proxyErrorFromResult(auth *Auth, resultErr *Error) *proxyutil.Error {
	if auth == nil || resultErr == nil || !isProxyErrorCode(resultErr.Code) {
		return nil
	}
	stage := proxyutil.StageProxyConnect
	switch resultErr.Code {
	case proxyutil.CodeConfigInvalid, proxyutil.CodeRequired:
		stage = proxyutil.StageConfig
	case proxyutil.CodeTLSTimeout, proxyutil.CodeTLSFailed:
		stage = proxyutil.StageTLSHandshake
	case proxyutil.CodeUpstreamHeaderTimeout:
		stage = proxyutil.StageUpstreamHeader
	}
	return proxyutil.NewError(resultErr.Code, stage, resultErr.Retryable, authProxyHash(auth), resultErr.Message, nil)
}

func isProxyErrorCode(code string) bool {
	switch code {
	case proxyutil.CodeConfigInvalid, proxyutil.CodeRequired, proxyutil.CodeAuthFailed,
		proxyutil.CodeConnectTimeout, proxyutil.CodeConnectFailed, proxyutil.CodeTLSTimeout,
		proxyutil.CodeTLSFailed, proxyutil.CodeUpstreamHeaderTimeout:
		return true
	default:
		return false
	}
}

func (m *Manager) RefreshProxyHealth(proxyHash string) {
	if m == nil || proxyHash == "" || m.scheduler == nil {
		return
	}
	m.mu.RLock()
	auths := make([]*Auth, 0)
	for _, auth := range m.auths {
		if auth != nil && authProxyHash(auth) == proxyHash {
			auths = append(auths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	for _, auth := range auths {
		m.scheduler.upsertAuth(auth)
	}
}

func (m *Manager) ProxyRuntimeStatus(authID string) ProxyRuntimeStatus {
	status := ProxyRuntimeStatus{Mode: "direct", Valid: true, CircuitState: "closed", AuthCircuitState: "closed"}
	if m == nil {
		return status
	}
	auth, ok := m.GetByID(authID)
	if !ok || auth == nil {
		return status
	}
	status.Mode = strings.TrimSpace(authAttributeValue(auth, proxyModeAttribute))
	if status.Mode == "" {
		status.Mode = "direct"
	}
	status.Valid = !strings.EqualFold(strings.TrimSpace(authAttributeValue(auth, proxyValidAttribute)), "false")
	status.Endpoint = strings.TrimSpace(authAttributeValue(auth, proxyEndpointAttribute))
	status.Hash = authProxyHash(auth)
	runtimeStatus := proxyutil.Status(auth.ID, status.Hash, time.Now())
	status.Verified = runtimeStatus.ProxyVerified
	status.CloudflarePOP = runtimeStatus.CloudflarePOP
	status.CircuitState = runtimeStatus.CircuitState
	status.AuthCircuitState = runtimeStatus.AuthCircuitState
	status.LastProbeAt = runtimeStatus.LastProbeAt
	status.LastProbeLatencyMS = runtimeStatus.LastProbeLatencyMS
	status.LastErrorCode = runtimeStatus.LastErrorCode
	if !status.Valid {
		status.CircuitState = "invalid"
		status.LastErrorCode = strings.TrimSpace(authAttributeValue(auth, proxyErrorCodeAttribute))
	}
	return status
}
