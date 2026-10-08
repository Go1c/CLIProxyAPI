package proxyutil

import (
	"sync"
	"time"
)

const (
	authTimeoutWindow       = 5 * time.Minute
	authIsolationDuration   = 5 * time.Minute
	proxyFailureThreshold   = 3
	proxyIsolationDuration  = 5 * time.Minute
	proxyProbeInterval      = time.Minute
	proxyProbeSuccessTarget = 2
)

type RuntimeStatus struct {
	ProxyHash          string    `json:"proxy_hash,omitempty"`
	ProxyVerified      bool      `json:"proxy_verified"`
	CloudflarePOP      string    `json:"cloudflare_pop,omitempty"`
	CircuitState       string    `json:"circuit_state"`
	AuthCircuitState   string    `json:"auth_circuit_state"`
	LastProbeAt        time.Time `json:"last_probe_at,omitempty"`
	LastProbeLatencyMS int64     `json:"last_probe_latency_ms,omitempty"`
	LastErrorCode      string    `json:"last_error_code,omitempty"`
}

type HealthTransition struct {
	ProxyOpened bool
	AuthOpened  bool
	ProxyHash   string
}

type proxyHealthState struct {
	consecutiveFailures int
	openUntil           time.Time
	probeSuccesses      int
	probeInFlight       bool
	lastProbeAt         time.Time
	lastProbeLatency    time.Duration
	lastErrorCode       string
	verified            bool
	cloudflarePOP       string
}

type authHealthState struct {
	headerTimeouts []time.Time
	openUntil      time.Time
	lastErrorCode  string
}

type runtimeHealthRegistry struct {
	mu      sync.Mutex
	proxies map[string]*proxyHealthState
	auths   map[string]*authHealthState
}

var runtimeHealth = runtimeHealthRegistry{
	proxies: make(map[string]*proxyHealthState),
	auths:   make(map[string]*authHealthState),
}

func RecordFailure(authID string, proxyErr *Error, now time.Time) HealthTransition {
	if proxyErr == nil {
		return HealthTransition{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	transition := HealthTransition{ProxyHash: proxyErr.ProxyHash}
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()

	if authID != "" {
		authState := runtimeHealth.auths[authID]
		if authState == nil {
			authState = &authHealthState{}
			runtimeHealth.auths[authID] = authState
		}
		authState.lastErrorCode = proxyErr.Code
		if proxyErr.Code == CodeUpstreamHeaderTimeout {
			cutoff := now.Add(-authTimeoutWindow)
			kept := authState.headerTimeouts[:0]
			for _, occurredAt := range authState.headerTimeouts {
				if occurredAt.After(cutoff) {
					kept = append(kept, occurredAt)
				}
			}
			authState.headerTimeouts = append(kept, now)
			if len(authState.headerTimeouts) >= 2 && !authState.openUntil.After(now) {
				authState.openUntil = now.Add(authIsolationDuration)
				transition.AuthOpened = true
			}
		}
	}

	if proxyErr.ProxyHash == "" || !countsTowardProxyCircuit(proxyErr.Code) {
		return transition
	}
	proxyState := runtimeHealth.proxies[proxyErr.ProxyHash]
	if proxyState == nil {
		proxyState = &proxyHealthState{}
		runtimeHealth.proxies[proxyErr.ProxyHash] = proxyState
	}
	proxyState.lastErrorCode = proxyErr.Code
	proxyState.consecutiveFailures++
	if proxyState.consecutiveFailures >= proxyFailureThreshold && !proxyState.openUntil.After(now) {
		proxyState.openUntil = now.Add(proxyIsolationDuration)
		proxyState.probeSuccesses = 0
		transition.ProxyOpened = true
	}
	return transition
}

func RecordSuccess(authID, proxyHash string, now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()
	if authState := runtimeHealth.auths[authID]; authState != nil && !authState.openUntil.After(now) {
		authState.lastErrorCode = ""
	}
	if proxyState := runtimeHealth.proxies[proxyHash]; proxyState != nil && !proxyState.openUntil.After(now) {
		proxyState.consecutiveFailures = 0
		proxyState.lastErrorCode = ""
	}
}

func MarkVerified(proxyHash, cloudflarePOP string, latency time.Duration, now time.Time) {
	if proxyHash == "" {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()
	state := runtimeHealth.proxies[proxyHash]
	if state == nil {
		state = &proxyHealthState{}
		runtimeHealth.proxies[proxyHash] = state
	}
	state.verified = true
	state.cloudflarePOP = cloudflarePOP
	state.lastProbeAt = now
	state.lastProbeLatency = latency
	state.lastErrorCode = ""
}

func Blocked(authID, proxyHash string, now time.Time) (bool, time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()
	if state := runtimeHealth.auths[authID]; state != nil {
		if state.openUntil.After(now) {
			return true, state.openUntil
		}
		state.openUntil = time.Time{}
		state.headerTimeouts = nil
	}
	if state := runtimeHealth.proxies[proxyHash]; state != nil {
		if state.openUntil.After(now) {
			return true, state.openUntil
		}
		state.openUntil = time.Time{}
		state.probeSuccesses = 0
		state.probeInFlight = false
		state.consecutiveFailures = 0
	}
	return false, time.Time{}
}

func Status(authID, proxyHash string, now time.Time) RuntimeStatus {
	if now.IsZero() {
		now = time.Now()
	}
	status := RuntimeStatus{
		ProxyHash:        proxyHash,
		CircuitState:     "closed",
		AuthCircuitState: "closed",
	}
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()
	if authState := runtimeHealth.auths[authID]; authState != nil {
		status.LastErrorCode = authState.lastErrorCode
		if authState.openUntil.After(now) {
			status.AuthCircuitState = "open"
		}
	}
	if proxyState := runtimeHealth.proxies[proxyHash]; proxyState != nil {
		status.ProxyVerified = proxyState.verified
		status.CloudflarePOP = proxyState.cloudflarePOP
		status.LastProbeAt = proxyState.lastProbeAt
		status.LastProbeLatencyMS = proxyState.lastProbeLatency.Milliseconds()
		if proxyState.lastErrorCode != "" {
			status.LastErrorCode = proxyState.lastErrorCode
		}
		if proxyState.openUntil.After(now) {
			if proxyState.probeInFlight {
				status.CircuitState = "half-open"
			} else {
				status.CircuitState = "open"
			}
		}
	}
	return status
}

func OpenProxyHashes(now time.Time) []string {
	if now.IsZero() {
		now = time.Now()
	}
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()
	hashes := make([]string, 0)
	for hash, state := range runtimeHealth.proxies {
		if state != nil && state.openUntil.After(now) {
			hashes = append(hashes, hash)
		}
	}
	return hashes
}

func TryBeginProbe(proxyHash string, now time.Time) bool {
	if proxyHash == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()
	state := runtimeHealth.proxies[proxyHash]
	if state == nil || !state.openUntil.After(now) || state.probeInFlight {
		return false
	}
	if !state.lastProbeAt.IsZero() && now.Sub(state.lastProbeAt) < proxyProbeInterval {
		return false
	}
	state.probeInFlight = true
	return true
}

func RecordProbe(proxyHash string, success bool, cloudflarePOP string, latency time.Duration, errorCode string, now time.Time) bool {
	if proxyHash == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()
	state := runtimeHealth.proxies[proxyHash]
	if state == nil {
		state = &proxyHealthState{}
		runtimeHealth.proxies[proxyHash] = state
	}
	state.probeInFlight = false
	state.lastProbeAt = now
	state.lastProbeLatency = latency
	if success {
		state.verified = true
		state.cloudflarePOP = cloudflarePOP
		state.lastErrorCode = ""
		state.probeSuccesses++
		if state.probeSuccesses >= proxyProbeSuccessTarget {
			state.openUntil = time.Time{}
			state.consecutiveFailures = 0
			state.probeSuccesses = 0
			return true
		}
		return false
	}
	state.lastErrorCode = errorCode
	state.probeSuccesses = 0
	state.openUntil = now.Add(proxyIsolationDuration)
	return false
}

func ResetHealthForTesting() {
	runtimeHealth.mu.Lock()
	defer runtimeHealth.mu.Unlock()
	runtimeHealth.proxies = make(map[string]*proxyHealthState)
	runtimeHealth.auths = make(map[string]*authHealthState)
}

func countsTowardProxyCircuit(code string) bool {
	switch code {
	case CodeAuthFailed, CodeConnectTimeout, CodeConnectFailed, CodeTLSTimeout, CodeTLSFailed, CodeUpstreamHeaderTimeout:
		return true
	default:
		return false
	}
}
