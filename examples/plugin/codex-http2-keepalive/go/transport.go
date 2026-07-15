package main

import (
	"context"
	"crypto/sha256"
	stdtls "crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tls "github.com/refraction-networking/utls"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	"golang.org/x/net/http2"
)

const (
	codexTransportConnectTimeout   = 30 * time.Second
	codexTransportHandshakeTimeout = 30 * time.Second
)

type codexPoolManager struct {
	mu     sync.Mutex
	pools  map[string]*codexUpstreamPool
	closed bool
}

type codexUpstreamPool struct {
	key              string
	upstreamURL      string
	upstreamHost     string
	proxyRaw         string
	proxyMasked      string
	readIdle         time.Duration
	pingTimeout      time.Duration
	client           *http.Client
	transport        *http2.Transport
	dialer           *codexTransportDialer
	tlsConfig        *tls.Config
	connections      map[*trackedConn]struct{}
	connMu           sync.Mutex
	requests         atomic.Uint64
	successes        atomic.Uint64
	failures         atomic.Uint64
	activeStreams    atomic.Int64
	connectionsNew   atomic.Uint64
	connectionsGone  atomic.Uint64
	lastSuccessNanos atomic.Int64
	lastErrorNanos   atomic.Int64
	lastErrorMu      sync.Mutex
	lastError        string
	shutdown         atomic.Bool
}

type codexTransportDialer struct {
	proxyRaw       string
	proxyDialer    proxyutil.ContextDialer
	connectTimeout time.Duration
	tlsTimeout     time.Duration
	tlsConfig      *tls.Config
	connected      chan struct{}
	connectedOnce  sync.Once
}

type poolSnapshot struct {
	UpstreamHost       string `json:"upstream_host"`
	Proxy              string `json:"proxy"`
	HTTP2Status        string `json:"http2_status"`
	ReadIdleTimeout    string `json:"read_idle_timeout"`
	PingTimeout        string `json:"ping_timeout"`
	Requests           uint64 `json:"requests"`
	Successes          uint64 `json:"successes"`
	Failures           uint64 `json:"failures"`
	ActiveStreams      int64  `json:"active_streams"`
	ConnectionsCreated uint64 `json:"connections_created"`
	ConnectionsRemoved uint64 `json:"connections_removed"`
	LastSuccess        string `json:"last_success,omitempty"`
	LastError          string `json:"last_error,omitempty"`
	LastErrorAt        string `json:"last_error_at,omitempty"`
}

type poolStatusResponse struct {
	GeneratedAt        string         `json:"generated_at"`
	Enabled            bool           `json:"enabled"`
	ManagementEnabled  bool           `json:"management_enabled"`
	BaseURL            string         `json:"base_url"`
	ModelPrefixes      []string       `json:"model_prefixes"`
	ReadIdleTimeout    string         `json:"read_idle_timeout"`
	PingTimeout        string         `json:"ping_timeout"`
	MaxIdleConnections int            `json:"max_idle_connections"`
	RetryNetworkErrors bool           `json:"retry_network_errors"`
	Pools              []poolSnapshot `json:"pools"`
}

type poolCloseResponse struct {
	ClosedIdlePools int            `json:"closed_idle_pools"`
	GeneratedAt     string         `json:"generated_at"`
	Pools           []poolSnapshot `json:"pools"`
}

func newCodexPoolManager() *codexPoolManager {
	return &codexPoolManager{pools: make(map[string]*codexUpstreamPool)}
}

func (m *codexPoolManager) acquire(baseURL, proxyURL string, cfg pluginConfig) (*codexUpstreamPool, error) {
	if m == nil {
		return nil, fmt.Errorf("codex pool manager is unavailable")
	}
	if m.closed {
		return nil, fmt.Errorf("codex pool manager is shut down")
	}
	normalizedBaseURL, errNormalize := normalizeResponsesEndpoint(baseURL)
	if errNormalize != nil {
		return nil, errNormalize
	}
	parsedURL, errParse := url.Parse(normalizedBaseURL)
	if errParse != nil {
		return nil, fmt.Errorf("parse upstream base url: %w", errParse)
	}
	key := poolKey(parsedURL, proxyURL, cfg.ReadIdleTimeout, cfg.PingTimeout)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("codex pool manager is shut down")
	}
	if pool := m.pools[key]; pool != nil && !pool.shutdown.Load() {
		pool.touch()
		return pool, nil
	}
	pool, errNew := newCodexUpstreamPool(key, normalizedBaseURL, proxyURL, cfg)
	if errNew != nil {
		return nil, errNew
	}
	m.pools[key] = pool
	pool.touch()
	m.evictIdleLocked(cfg.MaxIdleConnections)
	return pool, nil
}

func (m *codexPoolManager) evictIdleLocked(maxIdle int) {
	if m == nil || maxIdle <= 0 || len(m.pools) <= maxIdle {
		return
	}
	type candidate struct {
		key  string
		used time.Time
	}
	candidates := make([]candidate, 0, len(m.pools))
	for key, pool := range m.pools {
		if pool == nil || pool.activeStreams.Load() > 0 {
			continue
		}
		candidates = append(candidates, candidate{key: key, used: pool.lastUsed()})
	}
	if len(candidates) == 0 {
		return
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].used.Before(candidates[j].used)
	})
	for len(m.pools) > maxIdle && len(candidates) > 0 {
		victim := candidates[0]
		candidates = candidates[1:]
		pool := m.pools[victim.key]
		if pool == nil || pool.activeStreams.Load() > 0 {
			continue
		}
		pool.CloseIdleConnections()
		delete(m.pools, victim.key)
	}
}

func (m *codexPoolManager) CloseIdleConnections() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pool := range m.pools {
		if pool != nil {
			pool.CloseIdleConnections()
		}
	}
}

func (m *codexPoolManager) Shutdown() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	for key, pool := range m.pools {
		if pool != nil {
			pool.CloseAllConnections()
		}
		delete(m.pools, key)
	}
}

func (m *codexPoolManager) snapshot() []poolSnapshot {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]poolSnapshot, 0, len(m.pools))
	for _, pool := range m.pools {
		if pool == nil {
			continue
		}
		out = append(out, pool.snapshot())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpstreamHost == out[j].UpstreamHost {
			return out[i].Proxy < out[j].Proxy
		}
		return out[i].UpstreamHost < out[j].UpstreamHost
	})
	return out
}

func newCodexUpstreamPool(key, baseURL, proxyURL string, cfg pluginConfig) (*codexUpstreamPool, error) {
	normalizedBaseURL, errNormalize := normalizeResponsesEndpoint(baseURL)
	if errNormalize != nil {
		return nil, errNormalize
	}
	parsedURL, errParse := url.Parse(normalizedBaseURL)
	if errParse != nil {
		return nil, fmt.Errorf("parse upstream url: %w", errParse)
	}
	dialer, errDialer := newCodexTransportDialer(proxyURL)
	if errDialer != nil {
		return nil, errDialer
	}
	pool := &codexUpstreamPool{
		key:          key,
		upstreamURL:  normalizedBaseURL,
		upstreamHost: parsedURL.Host,
		proxyRaw:     strings.TrimSpace(proxyURL),
		proxyMasked:  maskProxyURL(proxyURL),
		readIdle:     cfg.ReadIdleTimeout,
		pingTimeout:  cfg.PingTimeout,
		dialer:       dialer,
		connections:  make(map[*trackedConn]struct{}),
		tlsConfig:    &tls.Config{ServerName: parsedURL.Hostname()},
	}
	pool.dialer.tlsConfig = pool.tlsConfig
	transport, errTransport := newCodexHTTP2Transport(pool)
	if errTransport != nil {
		return nil, errTransport
	}
	pool.transport = transport
	pool.client = &http.Client{Transport: transport}
	return pool, nil
}

func newCodexTransportDialer(proxyURL string) (*codexTransportDialer, error) {
	dialer := &codexTransportDialer{
		proxyRaw:       strings.TrimSpace(proxyURL),
		connectTimeout: codexTransportConnectTimeout,
		tlsTimeout:     codexTransportHandshakeTimeout,
		connected:      make(chan struct{}),
		tlsConfig:      &tls.Config{},
	}
	if dialer.proxyRaw == "" || strings.EqualFold(dialer.proxyRaw, "direct") || strings.EqualFold(dialer.proxyRaw, "none") {
		return dialer, nil
	}
	contextDialer, _, errBuild := proxyutil.BuildContextDialer(dialer.proxyRaw, codexTransportConnectTimeout)
	if errBuild != nil {
		return nil, errBuild
	}
	dialer.proxyDialer = contextDialer
	return dialer, nil
}

func newCodexHTTP2Transport(pool *codexUpstreamPool) (*http2.Transport, error) {
	if pool == nil || pool.dialer == nil {
		return nil, fmt.Errorf("codex transport dialer is unavailable")
	}
	transport := &http2.Transport{
		ReadIdleTimeout: pool.readIdle,
		PingTimeout:     pool.pingTimeout,
		CountError: func(errType string) {
			pool.recordTransportError(errType)
		},
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *stdtls.Config) (net.Conn, error) {
			serverName := ""
			if cfg != nil && strings.TrimSpace(cfg.ServerName) != "" {
				serverName = cfg.ServerName
			}
			if serverName == "" {
				if host, _, errSplit := net.SplitHostPort(addr); errSplit == nil {
					serverName = host
				} else {
					serverName = addr
				}
			}
			return pool.dialer.dialTLS(ctx, network, addr, serverName)
		},
	}
	return transport, nil
}

func (d *codexTransportDialer) dialTCP(ctx context.Context, network, addr string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stageCtx, cancel := context.WithTimeout(ctx, d.connectTimeout)
	defer cancel()
	started := time.Now()
	var conn net.Conn
	var errDial error
	if d.proxyDialer != nil {
		conn, errDial = d.proxyDialer.DialContext(stageCtx, network, addr)
	} else {
		conn, errDial = (&net.Dialer{Timeout: d.connectTimeout, KeepAlive: 30 * time.Second}).DialContext(stageCtx, network, addr)
	}
	if errDial == nil {
		return conn, nil
	}
	if errors.Is(stageCtx.Err(), context.DeadlineExceeded) || isTimeoutError(errDial) {
		return nil, fmt.Errorf("codex transport connection timed out after %s: %w", time.Since(started).Round(time.Millisecond), errDial)
	}
	return nil, errDial
}

func (d *codexTransportDialer) dialTLS(ctx context.Context, network, addr, serverName string) (net.Conn, error) {
	rawConn, errDial := d.dialTCP(ctx, network, addr)
	if errDial != nil {
		return nil, errDial
	}
	cfg := cloneUTLSConfig(d.tlsConfig)
	cfg.ServerName = serverName
	cfg.NextProtos = []string{http2.NextProtoTLS, "http/1.1"}
	tlsConn := tls.UClient(rawConn, cfg, tls.HelloCustom)
	if errPreset := tlsConn.ApplyPreset(codexClientHelloSpec(cfg.NextProtos)); errPreset != nil {
		_ = rawConn.Close()
		return nil, errPreset
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, d.tlsTimeout)
	defer cancel()
	if deadline, ok := handshakeCtx.Deadline(); ok {
		if errDeadline := tlsConn.SetDeadline(deadline); errDeadline != nil {
			_ = rawConn.Close()
			return nil, errDeadline
		}
		defer func() {
			_ = tlsConn.SetDeadline(time.Time{})
		}()
	}
	if errHandshake := tlsConn.HandshakeContext(handshakeCtx); errHandshake != nil {
		_ = tlsConn.Close()
		return nil, errHandshake
	}
	d.connectedOnce.Do(func() {
		if d.connected != nil {
			close(d.connected)
		}
	})
	return tlsConn, nil
}

func cloneUTLSConfig(src *tls.Config) *tls.Config {
	if src == nil {
		return &tls.Config{}
	}
	clone := *src
	if len(src.NextProtos) > 0 {
		clone.NextProtos = append([]string(nil), src.NextProtos...)
	}
	return &clone
}

func codexClientHelloSpec(nextProtos []string) *tls.ClientHelloSpec {
	extensions := []tls.TLSExtension{
		&tls.SNIExtension{},
		&tls.ExtendedMasterSecretExtension{},
		&tls.KeyShareExtension{KeyShares: []tls.KeyShare{{Group: tls.X25519MLKEM768}, {Group: tls.X25519}}},
		&tls.SupportedPointsExtension{SupportedPoints: []byte{0}},
		&tls.SessionTicketExtension{},
		&tls.SignatureAlgorithmsExtension{SupportedSignatureAlgorithms: []tls.SignatureScheme{
			tls.ECDSAWithP384AndSHA384,
			tls.ECDSAWithP256AndSHA256,
			tls.ECDSAWithP521AndSHA512,
			tls.Ed25519,
			tls.PSSWithSHA512,
			tls.PSSWithSHA384,
			tls.PSSWithSHA256,
			tls.PKCS1WithSHA512,
			tls.PKCS1WithSHA384,
			tls.PKCS1WithSHA256,
		}},
		&tls.StatusRequestExtension{},
		&tls.PSKKeyExchangeModesExtension{Modes: []uint8{tls.PskModeDHE}},
		&tls.SupportedCurvesExtension{Curves: []tls.CurveID{tls.X25519MLKEM768, tls.X25519, tls.CurveP256, tls.CurveP384}},
		&tls.SupportedVersionsExtension{Versions: []uint16{tls.VersionTLS13, tls.VersionTLS12}},
	}
	if len(nextProtos) > 0 {
		extensions = append(extensions, &tls.ALPNExtension{AlpnProtocols: append([]string(nil), nextProtos...)})
	}
	return &tls.ClientHelloSpec{
		TLSVersMin: tls.VersionTLS12,
		TLSVersMax: tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
			tls.FAKE_TLS_EMPTY_RENEGOTIATION_INFO_SCSV,
		},
		CompressionMethods: []uint8{0},
		Extensions:         tls.ShuffleChromeTLSExtensions(extensions),
	}
}

func poolKey(upstream *url.URL, proxyURL string, readIdle, ping time.Duration) string {
	parts := []string{
		strings.ToLower(strings.TrimSpace(upstream.Scheme)),
		strings.ToLower(strings.TrimSpace(upstream.Host)),
		strings.TrimSpace(proxyURL),
		readIdle.String(),
		ping.String(),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:16])
}

func normalizeResponsesEndpoint(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultCodexResponsesURL, nil
	}
	parsed, errParse := url.Parse(trimmed)
	if errParse != nil {
		return "", errParse
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("base_url must be absolute")
	}
	path := strings.TrimRight(parsed.Path, "/")
	if path == "" || path == "/" {
		path = "/backend-api/codex/responses"
	} else if strings.HasSuffix(path, "/responses") || strings.HasSuffix(path, "/responses/compact") {
		// keep
	} else if strings.HasSuffix(path, "/backend-api/codex") {
		path += "/responses"
	} else if !strings.Contains(path, "responses") {
		path += "/responses"
	}
	parsed.Path = path
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func maskProxyURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "direct") || strings.EqualFold(raw, "none") {
		return "direct"
	}
	parsed, errParse := url.Parse(raw)
	if errParse != nil {
		return "invalid-proxy"
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded")
}

func (p *codexUpstreamPool) touch() {
	if p == nil {
		return
	}
	p.lastSuccessNanos.CompareAndSwap(0, time.Now().UnixNano())
}

func (p *codexUpstreamPool) lastUsed() time.Time {
	if p == nil {
		return time.Time{}
	}
	if v := p.lastSuccessNanos.Load(); v > 0 {
		return time.Unix(0, v)
	}
	if v := p.lastErrorNanos.Load(); v > 0 {
		return time.Unix(0, v)
	}
	return time.Time{}
}

func (p *codexUpstreamPool) recordTransportError(errType string) {
	if p == nil {
		return
	}
	now := time.Now().UnixNano()
	p.lastErrorNanos.Store(now)
	p.lastErrorMu.Lock()
	p.lastError = strings.TrimSpace(errType)
	p.lastErrorMu.Unlock()
}

func (p *codexUpstreamPool) recordRequestStart() {
	if p == nil {
		return
	}
	p.requests.Add(1)
}

func (p *codexUpstreamPool) recordSuccess() {
	if p == nil {
		return
	}
	p.successes.Add(1)
	p.lastSuccessNanos.Store(time.Now().UnixNano())
}

func (p *codexUpstreamPool) recordFailure(err error) {
	if p == nil {
		return
	}
	p.failures.Add(1)
	if err != nil {
		p.lastErrorMu.Lock()
		p.lastError = strings.TrimSpace(err.Error())
		p.lastErrorMu.Unlock()
		p.lastErrorNanos.Store(time.Now().UnixNano())
	}
}

func (p *codexUpstreamPool) snapshot() poolSnapshot {
	if p == nil {
		return poolSnapshot{}
	}
	snap := poolSnapshot{
		UpstreamHost:       p.upstreamHost,
		Proxy:              p.proxyMasked,
		HTTP2Status:        "http2",
		ReadIdleTimeout:    p.readIdle.String(),
		PingTimeout:        p.pingTimeout.String(),
		Requests:           p.requests.Load(),
		Successes:          p.successes.Load(),
		Failures:           p.failures.Load(),
		ActiveStreams:      p.activeStreams.Load(),
		ConnectionsCreated: p.connectionsNew.Load(),
		ConnectionsRemoved: p.connectionsGone.Load(),
	}
	if v := p.lastSuccessNanos.Load(); v > 0 {
		snap.LastSuccess = time.Unix(0, v).Format(time.RFC3339Nano)
	}
	if v := p.lastErrorNanos.Load(); v > 0 {
		snap.LastErrorAt = time.Unix(0, v).Format(time.RFC3339Nano)
	}
	p.lastErrorMu.Lock()
	snap.LastError = p.lastError
	p.lastErrorMu.Unlock()
	return snap
}

func (p *codexUpstreamPool) closeIdleConnections() {
	if p == nil || p.transport == nil {
		return
	}
	p.transport.CloseIdleConnections()
}

func (p *codexUpstreamPool) CloseIdleConnections() {
	p.closeIdleConnections()
}

func (p *codexUpstreamPool) CloseAllConnections() {
	if p == nil {
		return
	}
	if !p.shutdown.CompareAndSwap(false, true) {
		return
	}
	p.connMu.Lock()
	conns := make([]*trackedConn, 0, len(p.connections))
	for conn := range p.connections {
		conns = append(conns, conn)
	}
	p.connections = make(map[*trackedConn]struct{})
	p.connMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	if p.transport != nil {
		p.transport.CloseIdleConnections()
	}
}

func (p *codexUpstreamPool) markConnectionCreated(conn net.Conn) net.Conn {
	if p == nil || conn == nil {
		return conn
	}
	tracked := &trackedConn{Conn: conn, pool: p}
	p.connMu.Lock()
	p.connections[tracked] = struct{}{}
	p.connMu.Unlock()
	p.connectionsNew.Add(1)
	return tracked
}

func (p *codexUpstreamPool) removeConnection(conn *trackedConn) {
	if p == nil || conn == nil {
		return
	}
	p.connMu.Lock()
	delete(p.connections, conn)
	p.connMu.Unlock()
	p.connectionsGone.Add(1)
}

func (p *codexUpstreamPool) clientDo(req *http.Request) (*http.Response, error) {
	if p == nil || p.client == nil {
		return nil, fmt.Errorf("codex upstream client is unavailable")
	}
	return p.client.Do(req)
}

type trackedConn struct {
	net.Conn
	pool *codexUpstreamPool
	once sync.Once
}

func (c *trackedConn) Close() error {
	if c == nil {
		return nil
	}
	var errClose error
	c.once.Do(func() {
		if c.pool != nil {
			c.pool.removeConnection(c)
		}
		if c.Conn != nil {
			errClose = c.Conn.Close()
		}
	})
	return errClose
}
