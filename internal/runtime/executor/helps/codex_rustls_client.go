package helps

import (
	"bufio"
	"context"
	stdtls "crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	tls "github.com/refraction-networking/utls"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
)

const (
	codexOfficialHost     = "chatgpt.com"
	codexOfficialAuthHost = "auth.openai.com"
)

// CodexProxyTimings captures pre-header network stages for management probes.
type CodexProxyTimings struct {
	mu           sync.Mutex
	ProxyConnect time.Duration
	TLSHandshake time.Duration
}

func (t *CodexProxyTimings) recordProxyConnect(duration time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.ProxyConnect = duration
	t.mu.Unlock()
}

func (t *CodexProxyTimings) recordTLSHandshake(duration time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.TLSHandshake = duration
	t.mu.Unlock()
}

func (t *CodexProxyTimings) Snapshot() (time.Duration, time.Duration) {
	if t == nil {
		return 0, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ProxyConnect, t.TLSHandshake
}

type codexHTTP2Transport struct {
	roundTripper http.RoundTripper
	connected    <-chan struct{}
	proxyHash    string
}

// codexRustlsRoundTripper applies the Codex CLI TLS profile only to official
// ChatGPT HTTPS traffic. API-key and custom-host requests use the fallback.
type codexRustlsRoundTripper struct {
	httpsHTTP2         codexHTTP2Transport
	fallback           http.RoundTripper
	enabled            bool
	inheritEnvProxy    bool
	timeouts           config.CodexProxyTimeouts
	proxyHash          string
	envProxyTransportM sync.Mutex
	envProxyTransports map[string]codexHTTP2Transport
}

type codexRustlsDialer struct {
	proxyDialer    proxyutil.ContextDialer
	httpProxyURL   *url.URL
	direct         bool
	proxyHash      string
	connectTimeout time.Duration
	tlsTimeout     time.Duration
	connected      chan struct{}
	connectedOnce  sync.Once
	timings        *CodexProxyTimings
}

func newCodexRustlsRoundTripper(proxyURL string, fallback http.RoundTripper, enabled bool, timeouts config.CodexProxyTimeouts, timings *CodexProxyTimings) (*codexRustlsRoundTripper, error) {
	if fallback == nil {
		fallback = http.DefaultTransport
	}
	httpsHTTP2, errTransport := newCodexRustlsHTTP2Transport(proxyURL, timeouts, timings)
	if errTransport != nil {
		return nil, errTransport
	}
	return &codexRustlsRoundTripper{
		httpsHTTP2:         httpsHTTP2,
		fallback:           fallback,
		enabled:            enabled,
		inheritEnvProxy:    strings.TrimSpace(proxyURL) == "",
		timeouts:           timeouts,
		proxyHash:          proxyutil.Hash(proxyURL),
		envProxyTransports: make(map[string]codexHTTP2Transport),
	}, nil
}

func newCodexRustlsDialer(proxyURL string, timeouts config.CodexProxyTimeouts, timings *CodexProxyTimings) (*codexRustlsDialer, error) {
	dialer := &codexRustlsDialer{
		direct:         true,
		proxyHash:      proxyutil.Hash(proxyURL),
		connectTimeout: timeouts.ProxyConnect,
		tlsTimeout:     timeouts.TLSHandshake,
		connected:      make(chan struct{}),
		timings:        timings,
	}
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return dialer, nil
	}

	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		return nil, errParse
	}
	switch setting.Mode {
	case proxyutil.ModeDirect, proxyutil.ModeInherit:
		return dialer, nil
	case proxyutil.ModeProxy:
		switch strings.ToLower(setting.URL.Scheme) {
		case "http", "https":
			dialer.httpProxyURL = setting.URL
			dialer.direct = false
		default:
			proxyDialer, _, errBuild := proxyutil.BuildContextDialer(proxyURL, timeouts.ProxyConnect)
			if errBuild != nil {
				return nil, errBuild
			}
			if proxyDialer != nil {
				dialer.proxyDialer = proxyDialer
				dialer.direct = false
			}
		}
	}
	return dialer, nil
}

func defaultProxyPort(proxyURL *url.URL) string {
	if proxyURL == nil {
		return ""
	}
	switch strings.ToLower(proxyURL.Scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}

func proxyNetworkAddress(proxyURL *url.URL) string {
	if proxyURL == nil {
		return ""
	}
	host := proxyURL.Hostname()
	port := proxyURL.Port()
	if port == "" {
		port = defaultProxyPort(proxyURL)
	}
	if port == "" {
		return proxyURL.Host
	}
	return net.JoinHostPort(host, port)
}

func proxyAuthorizationHeader(proxyURL *url.URL) string {
	if proxyURL == nil || proxyURL.User == nil {
		return ""
	}
	username := proxyURL.User.Username()
	password, _ := proxyURL.User.Password()
	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return "Basic " + token
}

func (d *codexRustlsDialer) dialHTTPProxyTunnel(ctx context.Context, network, targetAddr string) (net.Conn, error) {
	if d == nil || d.httpProxyURL == nil {
		return nil, fmt.Errorf("codex rustls: HTTP proxy URL is not configured")
	}
	proxyAddr := proxyNetworkAddress(d.httpProxyURL)
	if proxyAddr == "" {
		return nil, fmt.Errorf("codex rustls: HTTP proxy address is empty")
	}

	rawConn, errDial := (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext(ctx, network, proxyAddr)
	if errDial != nil {
		return nil, errDial
	}
	conn := rawConn
	if deadline, ok := ctx.Deadline(); ok {
		if errDeadline := conn.SetDeadline(deadline); errDeadline != nil {
			_ = conn.Close()
			return nil, errDeadline
		}
		defer func() {
			if errDeadline := conn.SetDeadline(time.Time{}); errDeadline != nil {
				log.Debugf("codex rustls: clear proxy handshake deadline failed: %v", errDeadline)
			}
		}()
	}

	if strings.EqualFold(d.httpProxyURL.Scheme, "https") {
		tlsConn := stdtls.Client(conn, &stdtls.Config{ServerName: d.httpProxyURL.Hostname()})
		if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
			_ = conn.Close()
			return nil, errHandshake
		}
		conn = tlsConn
	}

	var builder strings.Builder
	builder.WriteString("CONNECT ")
	builder.WriteString(targetAddr)
	builder.WriteString(" HTTP/1.1\r\nHost: ")
	builder.WriteString(targetAddr)
	builder.WriteString("\r\nProxy-Connection: Keep-Alive\r\n")
	if authHeader := proxyAuthorizationHeader(d.httpProxyURL); authHeader != "" {
		builder.WriteString("Proxy-Authorization: ")
		builder.WriteString(authHeader)
		builder.WriteString("\r\n")
	}
	builder.WriteString("\r\n")
	if _, errWrite := conn.Write([]byte(builder.String())); errWrite != nil {
		_ = conn.Close()
		return nil, errWrite
	}

	resp, errRead := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	if errRead != nil {
		_ = conn.Close()
		return nil, errRead
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		_ = conn.Close()
		if resp.StatusCode == http.StatusProxyAuthRequired {
			return nil, proxyutil.NewError(proxyutil.CodeAuthFailed, proxyutil.StageProxyConnect, true, d.proxyHash, "proxy authentication failed", nil)
		}
		return nil, proxyutil.NewError(proxyutil.CodeConnectFailed, proxyutil.StageProxyConnect, true, d.proxyHash, "proxy CONNECT request failed", nil)
	}
	return conn, nil
}

func (t *codexRustlsRoundTripper) roundTripperForEnvProxy(proxyURL *url.URL) (codexHTTP2Transport, error) {
	if proxyURL == nil {
		return t.httpsHTTP2, nil
	}
	key := proxyURL.String()
	t.envProxyTransportM.Lock()
	defer t.envProxyTransportM.Unlock()
	if roundTripper := t.envProxyTransports[key]; roundTripper.roundTripper != nil {
		return roundTripper, nil
	}
	roundTripper, errBuild := newCodexRustlsHTTP2Transport(key, t.timeouts, nil)
	if errBuild != nil {
		return codexHTTP2Transport{}, errBuild
	}
	t.envProxyTransports[key] = roundTripper
	return roundTripper, nil
}

func (t *codexRustlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil || !t.enabled || req.URL.Scheme != "https" || !isCodexOfficialHost(req.URL.Hostname()) {
		return t.fallback.RoundTrip(req)
	}
	if t.inheritEnvProxy {
		proxyURL, errProxy := http.ProxyFromEnvironment(req)
		if errProxy != nil {
			return nil, errProxy
		}
		if proxyURL != nil {
			transport, errTransport := t.roundTripperForEnvProxy(proxyURL)
			if errTransport != nil {
				return nil, errTransport
			}
			return roundTripBeforeHeaders(req, transport, t.timeouts)
		}
	}
	return roundTripBeforeHeaders(req, t.httpsHTTP2, t.timeouts)
}

type roundTripResult struct {
	response *http.Response
	err      error
}

type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (b *cancelOnCloseBody) Close() error {
	if b == nil {
		return nil
	}
	errClose := b.ReadCloser.Close()
	b.once.Do(b.cancel)
	return errClose
}

func roundTripBeforeHeaders(req *http.Request, transport codexHTTP2Transport, timeouts config.CodexProxyTimeouts) (*http.Response, error) {
	if req == nil || transport.roundTripper == nil {
		return nil, fmt.Errorf("codex rustls: transport is not configured")
	}
	requestCtx, cancelRequest := context.WithCancel(req.Context())
	requestWithCancel := req.Clone(requestCtx)
	resultCh := make(chan roundTripResult, 1)
	go func() {
		response, errRoundTrip := transport.roundTripper.RoundTrip(requestWithCancel)
		resultCh <- roundTripResult{response: response, err: errRoundTrip}
	}()

	var totalTimer *time.Timer
	var totalC <-chan time.Time
	if timeouts.FirstByte > 0 {
		totalTimer = time.NewTimer(timeouts.FirstByte)
		totalC = totalTimer.C
		defer totalTimer.Stop()
	}

	connected := transport.connected
	var headerTimer *time.Timer
	var headerC <-chan time.Time
	startHeaderTimer := func() {
		if headerTimer != nil || timeouts.ResponseHeader <= 0 {
			return
		}
		headerTimer = time.NewTimer(timeouts.ResponseHeader)
		headerC = headerTimer.C
	}
	if connected == nil {
		startHeaderTimer()
	}
	defer func() {
		if headerTimer != nil {
			headerTimer.Stop()
		}
	}()

	for {
		select {
		case result := <-resultCh:
			if result.err != nil || result.response == nil {
				cancelRequest()
				return result.response, result.err
			}
			result.response.Body = &cancelOnCloseBody{ReadCloser: result.response.Body, cancel: cancelRequest}
			return result.response, nil
		case <-connected:
			connected = nil
			startHeaderTimer()
		case <-headerC:
			cancelRequest()
			return nil, proxyutil.NewError(proxyutil.CodeUpstreamHeaderTimeout, proxyutil.StageUpstreamHeader, true, transport.proxyHash, "upstream response headers timed out", context.DeadlineExceeded)
		case <-totalC:
			cancelRequest()
			return nil, proxyutil.NewError(proxyutil.CodeUpstreamHeaderTimeout, proxyutil.StageUpstreamHeader, true, transport.proxyHash, "first-byte budget exceeded before response headers", context.DeadlineExceeded)
		case <-req.Context().Done():
			cancelRequest()
			return nil, req.Context().Err()
		}
	}
}

func isCodexOfficialHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	// ChatGPT API / websocket plane.
	if host == codexOfficialHost || strings.HasSuffix(host, "."+codexOfficialHost) {
		return true
	}
	// OAuth token exchange / refresh plane used by Codex CLI login.
	if host == codexOfficialAuthHost || strings.HasSuffix(host, "."+codexOfficialAuthHost) {
		return true
	}
	return false
}

func (d *codexRustlsDialer) dialTCP(ctx context.Context, network, addr string) (net.Conn, error) {
	if d == nil {
		return nil, proxyutil.NewError(proxyutil.CodeConfigInvalid, proxyutil.StageConfig, false, "", "Codex network dialer is not configured", nil)
	}
	stageCtx, cancelStage := context.WithTimeout(ctx, d.connectTimeout)
	defer cancelStage()
	startedAt := time.Now()
	var conn net.Conn
	var errDial error
	if d.direct {
		conn, errDial = (&net.Dialer{Timeout: d.connectTimeout, KeepAlive: 30 * time.Second}).DialContext(stageCtx, network, addr)
	} else if d.httpProxyURL != nil {
		conn, errDial = d.dialHTTPProxyTunnel(stageCtx, network, addr)
	} else if d.proxyDialer != nil {
		conn, errDial = d.proxyDialer.DialContext(stageCtx, network, addr)
	} else {
		errDial = errors.New("proxy dialer is not configured")
	}
	d.timings.recordProxyConnect(time.Since(startedAt))
	if errDial == nil {
		return conn, nil
	}
	if proxyErr, ok := proxyutil.AsError(errDial); ok {
		return nil, proxyErr
	}
	if errors.Is(stageCtx.Err(), context.DeadlineExceeded) || isTimeoutError(errDial) {
		return nil, proxyutil.NewError(proxyutil.CodeConnectTimeout, proxyutil.StageProxyConnect, true, d.proxyHash, "proxy connection timed out", errDial)
	}
	if strings.Contains(strings.ToLower(errDial.Error()), "authentication") {
		return nil, proxyutil.NewError(proxyutil.CodeAuthFailed, proxyutil.StageProxyConnect, true, d.proxyHash, "proxy authentication failed", errDial)
	}
	return nil, proxyutil.NewError(proxyutil.CodeConnectFailed, proxyutil.StageProxyConnect, true, d.proxyHash, "proxy connection failed", errDial)
}

func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (d *codexRustlsDialer) dialTLS(ctx context.Context, network, addr, serverName string, nextProtos []string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if serverName == "" {
		if host, _, errSplit := net.SplitHostPort(addr); errSplit == nil {
			serverName = host
		} else {
			serverName = addr
		}
	}

	rawConn, errDial := d.dialTCP(ctx, network, addr)
	if errDial != nil {
		return nil, errDial
	}

	tlsConn := tls.UClient(rawConn, &tls.Config{ServerName: serverName}, tls.HelloCustom)
	if errPreset := tlsConn.ApplyPreset(codexRustlsLikeClientHelloSpec(nextProtos)); errPreset != nil {
		_ = rawConn.Close()
		return nil, proxyutil.NewError(proxyutil.CodeTLSFailed, proxyutil.StageTLSHandshake, true, d.proxyHash, "TLS client profile setup failed", errPreset)
	}
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, d.tlsTimeout)
	defer cancelHandshake()
	if deadline, ok := handshakeCtx.Deadline(); ok {
		if errDeadline := tlsConn.SetDeadline(deadline); errDeadline != nil {
			_ = rawConn.Close()
			return nil, errDeadline
		}
		defer func() {
			if errDeadline := tlsConn.SetDeadline(time.Time{}); errDeadline != nil {
				log.Debugf("codex rustls: clear TLS handshake deadline failed: %v", errDeadline)
			}
		}()
	}
	startedAt := time.Now()
	if errHandshake := tlsConn.HandshakeContext(handshakeCtx); errHandshake != nil {
		d.timings.recordTLSHandshake(time.Since(startedAt))
		_ = tlsConn.Close()
		if errors.Is(handshakeCtx.Err(), context.DeadlineExceeded) || isTimeoutError(errHandshake) {
			return nil, proxyutil.NewError(proxyutil.CodeTLSTimeout, proxyutil.StageTLSHandshake, true, d.proxyHash, "TLS handshake timed out", errHandshake)
		}
		return nil, proxyutil.NewError(proxyutil.CodeTLSFailed, proxyutil.StageTLSHandshake, true, d.proxyHash, "TLS handshake failed", errHandshake)
	}
	d.timings.recordTLSHandshake(time.Since(startedAt))
	d.connectedOnce.Do(func() { close(d.connected) })
	return tlsConn, nil
}

func newCodexRustlsHTTP2Transport(proxyURL string, timeouts config.CodexProxyTimeouts, timings *CodexProxyTimings) (codexHTTP2Transport, error) {
	dialer, errDialer := newCodexRustlsDialer(proxyURL, timeouts, timings)
	if errDialer != nil {
		return codexHTTP2Transport{}, errDialer
	}
	transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *stdtls.Config) (net.Conn, error) {
			serverName := ""
			if cfg != nil {
				serverName = cfg.ServerName
			}
			return dialer.dialTLS(ctx, network, addr, serverName, []string{http2.NextProtoTLS, "http/1.1"})
		},
	}
	return codexHTTP2Transport{roundTripper: transport, connected: dialer.connected, proxyHash: dialer.proxyHash}, nil
}

func codexRustlsLikeClientHelloSpec(nextProtos []string) *tls.ClientHelloSpec {
	extensions := []tls.TLSExtension{
		&tls.SNIExtension{},
		&tls.ExtendedMasterSecretExtension{},
		&tls.KeyShareExtension{KeyShares: []tls.KeyShare{
			{Group: tls.X25519MLKEM768},
			{Group: tls.X25519},
		}},
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
		&tls.SupportedCurvesExtension{Curves: []tls.CurveID{
			tls.X25519MLKEM768,
			tls.X25519,
			tls.CurveP256,
			tls.CurveP384,
		}},
		&tls.SupportedVersionsExtension{Versions: []uint16{
			tls.VersionTLS13,
			tls.VersionTLS12,
		}},
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

func codexProxyURL(cfg *config.Config, auth *cliproxyauth.Auth) string {
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.ProxyURL); proxyURL != "" {
			return proxyURL
		}
	}
	if cfg != nil {
		return strings.TrimSpace(cfg.ProxyURL)
	}
	return ""
}

func codexAuthUsesAPIKey(auth *cliproxyauth.Auth) bool {
	return auth != nil && strings.TrimSpace(auth.Attributes["api_key"]) != ""
}

// NewCodexRustlsHTTPClient creates a proxy-aware HTTP/2 client matching the
// ClientHello aligned to local Codex CLI 0.146.0 (chatgpt.com OAuth capture).
func NewCodexRustlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) (*http.Client, error) {
	return newCodexRustlsHTTPClient(ctx, cfg, auth, nil)
}

func newCodexRustlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timings *CodexProxyTimings) (*http.Client, error) {
	proxyURL := codexProxyURL(cfg, auth)
	setting, errProxy := validateCodexProxySetting(proxyURL, cfg != nil && cfg.CodexProxyRequired)
	if errProxy != nil {
		return nil, errProxy
	}
	timeouts := cfg.CodexProxyTimeouts()
	fallback, errFallback := buildCodexFallbackTransport(proxyURL, timeouts)
	if errFallback != nil {
		return nil, errFallback
	}
	enabled := !codexAuthUsesAPIKey(auth)
	if setting.Mode == proxyutil.ModeInherit && ctx != nil {
		if roundTripper, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && roundTripper != nil {
			fallback = roundTripper
			enabled = false
		}
	}
	roundTripper, errRoundTripper := newCodexRustlsRoundTripper(proxyURL, fallback, enabled, timeouts, timings)
	if errRoundTripper != nil {
		return nil, errRoundTripper
	}
	return &http.Client{Transport: roundTripper}, nil
}

func validateCodexProxySetting(proxyURL string, required bool) (proxyutil.Setting, error) {
	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		return setting, errParse
	}
	if required && setting.Mode != proxyutil.ModeProxy {
		return setting, proxyutil.NewError(proxyutil.CodeRequired, proxyutil.StageConfig, false, "", "Codex proxy is required; direct or empty proxy settings are not allowed", nil)
	}
	return setting, nil
}

func buildCodexFallbackTransport(proxyURL string, timeouts config.CodexProxyTimeouts) (http.RoundTripper, error) {
	transport, _, errBuild := proxyutil.BuildHTTPTransportWithConnectTimeout(proxyURL, timeouts.ProxyConnect)
	if errBuild != nil {
		return nil, errBuild
	}
	if transport == nil {
		if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok && defaultTransport != nil {
			transport = defaultTransport.Clone()
		} else {
			transport = &http.Transport{}
		}
		transport.DialContext = (&net.Dialer{Timeout: timeouts.ProxyConnect, KeepAlive: 30 * time.Second}).DialContext
	}
	transport.TLSHandshakeTimeout = timeouts.TLSHandshake
	transport.ResponseHeaderTimeout = timeouts.ResponseHeader
	return transport, nil
}

// NewCodexRustlsNetDialTLSContext returns a TLS dialer for the official Codex
// websocket. The websocket caller must leave gorilla's Proxy hook disabled.
func NewCodexRustlsNetDialTLSContext(cfg *config.Config, auth *cliproxyauth.Auth) func(context.Context, string, string) (net.Conn, error) {
	proxyURL := codexProxyURL(cfg, auth)
	timeouts := cfg.CodexProxyTimeouts()
	_, configErr := validateCodexProxySetting(proxyURL, cfg != nil && cfg.CodexProxyRequired)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if configErr != nil {
			return nil, configErr
		}
		resolvedProxyURL := proxyURL
		if strings.TrimSpace(resolvedProxyURL) == "" {
			proxyRequest := &http.Request{URL: &url.URL{Scheme: "https", Host: addr}}
			envProxyURL, errProxy := http.ProxyFromEnvironment(proxyRequest)
			if errProxy != nil {
				return nil, fmt.Errorf("codex rustls: resolve websocket proxy: %w", errProxy)
			}
			if envProxyURL != nil {
				resolvedProxyURL = envProxyURL.String()
			}
		}
		dialer, errDialer := newCodexRustlsDialer(resolvedProxyURL, timeouts, nil)
		if errDialer != nil {
			return nil, errDialer
		}
		return dialer.dialTLS(ctx, network, addr, "", nil)
	}
}

// NewCodexOAuthHTTPClient builds the HTTP client used by Codex login/refresh.
// It applies the same rustls-like ClientHello profile as Codex CLI OAuth traffic
// for chatgpt.com and auth.openai.com, with proxy-aware fallback for other hosts.
// proxyURL overrides cfg.ProxyURL when non-empty (including "direct").
func NewCodexOAuthHTTPClient(cfg *config.Config, proxyURL string) (*http.Client, error) {
	effective := strings.TrimSpace(proxyURL)
	var full *config.Config
	if cfg != nil {
		clone := *cfg
		if effective != "" {
			clone.ProxyURL = effective
		}
		full = &clone
	} else if effective != "" {
		full = &config.Config{SDKConfig: config.SDKConfig{ProxyURL: effective}}
	}
	// OAuth is always credentialed as ChatGPT OAuth (not API key), so enable fingerprinting.
	auth := &cliproxyauth.Auth{Provider: "codex", ProxyURL: effective}
	return NewCodexRustlsHTTPClient(context.Background(), full, auth)
}

// IsCodexFingerprintTransport reports whether rt is the Codex rustls-like transport.
func IsCodexFingerprintTransport(rt http.RoundTripper) bool {
	_, ok := rt.(*codexRustlsRoundTripper)
	return ok
}

// CodexFingerprintTransportInfo exposes a few test/diagnostic fields for the
// Codex rustls-like transport without exporting the concrete type.
type CodexFingerprintTransportInfo struct {
	Enabled         bool
	InheritEnvProxy bool
	ProxyHash       string
}

// CodexFingerprintTransportInfoOf returns diagnostics when rt is a Codex
// fingerprint transport. ok is false for any other RoundTripper.
func CodexFingerprintTransportInfoOf(rt http.RoundTripper) (info CodexFingerprintTransportInfo, ok bool) {
	typed, ok := rt.(*codexRustlsRoundTripper)
	if !ok || typed == nil {
		return CodexFingerprintTransportInfo{}, false
	}
	return CodexFingerprintTransportInfo{
		Enabled:         typed.enabled,
		InheritEnvProxy: typed.inheritEnvProxy,
		ProxyHash:       typed.proxyHash,
	}, true
}
