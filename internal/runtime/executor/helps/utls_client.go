package helps

import (
	"context"
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
	"golang.org/x/net/proxy"
)

// utlsRoundTripper implements http.RoundTripper using utls with Chrome fingerprint
// to bypass Cloudflare's TLS fingerprinting on Anthropic domains.
type utlsRoundTripper struct {
	mu          sync.Mutex
	connections map[string]*http2.ClientConn
	pending     map[string]*sync.Cond
	dialer      proxy.Dialer
}

func newUtlsRoundTripper(proxyURL string) *utlsRoundTripper {
	var dialer proxy.Dialer = proxy.Direct
	if proxyURL != "" {
		proxyDialer, mode, errBuild := proxyutil.BuildDialer(proxyURL)
		if errBuild != nil {
			log.Errorf("utls: failed to configure proxy dialer for %q: %v", proxyutil.Redact(proxyURL), errBuild)
		} else if mode != proxyutil.ModeInherit && proxyDialer != nil {
			dialer = proxyDialer
		}
	}
	return &utlsRoundTripper{
		connections: make(map[string]*http2.ClientConn),
		pending:     make(map[string]*sync.Cond),
		dialer:      dialer,
	}
}

func (t *utlsRoundTripper) getOrCreateConnection(host, addr string) (*http2.ClientConn, error) {
	t.mu.Lock()

	if h2Conn, ok := t.connections[host]; ok && h2Conn.CanTakeNewRequest() {
		t.mu.Unlock()
		return h2Conn, nil
	}

	if cond, ok := t.pending[host]; ok {
		cond.Wait()
		if h2Conn, ok := t.connections[host]; ok && h2Conn.CanTakeNewRequest() {
			t.mu.Unlock()
			return h2Conn, nil
		}
	}

	cond := sync.NewCond(&t.mu)
	t.pending[host] = cond
	t.mu.Unlock()

	h2Conn, err := t.createConnection(host, addr)

	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.pending, host)
	cond.Broadcast()

	if err != nil {
		return nil, err
	}

	t.connections[host] = h2Conn
	return h2Conn, nil
}

func (t *utlsRoundTripper) createConnection(host, addr string) (*http2.ClientConn, error) {
	conn, err := t.dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{ServerName: host}
	tlsConn := tls.UClient(conn, tlsConfig, tls.HelloChrome_Auto)

	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		return nil, err
	}

	tr := &http2.Transport{}
	h2Conn, err := tr.NewClientConn(tlsConn)
	if err != nil {
		tlsConn.Close()
		return nil, err
	}

	return h2Conn, nil
}

func (t *utlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	hostname := req.URL.Hostname()
	port := req.URL.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(hostname, port)

	h2Conn, err := t.getOrCreateConnection(hostname, addr)
	if err != nil {
		return nil, err
	}

	resp, err := h2Conn.RoundTrip(req)
	if err != nil {
		t.mu.Lock()
		if cached, ok := t.connections[hostname]; ok && cached == h2Conn {
			delete(t.connections, hostname)
		}
		t.mu.Unlock()
		return nil, err
	}

	return resp, nil
}

func newCodexRustlsClientHelloSpec() *tls.ClientHelloSpec {
	return &tls.ClientHelloSpec{
		TLSVersMin: tls.VersionTLS12,
		TLSVersMax: tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.FAKE_TLS_EMPTY_RENEGOTIATION_INFO_SCSV,
		},
		CompressionMethods: []uint8{0},
		Extensions: []tls.TLSExtension{
			&tls.SNIExtension{},
			&tls.ExtendedMasterSecretExtension{},
			&tls.KeyShareExtension{KeyShares: []tls.KeyShare{
				{Group: tls.X25519MLKEM768},
				{Group: tls.X25519},
			}},
			&tls.SupportedPointsExtension{SupportedPoints: []uint8{0}},
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
		},
	}
}

func configuredProxyURL(cfg *config.Config, auth *cliproxyauth.Auth) string {
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

func proxyURLFromEnvironment(addr string, tlsEnabled bool) string {
	scheme := "http"
	if tlsEnabled {
		scheme = "https"
	}
	proxyURL, err := http.ProxyFromEnvironment(&http.Request{
		URL: &url.URL{Scheme: scheme, Host: addr},
	})
	if err != nil {
		log.Errorf("utls: resolve environment proxy failed: %v", err)
		return ""
	}
	if proxyURL == nil {
		return ""
	}
	return proxyURL.String()
}

func proxyAwareDial(ctx context.Context, proxyURL string, tlsEnabled bool, network string, addr string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		proxyURL = proxyURLFromEnvironment(addr, tlsEnabled)
	}

	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		log.Errorf("utls: %v", errParse)
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}

	switch setting.Mode {
	case proxyutil.ModeDirect, proxyutil.ModeInherit:
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	case proxyutil.ModeProxy:
		dialer, _, errBuild := proxyutil.BuildDialer(proxyURL)
		if errBuild != nil {
			return nil, errBuild
		}
		return dialer.Dial(network, addr)
	default:
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
}

func serverNameFromAddr(addr string) string {
	host, _, errSplit := net.SplitHostPort(addr)
	if errSplit != nil {
		return strings.Trim(addr, "[]")
	}
	return strings.Trim(host, "[]")
}

func applyContextDeadline(ctx context.Context, conn net.Conn, label string) func() {
	if ctx == nil {
		return func() {}
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return func() {}
	}
	if errDeadline := conn.SetDeadline(deadline); errDeadline != nil {
		log.Debugf("utls: set %s TLS handshake deadline failed: %v", label, errDeadline)
	}
	return func() {
		if errDeadline := conn.SetDeadline(time.Time{}); errDeadline != nil {
			log.Debugf("utls: clear %s TLS handshake deadline failed: %v", label, errDeadline)
		}
	}
}

func handshakeCodexRustlsTLS(ctx context.Context, conn net.Conn, serverName string) (net.Conn, error) {
	tlsConn := tls.UClient(conn, &tls.Config{ServerName: serverName}, tls.HelloCustom)
	if errPreset := tlsConn.ApplyPreset(newCodexRustlsClientHelloSpec()); errPreset != nil {
		if errClose := conn.Close(); errClose != nil {
			log.Debugf("utls: close failed Codex TLS connection after preset error: %v", errClose)
		}
		return nil, errPreset
	}
	clearDeadline := applyContextDeadline(ctx, conn, "Codex")
	defer clearDeadline()
	if errHandshake := tlsConn.Handshake(); errHandshake != nil {
		if errClose := conn.Close(); errClose != nil {
			log.Debugf("utls: close failed Codex TLS connection: %v", errClose)
		}
		return nil, errHandshake
	}
	return tlsConn, nil
}

// NewProxyAwareNetDialContext returns a context-aware plain TCP dial function
// that honors auth/config proxy settings, including connection-layer proxies.
func NewProxyAwareNetDialContext(cfg *config.Config, auth *cliproxyauth.Auth) func(context.Context, string, string) (net.Conn, error) {
	proxyURL := configuredProxyURL(cfg, auth)
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		return proxyAwareDial(ctx, proxyURL, false, network, addr)
	}
}

// NewCodexUtlsNetDialTLSContext returns a gorilla/websocket-compatible TLS dial
// function using the rustls-style ClientHello observed from the local Codex TUI.
func NewCodexUtlsNetDialTLSContext(cfg *config.Config, auth *cliproxyauth.Auth) func(context.Context, string, string) (net.Conn, error) {
	proxyURL := configuredProxyURL(cfg, auth)
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		conn, errDial := proxyAwareDial(ctx, proxyURL, true, network, addr)
		if errDial != nil {
			return nil, errDial
		}
		return handshakeCodexRustlsTLS(ctx, conn, serverNameFromAddr(addr))
	}
}

// NewUtlsNetDialTLSContext returns a gorilla/websocket-compatible TLS dial
// function that performs the upstream TLS handshake with a Chrome-like uTLS
// fingerprint after the configured proxy connection is established.
func NewUtlsNetDialTLSContext(cfg *config.Config, auth *cliproxyauth.Auth) func(context.Context, string, string) (net.Conn, error) {
	proxyURL := configuredProxyURL(cfg, auth)
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		conn, errDial := proxyAwareDial(ctx, proxyURL, true, network, addr)
		if errDial != nil {
			return nil, errDial
		}

		tlsConn := tls.UClient(conn, &tls.Config{ServerName: serverNameFromAddr(addr)}, tls.HelloChrome_Auto)
		clearDeadline := applyContextDeadline(ctx, conn, "websocket")
		defer clearDeadline()
		if errHandshake := tlsConn.Handshake(); errHandshake != nil {
			if errClose := conn.Close(); errClose != nil {
				log.Debugf("utls: close failed websocket TLS connection: %v", errClose)
			}
			return nil, errHandshake
		}
		return tlsConn, nil
	}
}

// utlsProtectedHosts contains the hosts that should use utls Chrome TLS fingerprint
// to bypass Cloudflare's TLS fingerprinting.
var utlsProtectedHosts = map[string]struct{}{
	"api.anthropic.com": {},
	"chatgpt.com":       {},
}

var codexUtlsProtectedHosts = map[string]struct{}{
	"chatgpt.com": {},
}

// hostFallbackRoundTripper uses a protected transport for selected HTTPS hosts
// and falls back to the standard transport for all other requests.
type hostFallbackRoundTripper struct {
	protectedHosts map[string]struct{}
	protected      http.RoundTripper
	fallback       http.RoundTripper
}

func (f *hostFallbackRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		if _, ok := f.protectedHosts[strings.ToLower(req.URL.Hostname())]; ok {
			return f.protected.RoundTrip(req)
		}
	}
	return f.fallback.RoundTrip(req)
}

func codexUtlsRoundTripper(cfg *config.Config, auth *cliproxyauth.Auth) http.RoundTripper {
	transport := &http.Transport{
		Proxy:             nil,
		ForceAttemptHTTP2: false,
		DialContext:       NewProxyAwareNetDialContext(cfg, auth),
		DialTLSContext:    NewCodexUtlsNetDialTLSContext(cfg, auth),
	}
	return transport
}

// NewCodexUtlsHTTPClient creates an HTTP client using the rustls-style TLS
// fingerprint observed from the local Codex TUI for Codex's official host.
func NewCodexUtlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	proxyURL := configuredProxyURL(cfg, auth)

	var ctxRoundTripper http.RoundTripper
	if ctx != nil {
		ctxRoundTripper, _ = ctx.Value("cliproxy.roundtripper").(http.RoundTripper)
	}

	var codexRT http.RoundTripper = codexUtlsRoundTripper(cfg, auth)
	var standardTransport http.RoundTripper = http.DefaultTransport
	if proxyURL != "" {
		if transport := buildProxyTransport(proxyURL); transport != nil {
			standardTransport = transport
		}
	} else if ctxRoundTripper != nil {
		codexRT = ctxRoundTripper
		standardTransport = ctxRoundTripper
	}

	client := &http.Client{
		Transport: &hostFallbackRoundTripper{
			protectedHosts: codexUtlsProtectedHosts,
			protected:      codexRT,
			fallback:       standardTransport,
		},
	}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}

// NewUtlsHTTPClient creates an HTTP client using utls Chrome TLS fingerprint.
// Use this for provider requests that need a Chrome-like TLS fingerprint.
// Falls back to standard transport for non-HTTPS requests.
func NewUtlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	proxyURL := configuredProxyURL(cfg, auth)

	var ctxRoundTripper http.RoundTripper
	if ctx != nil {
		ctxRoundTripper, _ = ctx.Value("cliproxy.roundtripper").(http.RoundTripper)
	}

	var utlsRT http.RoundTripper = newUtlsRoundTripper(proxyURL)
	var standardTransport http.RoundTripper = http.DefaultTransport
	if proxyURL != "" {
		if transport := buildProxyTransport(proxyURL); transport != nil {
			standardTransport = transport
		}
	} else if ctxRoundTripper != nil {
		utlsRT = ctxRoundTripper
		standardTransport = ctxRoundTripper
	}

	client := &http.Client{
		Transport: &hostFallbackRoundTripper{
			protectedHosts: utlsProtectedHosts,
			protected:      utlsRT,
			fallback:       standardTransport,
		},
	}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}
