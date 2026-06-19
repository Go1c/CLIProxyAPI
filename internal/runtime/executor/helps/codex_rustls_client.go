package helps

import (
	"bufio"
	"context"
	stdtls "crypto/tls"
	"encoding/base64"
	"fmt"
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
	"golang.org/x/net/proxy"
)

// codexRustlsRoundTripper is used only for Codex upstream traffic. It forces a
// rustls-like ClientHello while keeping the HTTP layer on HTTP/1.1, matching
// the observed Codex CLI wire shape.
type codexRustlsRoundTripper struct {
	httpsHTTP1          http.RoundTripper
	fallback            http.RoundTripper
	fallbackHTTPS       bool
	inheritEnvProxy     bool
	envProxyTransportMu sync.Mutex
	envProxyTransports  map[string]http.RoundTripper
}

type codexRustlsDialer struct {
	proxyDialer  proxy.Dialer
	httpProxyURL *url.URL
	direct       bool
}

func newCodexRustlsRoundTripper(proxyURL string, fallback http.RoundTripper, fallbackHTTPS bool) *codexRustlsRoundTripper {
	return &codexRustlsRoundTripper{
		httpsHTTP1:         newCodexRustlsHTTP1Transport(proxyURL),
		fallback:           fallback,
		fallbackHTTPS:      fallbackHTTPS,
		inheritEnvProxy:    strings.TrimSpace(proxyURL) == "" && !fallbackHTTPS,
		envProxyTransports: make(map[string]http.RoundTripper),
	}
}

func newCodexRustlsDialer(proxyURL string) *codexRustlsDialer {
	d := &codexRustlsDialer{direct: true}
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return d
	}

	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		log.Errorf("codex rustls: failed to parse proxy config: %v", errParse)
		return d
	}
	switch setting.Mode {
	case proxyutil.ModeDirect, proxyutil.ModeInherit:
		return d
	case proxyutil.ModeProxy:
		switch strings.ToLower(setting.URL.Scheme) {
		case "http", "https":
			d.httpProxyURL = setting.URL
			d.direct = false
		default:
			proxyDialer, _, errBuild := proxyutil.BuildDialer(proxyURL)
			if errBuild != nil {
				log.Errorf("codex rustls: failed to configure proxy dialer: %v", errBuild)
				return d
			}
			if proxyDialer != nil {
				d.proxyDialer = proxyDialer
				d.direct = false
			}
		}
	}
	return d
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

	rawConn, errDial := (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, proxyAddr)
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
			_ = conn.SetDeadline(time.Time{})
		}()
	}

	if strings.EqualFold(d.httpProxyURL.Scheme, "https") {
		tlsConn := stdtls.Client(conn, &stdtls.Config{ServerName: d.httpProxyURL.Hostname()})
		if errHandshake := tlsConn.Handshake(); errHandshake != nil {
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
		_ = conn.Close()
		return nil, fmt.Errorf("codex rustls: HTTP proxy CONNECT failed with status %s", resp.Status)
	}
	return conn, nil
}

func (t *codexRustlsRoundTripper) roundTripperForEnvProxy(proxyURL *url.URL) http.RoundTripper {
	if proxyURL == nil {
		return t.httpsHTTP1
	}
	key := proxyURL.String()
	t.envProxyTransportMu.Lock()
	defer t.envProxyTransportMu.Unlock()
	if rt := t.envProxyTransports[key]; rt != nil {
		return rt
	}
	rt := newCodexRustlsHTTP1Transport(key)
	t.envProxyTransports[key] = rt
	return rt
}

func (t *codexRustlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil || req.URL.Scheme != "https" {
		return t.fallback.RoundTrip(req)
	}
	if t.fallbackHTTPS {
		return t.fallback.RoundTrip(req)
	}
	if t.inheritEnvProxy {
		proxyURL, errProxy := http.ProxyFromEnvironment(req)
		if errProxy != nil {
			return nil, errProxy
		}
		if proxyURL != nil {
			return t.roundTripperForEnvProxy(proxyURL).RoundTrip(req)
		}
	}

	return t.httpsHTTP1.RoundTrip(req)
}

func (d *codexRustlsDialer) dialTCP(ctx context.Context, network, addr string) (net.Conn, error) {
	if d == nil || d.direct {
		return (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, addr)
	}
	if d.httpProxyURL != nil {
		return d.dialHTTPProxyTunnel(ctx, network, addr)
	}
	if d.proxyDialer != nil {
		return d.proxyDialer.Dial(network, addr)
	}
	return (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, addr)
}

func (d *codexRustlsDialer) dialTLS(ctx context.Context, network, addr, serverName string) (net.Conn, error) {
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

	rawConn, err := d.dialTCP(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		ServerName: serverName,
	}
	tlsConn := tls.UClient(rawConn, tlsConfig, tls.HelloCustom)
	if errPreset := tlsConn.ApplyPreset(codexRustlsLikeClientHelloSpec()); errPreset != nil {
		_ = rawConn.Close()
		return nil, errPreset
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = tlsConn.SetDeadline(deadline)
		defer func() {
			_ = tlsConn.SetDeadline(time.Time{})
		}()
	}
	if errHandshake := tlsConn.Handshake(); errHandshake != nil {
		_ = tlsConn.Close()
		return nil, errHandshake
	}
	return tlsConn, nil
}

func newCodexRustlsHTTP1Transport(proxyURL string) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := newCodexRustlsDialer(proxyURL)
	transport.Proxy = nil
	transport.ForceAttemptHTTP2 = false
	transport.TLSNextProto = make(map[string]func(authority string, c *stdtls.Conn) http.RoundTripper)
	transport.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		serverName := ""
		if host, _, errSplit := net.SplitHostPort(addr); errSplit == nil {
			serverName = host
		}
		return dialer.dialTLS(ctx, network, addr, serverName)
	}
	return transport
}

func codexRustlsLikeClientHelloSpec() *tls.ClientHelloSpec {
	return &tls.ClientHelloSpec{
		TLSVersMin: tls.VersionTLS12,
		TLSVersMax: tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			0x009f,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
			0xccaa,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			0x009e,
			0xc024,
			0xc028,
			0x006b,
			0xc023,
			0xc027,
			0x0067,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			0x0039,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			0x0033,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			0x003d,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		},
		CompressionMethods: []uint8{0},
		Extensions: []tls.TLSExtension{
			&tls.RenegotiationInfoExtension{Renegotiation: tls.RenegotiateNever},
			&tls.SNIExtension{},
			&tls.SupportedPointsExtension{SupportedPoints: []byte{0, 1, 2}},
			&tls.SupportedCurvesExtension{Curves: []tls.CurveID{
				tls.X25519MLKEM768,
				tls.X25519,
				tls.CurveP256,
				tls.CurveID(0x001e),
				tls.CurveP384,
				tls.CurveP521,
				tls.FakeCurveFFDHE2048,
				tls.FakeCurveFFDHE3072,
			}},
			&tls.SessionTicketExtension{},
			&tls.GenericExtension{Id: 0x0016},
			&tls.ExtendedMasterSecretExtension{},
			&tls.SignatureAlgorithmsExtension{SupportedSignatureAlgorithms: []tls.SignatureScheme{
				tls.SignatureScheme(0x0905),
				tls.SignatureScheme(0x0906),
				tls.SignatureScheme(0x0904),
				tls.ECDSAWithP256AndSHA256,
				tls.ECDSAWithP384AndSHA384,
				tls.ECDSAWithP521AndSHA512,
				tls.Ed25519,
				tls.SignatureScheme(0x0808),
				tls.SignatureScheme(0x081a),
				tls.SignatureScheme(0x081b),
				tls.SignatureScheme(0x081c),
				tls.SignatureScheme(0x0809),
				tls.SignatureScheme(0x080a),
				tls.SignatureScheme(0x080b),
				tls.PSSWithSHA256,
				tls.PSSWithSHA384,
				tls.PSSWithSHA512,
				tls.PKCS1WithSHA256,
				tls.PKCS1WithSHA384,
				tls.PKCS1WithSHA512,
				tls.SignatureScheme(0x0303),
				tls.SignatureScheme(0x0301),
				tls.SignatureScheme(0x0302),
				tls.SignatureScheme(0x0402),
				tls.SignatureScheme(0x0502),
				tls.SignatureScheme(0x0602),
			}},
			&tls.SupportedVersionsExtension{Versions: []uint16{
				tls.VersionTLS13,
				tls.VersionTLS12,
			}},
			&tls.PSKKeyExchangeModesExtension{Modes: []uint8{1}},
			&tls.KeyShareExtension{KeyShares: []tls.KeyShare{
				{Group: tls.X25519MLKEM768},
				{Group: tls.X25519},
			}},
		},
	}
}

// NewCodexRustlsHTTPClient creates a proxy-aware HTTP client for Codex that
// forces a rustls-like TLS fingerprint for HTTPS upstreams.
func NewCodexRustlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	var proxyURL string
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}

	var fallback http.RoundTripper = http.DefaultTransport
	fallbackHTTPS := false
	if proxyURL != "" {
		if transport := buildProxyTransport(proxyURL); transport != nil {
			fallback = transport
		}
	} else if ctx != nil {
		if rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && rt != nil {
			fallback = rt
			fallbackHTTPS = true
		}
	}

	client := &http.Client{
		Transport: newCodexRustlsRoundTripper(proxyURL, fallback, fallbackHTTPS),
	}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}

// NewCodexRustlsNetDialTLSContext returns a TLS dialer suitable for gorilla
// websocket Dialer.NetDialTLSContext. Proxy handling is performed inside this
// function so gorilla's Proxy hook must remain nil when it is used.
func NewCodexRustlsNetDialTLSContext(cfg *config.Config, auth *cliproxyauth.Auth) func(context.Context, string, string) (net.Conn, error) {
	var proxyURL string
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}
	dialer := newCodexRustlsDialer(proxyURL)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.dialTLS(ctx, network, addr, "")
	}
}
