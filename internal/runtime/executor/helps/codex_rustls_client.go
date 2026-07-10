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
	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
)

const codexOfficialHost = "chatgpt.com"

// codexRustlsRoundTripper applies the Codex CLI TLS profile only to official
// ChatGPT HTTPS traffic. API-key and custom-host requests use the fallback.
type codexRustlsRoundTripper struct {
	httpsHTTP2         http.RoundTripper
	fallback           http.RoundTripper
	enabled            bool
	inheritEnvProxy    bool
	envProxyTransportM sync.Mutex
	envProxyTransports map[string]http.RoundTripper
}

type codexRustlsDialer struct {
	proxyDialer  proxy.Dialer
	httpProxyURL *url.URL
	direct       bool
}

func newCodexRustlsRoundTripper(proxyURL string, fallback http.RoundTripper, enabled bool) *codexRustlsRoundTripper {
	if fallback == nil {
		fallback = http.DefaultTransport
	}
	return &codexRustlsRoundTripper{
		httpsHTTP2:         newCodexRustlsHTTP2Transport(proxyURL),
		fallback:           fallback,
		enabled:            enabled,
		inheritEnvProxy:    strings.TrimSpace(proxyURL) == "",
		envProxyTransports: make(map[string]http.RoundTripper),
	}
}

func newCodexRustlsDialer(proxyURL string) *codexRustlsDialer {
	dialer := &codexRustlsDialer{direct: true}
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return dialer
	}

	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		log.Errorf("codex rustls: failed to parse proxy config %q: %v", proxyutil.Redact(proxyURL), errParse)
		return dialer
	}
	switch setting.Mode {
	case proxyutil.ModeDirect, proxyutil.ModeInherit:
		return dialer
	case proxyutil.ModeProxy:
		switch strings.ToLower(setting.URL.Scheme) {
		case "http", "https":
			dialer.httpProxyURL = setting.URL
			dialer.direct = false
		default:
			proxyDialer, _, errBuild := proxyutil.BuildDialer(proxyURL)
			if errBuild != nil {
				log.Errorf("codex rustls: failed to configure proxy dialer: %v", errBuild)
				return dialer
			}
			if proxyDialer != nil {
				dialer.proxyDialer = proxyDialer
				dialer.direct = false
			}
		}
	}
	return dialer
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
		return nil, fmt.Errorf("codex rustls: HTTP proxy CONNECT failed with status %s", resp.Status)
	}
	return conn, nil
}

func (t *codexRustlsRoundTripper) roundTripperForEnvProxy(proxyURL *url.URL) http.RoundTripper {
	if proxyURL == nil {
		return t.httpsHTTP2
	}
	key := proxyURL.String()
	t.envProxyTransportM.Lock()
	defer t.envProxyTransportM.Unlock()
	if roundTripper := t.envProxyTransports[key]; roundTripper != nil {
		return roundTripper
	}
	roundTripper := newCodexRustlsHTTP2Transport(key)
	t.envProxyTransports[key] = roundTripper
	return roundTripper
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
			return t.roundTripperForEnvProxy(proxyURL).RoundTrip(req)
		}
	}
	return t.httpsHTTP2.RoundTrip(req)
}

func isCodexOfficialHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == codexOfficialHost || strings.HasSuffix(host, "."+codexOfficialHost)
}

func (d *codexRustlsDialer) dialTCP(ctx context.Context, network, addr string) (net.Conn, error) {
	if d == nil || d.direct {
		return (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext(ctx, network, addr)
	}
	if d.httpProxyURL != nil {
		return d.dialHTTPProxyTunnel(ctx, network, addr)
	}
	if d.proxyDialer != nil {
		return d.proxyDialer.Dial(network, addr)
	}
	return (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext(ctx, network, addr)
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
		return nil, errPreset
	}
	if deadline, ok := ctx.Deadline(); ok {
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
	if errHandshake := tlsConn.Handshake(); errHandshake != nil {
		_ = tlsConn.Close()
		return nil, errHandshake
	}
	return tlsConn, nil
}

func newCodexRustlsHTTP2Transport(proxyURL string) *http2.Transport {
	dialer := newCodexRustlsDialer(proxyURL)
	return &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *stdtls.Config) (net.Conn, error) {
			serverName := ""
			if cfg != nil {
				serverName = cfg.ServerName
			}
			return dialer.dialTLS(ctx, network, addr, serverName, []string{http2.NextProtoTLS, "http/1.1"})
		},
	}
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
// ClientHello captured from Codex CLI 0.144.1 for official OAuth traffic.
func NewCodexRustlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) *http.Client {
	proxyURL := codexProxyURL(cfg, auth)
	fallback := http.RoundTripper(http.DefaultTransport)
	enabled := !codexAuthUsesAPIKey(auth)
	if proxyURL != "" {
		if transport := buildProxyTransport(proxyURL); transport != nil {
			fallback = transport
		}
	} else if ctx != nil {
		if roundTripper, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && roundTripper != nil {
			fallback = roundTripper
			enabled = false
		}
	}

	return &http.Client{Transport: newCodexRustlsRoundTripper(proxyURL, fallback, enabled)}
}

// NewCodexRustlsNetDialTLSContext returns a TLS dialer for the official Codex
// websocket. The websocket caller must leave gorilla's Proxy hook disabled.
func NewCodexRustlsNetDialTLSContext(cfg *config.Config, auth *cliproxyauth.Auth) func(context.Context, string, string) (net.Conn, error) {
	proxyURL := codexProxyURL(cfg, auth)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
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
		dialer := newCodexRustlsDialer(resolvedProxyURL)
		return dialer.dialTLS(ctx, network, addr, "", nil)
	}
}
