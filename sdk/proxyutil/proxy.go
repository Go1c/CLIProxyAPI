package proxyutil

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/proxy"
)

// Mode describes how a proxy setting should be interpreted.
type Mode int

const (
	// ModeInherit means no explicit proxy behavior was configured.
	ModeInherit Mode = iota
	// ModeDirect means outbound requests must bypass proxies explicitly.
	ModeDirect
	// ModeProxy means a concrete proxy URL was configured.
	ModeProxy
	// ModeInvalid means the proxy setting is present but malformed or unsupported.
	ModeInvalid
)

// Setting is the normalized interpretation of a proxy configuration value.
type Setting struct {
	Raw  string
	Mode Mode
	URL  *url.URL
}

// Parse normalizes a proxy configuration value into inherit, direct, or proxy modes.
func Parse(raw string) (Setting, error) {
	trimmed := strings.TrimSpace(raw)
	setting := Setting{Raw: trimmed}

	if trimmed == "" {
		setting.Mode = ModeInherit
		return setting, nil
	}

	if strings.EqualFold(trimmed, "direct") || strings.EqualFold(trimmed, "none") {
		setting.Mode = ModeDirect
		return setting, nil
	}

	parsedURL, errParse := url.Parse(trimmed)
	if errParse != nil {
		setting.Mode = ModeInvalid
		return setting, NewError(CodeConfigInvalid, StageConfig, false, "", "failed to parse proxy URL", errParse)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" || parsedURL.Hostname() == "" {
		setting.Mode = ModeInvalid
		return setting, NewError(CodeConfigInvalid, StageConfig, false, "", "proxy URL missing scheme/host; expected socks5://<credentials>@host:port", errParse)
	}

	parsedURL.Scheme = strings.ToLower(parsedURL.Scheme)
	switch parsedURL.Scheme {
	case "socks5", "socks5h", "http", "https":
		if (parsedURL.Scheme == "socks5" || parsedURL.Scheme == "socks5h") && parsedURL.Port() == "" {
			setting.Mode = ModeInvalid
			return setting, NewError(CodeConfigInvalid, StageConfig, false, "", "proxy URL missing port; expected socks5://<credentials>@host:port", nil)
		}
		setting.Mode = ModeProxy
		setting.URL = parsedURL
		return setting, nil
	default:
		setting.Mode = ModeInvalid
		return setting, NewError(CodeConfigInvalid, StageConfig, false, "", "unsupported proxy scheme", nil)
	}
}

// Hash returns a stable credential-free hash derived only from scheme, host,
// and port. Direct and inherited modes do not have a proxy endpoint hash.
func Hash(raw string) string {
	setting, errParse := Parse(raw)
	if errParse != nil || setting.Mode != ModeProxy || setting.URL == nil {
		return ""
	}
	port := setting.URL.Port()
	if port == "" {
		port = defaultPort(setting.URL.Scheme)
	}
	endpoint := strings.ToLower(setting.URL.Scheme) + "://" + net.JoinHostPort(strings.ToLower(setting.URL.Hostname()), port)
	sum := sha256.Sum256([]byte(endpoint))
	return hex.EncodeToString(sum[:16])
}

func defaultPort(scheme string) string {
	switch strings.ToLower(scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}

func cloneDefaultTransport() *http.Transport {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok && transport != nil {
		return transport.Clone()
	}
	return &http.Transport{}
}

// NewDirectTransport returns a transport that bypasses environment proxies.
func NewDirectTransport() *http.Transport {
	clone := cloneDefaultTransport()
	clone.Proxy = nil
	return clone
}

// BuildHTTPTransport constructs an HTTP transport for the provided proxy setting.
func BuildHTTPTransport(raw string) (*http.Transport, Mode, error) {
	return BuildHTTPTransportWithConnectTimeout(raw, 0)
}

// BuildHTTPTransportWithConnectTimeout constructs an HTTP transport whose
// connection-layer dial respects the supplied timeout and request context.
func BuildHTTPTransportWithConnectTimeout(raw string, connectTimeout time.Duration) (*http.Transport, Mode, error) {
	setting, errParse := Parse(raw)
	if errParse != nil {
		return nil, setting.Mode, errParse
	}

	switch setting.Mode {
	case ModeInherit:
		return nil, setting.Mode, nil
	case ModeDirect:
		return NewDirectTransport(), setting.Mode, nil
	case ModeProxy:
		if setting.URL.Scheme == "socks5" || setting.URL.Scheme == "socks5h" {
			dialer, _, errSOCKS5 := BuildContextDialer(raw, connectTimeout)
			if errSOCKS5 != nil {
				return nil, setting.Mode, errSOCKS5
			}
			transport := cloneDefaultTransport()
			transport.Proxy = nil
			transport.DialContext = dialer.DialContext
			return transport, setting.Mode, nil
		}
		transport := cloneDefaultTransport()
		transport.Proxy = http.ProxyURL(setting.URL)
		transport.DialContext = (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext
		return transport, setting.Mode, nil
	default:
		return nil, setting.Mode, nil
	}
}

// ContextDialer is a proxy dialer whose in-flight connection and handshake can
// be cancelled by the request context.
type ContextDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

type directContextDialer struct {
	dialer net.Dialer
}

func (d *directContextDialer) Dial(network, addr string) (net.Conn, error) {
	return d.dialer.Dial(network, addr)
}

func (d *directContextDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, addr)
}

type proxyContextDialer struct {
	dialer proxy.ContextDialer
}

func (d *proxyContextDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, addr)
}

// BuildContextDialer constructs a context-aware connection-layer dialer.
func BuildContextDialer(raw string, connectTimeout time.Duration) (ContextDialer, Mode, error) {
	setting, errParse := Parse(raw)
	if errParse != nil {
		return nil, setting.Mode, errParse
	}
	base := &directContextDialer{dialer: net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}}
	switch setting.Mode {
	case ModeInherit, ModeDirect:
		return base, setting.Mode, nil
	case ModeProxy:
		if setting.URL.Scheme == "http" || setting.URL.Scheme == "https" {
			return &httpConnectContextDialer{proxyURL: setting.URL, dialer: base}, setting.Mode, nil
		}
		var proxyAuth *proxy.Auth
		if setting.URL.User != nil {
			password, _ := setting.URL.User.Password()
			proxyAuth = &proxy.Auth{User: setting.URL.User.Username(), Password: password}
		}
		socksDialer, errSOCKS5 := proxy.SOCKS5("tcp", setting.URL.Host, proxyAuth, base)
		if errSOCKS5 != nil {
			return nil, setting.Mode, NewError(CodeConfigInvalid, StageConfig, false, Hash(raw), "failed to create SOCKS5 dialer", errSOCKS5)
		}
		contextDialer, ok := socksDialer.(proxy.ContextDialer)
		if !ok {
			return nil, setting.Mode, NewError(CodeConfigInvalid, StageConfig, false, Hash(raw), "SOCKS5 dialer does not support context cancellation", nil)
		}
		return &proxyContextDialer{dialer: contextDialer}, setting.Mode, nil
	default:
		return nil, setting.Mode, NewError(CodeConfigInvalid, StageConfig, false, "", "proxy URL is invalid", nil)
	}
}

// BuildDialer constructs a proxy dialer for settings that operate at the connection layer.
func BuildDialer(raw string) (proxy.Dialer, Mode, error) {
	setting, errParse := Parse(raw)
	if errParse != nil {
		return nil, setting.Mode, errParse
	}

	switch setting.Mode {
	case ModeInherit:
		return nil, setting.Mode, nil
	case ModeDirect:
		return proxy.Direct, setting.Mode, nil
	case ModeProxy:
		if setting.URL.Scheme == "http" || setting.URL.Scheme == "https" {
			return &httpConnectDialer{proxyURL: setting.URL, dialer: proxy.Direct}, setting.Mode, nil
		}
		dialer, errDialer := proxy.FromURL(setting.URL, proxy.Direct)
		if errDialer != nil {
			return nil, setting.Mode, fmt.Errorf("create proxy dialer failed: %w", errDialer)
		}
		return dialer, setting.Mode, nil
	default:
		return nil, setting.Mode, nil
	}
}

type httpConnectDialer struct {
	proxyURL *url.URL
	dialer   proxy.Dialer
}

type httpConnectContextDialer struct {
	proxyURL *url.URL
	dialer   ContextDialer
}

func (d *httpConnectContextDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d == nil || d.proxyURL == nil || d.dialer == nil {
		return nil, fmt.Errorf("HTTP proxy dialer is not configured")
	}
	proxyConn, errDial := d.dialer.DialContext(ctx, network, proxyDialAddr(d.proxyURL))
	if errDial != nil {
		return nil, fmt.Errorf("dial HTTP proxy failed: %w", errDial)
	}
	conn := proxyConn
	if deadline, ok := ctx.Deadline(); ok {
		if errDeadline := conn.SetDeadline(deadline); errDeadline != nil {
			_ = conn.Close()
			return nil, errDeadline
		}
		defer func() { _ = conn.SetDeadline(time.Time{}) }()
	}
	if d.proxyURL.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: d.proxyURL.Hostname()})
		if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("HTTPS proxy TLS handshake failed: %w", errHandshake)
		}
		conn = tlsConn
	}
	req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Host: addr}, Host: addr, Header: make(http.Header)}
	if d.proxyURL.User != nil {
		req.Header.Set("Proxy-Authorization", proxyAuthorization(d.proxyURL.User))
	}
	if errWrite := req.Write(conn); errWrite != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write CONNECT request failed: %w", errWrite)
	}
	reader := bufio.NewReader(conn)
	resp, errRead := http.ReadResponse(reader, req)
	if errRead != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read CONNECT response failed: %w", errRead)
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	if resp.StatusCode != http.StatusOK {
		_ = conn.Close()
		return nil, fmt.Errorf("proxy CONNECT returned status %s", resp.Status)
	}
	if reader.Buffered() > 0 {
		return &bufferedConn{Conn: conn, reader: reader}, nil
	}
	return conn, nil
}

func (d *httpConnectDialer) Dial(network, addr string) (net.Conn, error) {
	proxyConn, errDial := d.dialer.Dial(network, proxyDialAddr(d.proxyURL))
	if errDial != nil {
		return nil, fmt.Errorf("dial HTTP proxy failed: %w", errDial)
	}

	conn := proxyConn
	if d.proxyURL.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: d.proxyURL.Hostname()})
		if errHandshake := tlsConn.Handshake(); errHandshake != nil {
			if errClose := conn.Close(); errClose != nil {
				return nil, fmt.Errorf("HTTPS proxy TLS handshake failed: %w; close failed: %v", errHandshake, errClose)
			}
			return nil, fmt.Errorf("HTTPS proxy TLS handshake failed: %w", errHandshake)
		}
		conn = tlsConn
	}

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: addr},
		Host:   addr,
		Header: make(http.Header),
	}
	if d.proxyURL.User != nil {
		req.Header.Set("Proxy-Authorization", proxyAuthorization(d.proxyURL.User))
	}
	if errWrite := req.Write(conn); errWrite != nil {
		if errClose := conn.Close(); errClose != nil {
			return nil, fmt.Errorf("write CONNECT request failed: %w; close failed: %v", errWrite, errClose)
		}
		return nil, fmt.Errorf("write CONNECT request failed: %w", errWrite)
	}

	reader := bufio.NewReader(conn)
	resp, errRead := http.ReadResponse(reader, req)
	if errRead != nil {
		if errClose := conn.Close(); errClose != nil {
			return nil, fmt.Errorf("read CONNECT response failed: %w; close failed: %v", errRead, errClose)
		}
		return nil, fmt.Errorf("read CONNECT response failed: %w", errRead)
	}
	if resp.StatusCode != http.StatusOK {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		if errClose := conn.Close(); errClose != nil {
			return nil, fmt.Errorf("proxy CONNECT returned status %s; close failed: %v", resp.Status, errClose)
		}
		return nil, fmt.Errorf("proxy CONNECT returned status %s", resp.Status)
	}

	if reader.Buffered() > 0 {
		return &bufferedConn{Conn: conn, reader: reader}, nil
	}
	return conn, nil
}

func proxyDialAddr(proxyURL *url.URL) string {
	port := proxyURL.Port()
	if port == "" {
		port = "80"
		if proxyURL.Scheme == "https" {
			port = "443"
		}
	}
	return net.JoinHostPort(proxyURL.Hostname(), port)
}

func proxyAuthorization(user *url.Userinfo) string {
	username := user.Username()
	password, _ := user.Password()
	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return "Basic " + encoded
}

// Redact returns a log-safe proxy URL with credentials and path-like data removed.
func Redact(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	parsedURL, errParse := url.Parse(trimmed)
	if errParse != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return "<invalid proxy URL>"
	}

	redacted := &url.URL{
		Scheme: parsedURL.Scheme,
		Host:   parsedURL.Host,
	}
	if parsedURL.User != nil {
		redacted.User = url.User("redacted")
	}
	return redacted.String()
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if c.reader.Buffered() > 0 {
		return c.reader.Read(p)
	}
	return c.Conn.Read(p)
}
