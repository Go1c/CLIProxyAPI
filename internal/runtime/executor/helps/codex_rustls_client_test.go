package helps

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	tls "github.com/refraction-networking/utls"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	"golang.org/x/net/http2"
)

// roundTripFunc is declared in usage_helpers_test.go (same package).

func TestNewCodexRustlsHTTPClientUsesHTTP2Transport(t *testing.T) {
	t.Parallel()

	client, errClient := NewCodexRustlsHTTPClient(context.Background(), nil, nil)
	if errClient != nil {
		t.Fatalf("NewCodexRustlsHTTPClient returned error: %v", errClient)
	}
	roundTripper, ok := client.Transport.(*codexRustlsRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want *codexRustlsRoundTripper", client.Transport)
	}
	transport, ok := roundTripper.httpsHTTP2.roundTripper.(*http2.Transport)
	if !ok {
		t.Fatalf("HTTPS transport type = %T, want *http2.Transport", roundTripper.httpsHTTP2.roundTripper)
	}
	if transport.DialTLSContext == nil {
		t.Fatal("expected custom rustls-like DialTLSContext")
	}
}

func TestCodexRustlsRoundTripperFallsBackForPlainHTTP(t *testing.T) {
	t.Parallel()

	called := false
	fallback := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		return emptyTestResponse(req), nil
	})
	client, errClient := NewCodexRustlsHTTPClient(context.WithValue(context.Background(), "cliproxy.roundtripper", fallback), nil, nil)
	if errClient != nil {
		t.Fatalf("NewCodexRustlsHTTPClient returned error: %v", errClient)
	}

	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:18092/v1/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do returned error: %v", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		t.Fatalf("response close returned error: %v", errClose)
	}
	if !called {
		t.Fatal("expected HTTP request to use fallback round tripper")
	}
}

func TestCodexRustlsRoundTripperUsesContextFallbackForHTTPS(t *testing.T) {
	t.Parallel()

	called := false
	fallback := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		return emptyTestResponse(req), nil
	})
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", fallback)
	client, errClient := NewCodexRustlsHTTPClient(ctx, nil, nil)
	if errClient != nil {
		t.Fatalf("NewCodexRustlsHTTPClient returned error: %v", errClient)
	}

	roundTripper := client.Transport.(*codexRustlsRoundTripper)
	roundTripper.httpsHTTP2.roundTripper = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("HTTPS request bypassed context fallback for %s", req.URL.String())
		return nil, nil
	})

	req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do returned error: %v", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		t.Fatalf("response close returned error: %v", errClose)
	}
	if !called {
		t.Fatal("expected HTTPS request to use context fallback round tripper")
	}
}

func TestCodexRustlsRoundTripperFallsBackForCustomHost(t *testing.T) {
	t.Parallel()

	called := false
	fallback := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		return emptyTestResponse(req), nil
	})
	roundTripper, errRoundTripper := newCodexRustlsRoundTripper("", fallback, true, (&config.Config{}).CodexProxyTimeouts(), nil)
	if errRoundTripper != nil {
		t.Fatalf("newCodexRustlsRoundTripper returned error: %v", errRoundTripper)
	}
	roundTripper.httpsHTTP2.roundTripper = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("custom host used Codex TLS transport for %s", req.URL.String())
		return nil, nil
	})

	req, err := http.NewRequest(http.MethodPost, "https://example.test/v1/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	resp, err := roundTripper.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip returned error: %v", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		t.Fatalf("response close returned error: %v", errClose)
	}
	if !called {
		t.Fatal("expected custom host to use fallback round tripper")
	}
}

func TestCodexRustlsRoundTripperUsesFingerprintForOfficialHost(t *testing.T) {
	t.Parallel()

	called := false
	roundTripper, errRoundTripper := newCodexRustlsRoundTripper("direct", roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("official host used fallback transport for %s", req.URL.String())
		return nil, nil
	}), true, (&config.Config{}).CodexProxyTimeouts(), nil)
	if errRoundTripper != nil {
		t.Fatalf("newCodexRustlsRoundTripper returned error: %v", errRoundTripper)
	}
	roundTripper.httpsHTTP2.roundTripper = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		return emptyTestResponse(req), nil
	})

	req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	resp, err := roundTripper.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip returned error: %v", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		t.Fatalf("response close returned error: %v", errClose)
	}
	if !called {
		t.Fatal("expected official host to use Codex TLS transport")
	}
}

func TestNewCodexRustlsHTTPClientDisablesFingerprintForAPIKey(t *testing.T) {
	t.Parallel()

	client, errClient := NewCodexRustlsHTTPClient(context.Background(), nil, &cliproxyauth.Auth{
		Attributes: map[string]string{"api_key": "sk-test"},
	})
	if errClient != nil {
		t.Fatalf("NewCodexRustlsHTTPClient returned error: %v", errClient)
	}
	roundTripper := client.Transport.(*codexRustlsRoundTripper)
	if roundTripper.enabled {
		t.Fatal("Codex TLS fingerprint should be disabled for API-key auth")
	}
}

func TestNewCodexRustlsHTTPClientInvalidProxyFailsClosed(t *testing.T) {
	t.Parallel()

	calledFallback := false
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calledFallback = true
		return emptyTestResponse(req), nil
	}))
	client, errClient := NewCodexRustlsHTTPClient(ctx, nil, &cliproxyauth.Auth{ProxyURL: "socks5:user:pass@host:443"})
	if errClient == nil || client != nil {
		t.Fatalf("client=%v error=%v, want fail-closed construction error", client, errClient)
	}
	proxyErr, ok := proxyutil.AsError(errClient)
	if !ok || proxyErr.Code != proxyutil.CodeConfigInvalid {
		t.Fatalf("error = %v, want %s", errClient, proxyutil.CodeConfigInvalid)
	}
	if calledFallback {
		t.Fatal("invalid proxy configuration called fallback/direct transport")
	}
}

func TestNewCodexRustlsHTTPClientRequiredProxyRejectsDirect(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{SDKConfig: config.SDKConfig{CodexProxyRequired: true}}
	client, errClient := NewCodexRustlsHTTPClient(context.Background(), cfg, &cliproxyauth.Auth{ProxyURL: "direct"})
	if errClient == nil || client != nil {
		t.Fatalf("client=%v error=%v, want required proxy error", client, errClient)
	}
	proxyErr, ok := proxyutil.AsError(errClient)
	if !ok || proxyErr.Code != proxyutil.CodeRequired {
		t.Fatalf("error = %v, want %s", errClient, proxyutil.CodeRequired)
	}
}

func TestCodexRustlsSOCKS5AuthenticationFailureIsTyped(t *testing.T) {
	t.Parallel()

	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("listen: %v", errListen)
	}
	defer listener.Close()
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		header := make([]byte, 2)
		if _, errRead := io.ReadFull(conn, header); errRead != nil {
			return
		}
		methods := make([]byte, int(header[1]))
		if _, errRead := io.ReadFull(conn, methods); errRead != nil {
			return
		}
		_, _ = conn.Write([]byte{0x05, 0x02})
		authHeader := make([]byte, 2)
		if _, errRead := io.ReadFull(conn, authHeader); errRead != nil {
			return
		}
		username := make([]byte, int(authHeader[1]))
		_, _ = io.ReadFull(conn, username)
		passwordLength := make([]byte, 1)
		_, _ = io.ReadFull(conn, passwordLength)
		password := make([]byte, int(passwordLength[0]))
		_, _ = io.ReadFull(conn, password)
		_, _ = conn.Write([]byte{0x01, 0x01})
	}()

	timeouts := (&config.Config{}).CodexProxyTimeouts()
	timeouts.ProxyConnect = time.Second
	dialer, errDialer := newCodexRustlsDialer("socks5://user:pass@"+listener.Addr().String(), timeouts, nil)
	if errDialer != nil {
		t.Fatalf("new dialer: %v", errDialer)
	}
	_, errDial := dialer.dialTCP(context.Background(), "tcp", "example.com:443")
	proxyErr, ok := proxyutil.AsError(errDial)
	if !ok || proxyErr.Code != proxyutil.CodeAuthFailed {
		t.Fatalf("error = %v, want %s", errDial, proxyutil.CodeAuthFailed)
	}
}

func TestCodexRustlsSOCKS5BlackholeTimesOut(t *testing.T) {
	t.Parallel()

	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("listen: %v", errListen)
	}
	defer listener.Close()
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()

	timeouts := (&config.Config{}).CodexProxyTimeouts()
	timeouts.ProxyConnect = 80 * time.Millisecond
	dialer, errDialer := newCodexRustlsDialer("socks5://user:pass@"+listener.Addr().String(), timeouts, nil)
	if errDialer != nil {
		t.Fatalf("new dialer: %v", errDialer)
	}
	startedAt := time.Now()
	_, errDial := dialer.dialTCP(context.Background(), "tcp", "example.com:443")
	proxyErr, ok := proxyutil.AsError(errDial)
	if !ok || proxyErr.Code != proxyutil.CodeConnectTimeout {
		t.Fatalf("error = %v, want %s", errDial, proxyutil.CodeConnectTimeout)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("blackhole timeout took %v", elapsed)
	}
}

func TestCodexRustlsTLSBlackholeTimesOut(t *testing.T) {
	t.Parallel()

	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("listen: %v", errListen)
	}
	defer listener.Close()
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()

	timeouts := (&config.Config{}).CodexProxyTimeouts()
	timeouts.ProxyConnect = time.Second
	timeouts.TLSHandshake = 80 * time.Millisecond
	dialer, errDialer := newCodexRustlsDialer("direct", timeouts, nil)
	if errDialer != nil {
		t.Fatalf("new dialer: %v", errDialer)
	}
	_, errDial := dialer.dialTLS(context.Background(), "tcp", listener.Addr().String(), "example.com", []string{"h2"})
	proxyErr, ok := proxyutil.AsError(errDial)
	if !ok || proxyErr.Code != proxyutil.CodeTLSTimeout {
		t.Fatalf("error = %v, want %s", errDial, proxyutil.CodeTLSTimeout)
	}
}

func TestRoundTripBeforeHeadersTimesOutWithoutLimitingBody(t *testing.T) {
	t.Parallel()

	t.Run("header timeout", func(t *testing.T) {
		connected := make(chan struct{})
		close(connected)
		transport := codexHTTP2Transport{
			connected: connected,
			proxyHash: "proxy-a",
			roundTripper: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}),
		}
		timeouts := config.CodexProxyTimeouts{ResponseHeader: 40 * time.Millisecond, FirstByte: time.Second}
		req, _ := http.NewRequest(http.MethodGet, "https://chatgpt.com/test", nil)
		_, errRoundTrip := roundTripBeforeHeaders(req, transport, timeouts)
		proxyErr, ok := proxyutil.AsError(errRoundTrip)
		if !ok || proxyErr.Code != proxyutil.CodeUpstreamHeaderTimeout {
			t.Fatalf("error = %v, want %s", errRoundTrip, proxyutil.CodeUpstreamHeaderTimeout)
		}
	})

	t.Run("body remains readable", func(t *testing.T) {
		connected := make(chan struct{})
		close(connected)
		transport := codexHTTP2Transport{
			connected: connected,
			roundTripper: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: &contextCheckingBody{ctx: req.Context(), data: []byte("ok")}, Request: req}, nil
			}),
		}
		timeouts := config.CodexProxyTimeouts{ResponseHeader: 30 * time.Millisecond, FirstByte: 50 * time.Millisecond}
		req, _ := http.NewRequest(http.MethodGet, "https://chatgpt.com/test", nil)
		response, errRoundTrip := roundTripBeforeHeaders(req, transport, timeouts)
		if errRoundTrip != nil {
			t.Fatalf("RoundTrip returned error: %v", errRoundTrip)
		}
		time.Sleep(100 * time.Millisecond)
		body, errRead := io.ReadAll(response.Body)
		if errRead != nil || string(body) != "ok" {
			t.Fatalf("body=%q error=%v", body, errRead)
		}
		_ = response.Body.Close()
	})
}

type contextCheckingBody struct {
	ctx  context.Context
	data []byte
}

func (b *contextCheckingBody) Read(p []byte) (int, error) {
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	default:
	}
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func (b *contextCheckingBody) Close() error { return nil }

func TestCodexRustlsClientHelloMatchesCLI01460(t *testing.T) {
	t.Parallel()

	spec := codexRustlsLikeClientHelloSpec([]string{"h2", "http/1.1"})
	wantCiphers := []uint16{
		0x1302,
		0x1301,
		0x1303,
		0xc02c,
		0xc02b,
		0xcca9,
		0xc030,
		0xc02f,
		0xcca8,
		0x00ff,
	}
	if !reflect.DeepEqual(spec.CipherSuites, wantCiphers) {
		t.Fatalf("CipherSuites = %#v, want %#v", spec.CipherSuites, wantCiphers)
	}

	gotExtensionIDs := codexTestExtensionIDs(spec.Extensions)
	sort.Slice(gotExtensionIDs, func(i, j int) bool { return gotExtensionIDs[i] < gotExtensionIDs[j] })
	wantExtensionIDs := []uint16{0, 5, 10, 11, 13, 16, 23, 35, 43, 45, 51}
	if !reflect.DeepEqual(gotExtensionIDs, wantExtensionIDs) {
		t.Fatalf("extension IDs = %#v, want %#v", gotExtensionIDs, wantExtensionIDs)
	}

	foundALPN := false
	for _, extension := range spec.Extensions {
		alpn, ok := extension.(*tls.ALPNExtension)
		if !ok {
			continue
		}
		foundALPN = true
		if !reflect.DeepEqual(alpn.AlpnProtocols, []string{"h2", "http/1.1"}) {
			t.Fatalf("ALPN protocols = %#v", alpn.AlpnProtocols)
		}
	}
	if !foundALPN {
		t.Fatal("expected ALPN extension")
	}

	wantKeyShares := []tls.CurveID{tls.X25519MLKEM768, tls.X25519}
	wantCurves := []tls.CurveID{tls.X25519MLKEM768, tls.X25519, tls.CurveP256, tls.CurveP384}
	wantSignatures := []tls.SignatureScheme{
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
	}
	for _, extension := range spec.Extensions {
		switch typed := extension.(type) {
		case *tls.KeyShareExtension:
			got := make([]tls.CurveID, 0, len(typed.KeyShares))
			for _, keyShare := range typed.KeyShares {
				got = append(got, keyShare.Group)
			}
			if !reflect.DeepEqual(got, wantKeyShares) {
				t.Fatalf("key shares = %#v, want %#v", got, wantKeyShares)
			}
		case *tls.SupportedCurvesExtension:
			if !reflect.DeepEqual(typed.Curves, wantCurves) {
				t.Fatalf("curves = %#v, want %#v", typed.Curves, wantCurves)
			}
		case *tls.SignatureAlgorithmsExtension:
			if !reflect.DeepEqual(typed.SupportedSignatureAlgorithms, wantSignatures) {
				t.Fatalf("signature algorithms = %#v, want %#v", typed.SupportedSignatureAlgorithms, wantSignatures)
			}
		}
	}
}

func TestCodexRustlsWebsocketSpecOmitsALPN(t *testing.T) {
	t.Parallel()

	spec := codexRustlsLikeClientHelloSpec(nil)
	for _, extension := range spec.Extensions {
		if _, ok := extension.(*tls.ALPNExtension); ok {
			t.Fatal("websocket ClientHello must not offer h2")
		}
	}
}

func TestCodexRustlsDialerUsesHTTPProxyConnect(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen returned error: %v", err)
	}
	defer func() {
		if errClose := listener.Close(); errClose != nil {
			t.Fatalf("listener close returned error: %v", errClose)
		}
	}()

	connectLineCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			errCh <- errAccept
			return
		}
		defer func() { _ = conn.Close() }()

		if errDeadline := conn.SetDeadline(time.Now().Add(2 * time.Second)); errDeadline != nil {
			errCh <- errDeadline
			return
		}
		reader := bufio.NewReader(conn)
		line, errRead := reader.ReadString('\n')
		if errRead != nil {
			errCh <- errRead
			return
		}
		connectLineCh <- strings.TrimSpace(line)
		for {
			headerLine, errReadHeader := reader.ReadString('\n')
			if errReadHeader != nil {
				errCh <- errReadHeader
				return
			}
			if strings.TrimSpace(headerLine) == "" {
				break
			}
		}
		if _, errWrite := conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); errWrite != nil {
			errCh <- errWrite
			return
		}
		_, _ = reader.Peek(1)
	}()

	dialer, errDialer := newCodexRustlsDialer("http://"+listener.Addr().String(), (&config.Config{}).CodexProxyTimeouts(), nil)
	if errDialer != nil {
		t.Fatalf("newCodexRustlsDialer returned error: %v", errDialer)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, errDial := dialer.dialTCP(ctx, "tcp", "codex.example:443")
	if errDial != nil {
		t.Fatalf("dialTCP returned error: %v", errDial)
	}
	if errClose := conn.Close(); errClose != nil {
		t.Fatalf("conn close returned error: %v", errClose)
	}

	select {
	case line := <-connectLineCh:
		if line != "CONNECT codex.example:443 HTTP/1.1" {
			t.Fatalf("CONNECT line = %q, want %q", line, "CONNECT codex.example:443 HTTP/1.1")
		}
	case errProxy := <-errCh:
		t.Fatalf("proxy returned error: %v", errProxy)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for proxy CONNECT")
	}
}

func emptyTestResponse(req *http.Request) *http.Response {
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    req,
	}
}

func codexTestExtensionIDs(extensions []tls.TLSExtension) []uint16 {
	ids := make([]uint16, 0, len(extensions))
	for _, extension := range extensions {
		switch extension.(type) {
		case *tls.SNIExtension:
			ids = append(ids, 0)
		case *tls.StatusRequestExtension:
			ids = append(ids, 5)
		case *tls.SupportedCurvesExtension:
			ids = append(ids, 10)
		case *tls.SupportedPointsExtension:
			ids = append(ids, 11)
		case *tls.SignatureAlgorithmsExtension:
			ids = append(ids, 13)
		case *tls.ALPNExtension:
			ids = append(ids, 16)
		case *tls.ExtendedMasterSecretExtension:
			ids = append(ids, 23)
		case *tls.SessionTicketExtension:
			ids = append(ids, 35)
		case *tls.SupportedVersionsExtension:
			ids = append(ids, 43)
		case *tls.PSKKeyExchangeModesExtension:
			ids = append(ids, 45)
		case *tls.KeyShareExtension:
			ids = append(ids, 51)
		default:
			ids = append(ids, 0xffff)
		}
	}
	return ids
}

func TestIsCodexOfficialHostIncludesAuthOpenAI(t *testing.T) {
	t.Parallel()
	cases := []struct {
		host string
		want bool
	}{
		{"chatgpt.com", true},
		{"www.chatgpt.com", true},
		{"auth.openai.com", true},
		{"AUTH.OPENAI.COM", true},
		{"api.openai.com", false},
		{"example.com", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isCodexOfficialHost(tc.host); got != tc.want {
			t.Fatalf("isCodexOfficialHost(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}
