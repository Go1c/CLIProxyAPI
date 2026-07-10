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
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"golang.org/x/net/http2"
)

// roundTripFunc is declared in usage_helpers_test.go (same package).

func TestNewCodexRustlsHTTPClientUsesHTTP2Transport(t *testing.T) {
	t.Parallel()

	client := NewCodexRustlsHTTPClient(context.Background(), nil, nil)
	roundTripper, ok := client.Transport.(*codexRustlsRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want *codexRustlsRoundTripper", client.Transport)
	}
	transport, ok := roundTripper.httpsHTTP2.(*http2.Transport)
	if !ok {
		t.Fatalf("HTTPS transport type = %T, want *http2.Transport", roundTripper.httpsHTTP2)
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
	client := NewCodexRustlsHTTPClient(context.WithValue(context.Background(), "cliproxy.roundtripper", fallback), nil, nil)

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
	client := NewCodexRustlsHTTPClient(ctx, nil, nil)

	roundTripper := client.Transport.(*codexRustlsRoundTripper)
	roundTripper.httpsHTTP2 = roundTripFunc(func(req *http.Request) (*http.Response, error) {
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
	roundTripper := newCodexRustlsRoundTripper("", fallback, true)
	roundTripper.httpsHTTP2 = roundTripFunc(func(req *http.Request) (*http.Response, error) {
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
	roundTripper := newCodexRustlsRoundTripper("direct", roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("official host used fallback transport for %s", req.URL.String())
		return nil, nil
	}), true)
	roundTripper.httpsHTTP2 = roundTripFunc(func(req *http.Request) (*http.Response, error) {
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

	client := NewCodexRustlsHTTPClient(context.Background(), nil, &cliproxyauth.Auth{
		Attributes: map[string]string{"api_key": "sk-test"},
	})
	roundTripper := client.Transport.(*codexRustlsRoundTripper)
	if roundTripper.enabled {
		t.Fatal("Codex TLS fingerprint should be disabled for API-key auth")
	}
}

func TestCodexRustlsClientHelloMatchesCLI01441(t *testing.T) {
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

	dialer := newCodexRustlsDialer("http://" + listener.Addr().String())
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
