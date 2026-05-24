package helps

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewCodexRustlsHTTPClientUsesHTTP1Transport(t *testing.T) {
	t.Parallel()

	client := NewCodexRustlsHTTPClient(context.Background(), nil, nil, 0)
	rt, ok := client.Transport.(*codexRustlsRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want *codexRustlsRoundTripper", client.Transport)
	}

	transport, ok := rt.httpsHTTP1.(*http.Transport)
	if !ok {
		t.Fatalf("https transport type = %T, want *http.Transport", rt.httpsHTTP1)
	}
	if transport.ForceAttemptHTTP2 {
		t.Fatal("Codex rustls transport must not force HTTP/2")
	}
	if transport.TLSNextProto == nil {
		t.Fatal("Codex rustls transport should disable implicit HTTP/2 with an empty TLSNextProto map")
	}
	if len(transport.TLSNextProto) != 0 {
		t.Fatalf("TLSNextProto has %d entries, want 0", len(transport.TLSNextProto))
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
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", fallback)
	client := NewCodexRustlsHTTPClient(ctx, nil, nil, 0)

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
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", fallback)
	client := NewCodexRustlsHTTPClient(ctx, nil, nil, 0)

	rt, ok := client.Transport.(*codexRustlsRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want *codexRustlsRoundTripper", client.Transport)
	}
	rt.httpsHTTP1 = roundTripFunc(func(req *http.Request) (*http.Response, error) {
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

func TestCodexRustlsDialerUsesHTTPProxyConnect(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen returned error: %v", err)
	}
	defer func() {
		if errClose := ln.Close(); errClose != nil {
			t.Fatalf("listener close returned error: %v", errClose)
		}
	}()

	connectLineCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, errAccept := ln.Accept()
		if errAccept != nil {
			errCh <- errAccept
			return
		}
		defer func() {
			_ = conn.Close()
		}()

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

	dialer := newCodexRustlsDialer("http://" + ln.Addr().String())
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
