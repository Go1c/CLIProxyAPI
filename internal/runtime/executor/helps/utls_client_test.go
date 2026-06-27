package helps

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	tls "github.com/refraction-networking/utls"
)

type utlsClientRoundTripFunc func(*http.Request) (*http.Response, error)

func (f utlsClientRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewUtlsHTTPClientUsesContextRoundTripperForProtectedHost(t *testing.T) {
	t.Parallel()

	called := false
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", utlsClientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		if req.URL.Hostname() != "chatgpt.com" {
			t.Fatalf("hostname = %q, want chatgpt.com", req.URL.Hostname())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("{}")),
			Request:    req,
		}, nil
	}))

	client := NewUtlsHTTPClient(ctx, nil, nil, 0)
	resp, err := client.Get("https://chatgpt.com/backend-api/codex/responses")
	if err != nil {
		t.Fatalf("client.Get returned error: %v", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		t.Fatalf("response body close returned error: %v", errClose)
	}
	if !called {
		t.Fatal("expected context RoundTripper to handle protected host request")
	}
}

func TestCodexRustlsClientHelloSpecMatchesCapturedOrder(t *testing.T) {
	t.Parallel()

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
	wantExtensions := []uint16{0x0000, 0x0017, 0x0033, 0x000b, 0x0023, 0x000d, 0x0005, 0x002d, 0x000a, 0x002b}

	specs := map[string]*tls.ClientHelloSpec{
		"utls":   newCodexRustlsClientHelloSpec(),
		"rustls": codexRustlsLikeClientHelloSpec(),
	}
	for name, spec := range specs {
		if !reflect.DeepEqual(spec.CipherSuites, wantCiphers) {
			t.Fatalf("%s CipherSuites = %#v, want %#v", name, spec.CipherSuites, wantCiphers)
		}
		if got := codexTestExtensionIDs(spec.Extensions); !reflect.DeepEqual(got, wantExtensions) {
			t.Fatalf("%s extension IDs = %#v, want %#v", name, got, wantExtensions)
		}
	}
}

func TestNewCodexUtlsHTTPClientUsesContextRoundTripperForProtectedHost(t *testing.T) {
	t.Parallel()

	called := false
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", utlsClientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		if req.URL.Hostname() != "chatgpt.com" {
			t.Fatalf("hostname = %q, want chatgpt.com", req.URL.Hostname())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("{}")),
			Request:    req,
		}, nil
	}))

	client := NewCodexUtlsHTTPClient(ctx, nil, nil, 0)
	resp, err := client.Get("https://chatgpt.com/backend-api/codex/responses")
	if err != nil {
		t.Fatalf("client.Get returned error: %v", err)
	}
	if errClose := resp.Body.Close(); errClose != nil {
		t.Fatalf("response body close returned error: %v", errClose)
	}
	if !called {
		t.Fatal("expected context RoundTripper to handle protected host request")
	}
}

func codexTestExtensionIDs(exts []tls.TLSExtension) []uint16 {
	ids := make([]uint16, 0, len(exts))
	for _, ext := range exts {
		switch ext.(type) {
		case *tls.SNIExtension:
			ids = append(ids, 0x0000)
		case *tls.ExtendedMasterSecretExtension:
			ids = append(ids, 0x0017)
		case *tls.KeyShareExtension:
			ids = append(ids, 0x0033)
		case *tls.SupportedPointsExtension:
			ids = append(ids, 0x000b)
		case *tls.SessionTicketExtension:
			ids = append(ids, 0x0023)
		case *tls.SignatureAlgorithmsExtension:
			ids = append(ids, 0x000d)
		case *tls.StatusRequestExtension:
			ids = append(ids, 0x0005)
		case *tls.PSKKeyExchangeModesExtension:
			ids = append(ids, 0x002d)
		case *tls.SupportedCurvesExtension:
			ids = append(ids, 0x000a)
		case *tls.SupportedVersionsExtension:
			ids = append(ids, 0x002b)
		default:
			ids = append(ids, 0xffff)
		}
	}
	return ids
}
