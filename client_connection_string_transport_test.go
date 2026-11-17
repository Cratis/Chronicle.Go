// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// transportFixture is a kernel stand-in that records the metadata of every call.
type transportFixture struct {
	address  string
	cert     string // PEM file with the server certificate, usable as caCertificatePath.
	mu       sync.Mutex
	metadata []metadata.MD
}

func (f *transportFixture) seen(key string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var values []string
	for _, md := range f.metadata {
		values = append(values, md.Get(key)...)
	}
	return values
}

func startTransportFixture(t *testing.T, useTLS bool) *transportFixture {
	t.Helper()
	fixture := &transportFixture{}
	options := []grpc.ServerOption{grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		fixture.mu.Lock()
		fixture.metadata = append(fixture.metadata, md)
		fixture.mu.Unlock()
		return handler(ctx, request)
	})}
	if useTLS {
		certificateServer := httptest.NewTLSServer(nil)
		serverTLS := certificateServer.TLS.Clone()
		fixture.cert = filepath.Join(t.TempDir(), "ca.pem")
		block := &pem.Block{Type: "CERTIFICATE", Bytes: certificateServer.Certificate().Raw}
		if err := os.WriteFile(fixture.cert, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
		certificateServer.Close()
		options = append(options, grpc.Creds(credentials.NewTLS(serverTLS)))
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(options...)
	clients.RegisterConnectionServiceServer(server, &boundsKernel{})
	served := make(chan struct{})
	go serveSupervisionFixture(server, listener, served, t.Error)
	t.Cleanup(func() {
		server.Stop()
		<-served
	})
	fixture.address = listener.Addr().String()
	return fixture
}

func dialFixture(t *testing.T, uri string) (*Client, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, WithConnectionString(uri))
	if err == nil {
		t.Cleanup(func() { _ = client.Close() })
	}
	return client, err
}

func TestConnectionStringAPIKeyTravelsAsMetadata(t *testing.T) {
	fixture := startTransportFixture(t, false)
	if _, err := dialFixture(t, "chronicle://"+fixture.address+"?disableTls=true&apiKey=the-key&skipKeepAlive=true&skipCompatibilityCheck=true"); err != nil {
		t.Fatal(err)
	}
	if got := fixture.seen("api-key"); len(got) == 0 || got[0] != "the-key" {
		t.Fatalf("api-key metadata = %v", got)
	}
	if got := fixture.seen("authorization"); len(got) != 0 {
		t.Fatalf("an API key must not also exchange a token, saw %v", got)
	}
}

func TestConnectionStringAPIKeyIsRefusedOverTLSOnlyWhenTLSIsOff(t *testing.T) {
	fixture := startTransportFixture(t, true)
	if _, err := dialFixture(t, "chronicle://"+fixture.address+"?apiKey=the-key&skipTlsValidation=true&skipKeepAlive=true&skipCompatibilityCheck=true"); err != nil {
		t.Fatal(err)
	}
	if got := fixture.seen("api-key"); len(got) == 0 {
		t.Fatal("the API key was not sent over TLS")
	}
}

func TestConnectionStringCleartextWithoutCredentialsAuthenticatesWithNothing(t *testing.T) {
	fixture := startTransportFixture(t, false)
	if _, err := dialFixture(t, "chronicle://"+fixture.address+"?disableTls=true&skipKeepAlive=true&skipCompatibilityCheck=true"); err != nil {
		t.Fatal(err)
	}
	if len(fixture.seen("authorization")) != 0 || len(fixture.seen("api-key")) != 0 {
		t.Fatal("a cleartext connection without credentials sent authentication")
	}
	if len(fixture.metadata) == 0 {
		t.Fatal("the kernel saw no call")
	}
}

func TestConnectionStringCACertificatePathValidatesTheServer(t *testing.T) {
	fixture := startTransportFixture(t, true)
	trusted := "chronicle://" + fixture.address + "?auth=none&skipKeepAlive=true&skipCompatibilityCheck=true&caCertificatePath=" + url.QueryEscape(fixture.cert)
	if _, err := dialFixture(t, trusted); err != nil {
		t.Fatalf("a server vouched for by caCertificatePath was rejected: %v", err)
	}
	if _, err := dialFixture(t, "chronicle://"+fixture.address+"?auth=none&skipKeepAlive=true&skipCompatibilityCheck=true"); err == nil {
		t.Fatal("validation is on by default, so an unknown certificate authority must be rejected")
	}
}

func TestConnectionStringSkipKeepAliveRefusesObserversWithoutAnOption(t *testing.T) {
	uri, err := ParseConnectionString("chronicle://host?skipKeepAlive=true")
	if err != nil {
		t.Fatal(err)
	}
	if !uri.skipKeepAlive {
		t.Fatal("skipKeepAlive was not parsed")
	}
	_, err = NewClient(WithConnectionString("chronicle://host?skipKeepAlive=true"), WithKeepAliveTimeout(time.Second))
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("an explicit keepalive timeout must conflict with the URI option: %v", err)
	}
}

// selfSignedPEM writes a self-signed certificate and key, in the given order of blocks.
func selfSignedPEM(t *testing.T, keyFirst bool) string {
	t.Helper()
	certificateServer := httptest.NewTLSServer(nil)
	defer certificateServer.Close()
	certificate := certificateServer.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificateBlock := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyBlock := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	contents := append(append([]byte(nil), certificateBlock...), keyBlock...)
	if keyFirst {
		contents = append(append([]byte(nil), keyBlock...), certificateBlock...)
	}
	path := filepath.Join(t.TempDir(), "client.pem")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestConnectionStringTLSMaterialLoading(t *testing.T) {
	valid := selfSignedPEM(t, false)
	keyFirst := selfSignedPEM(t, true)
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, query string
		options     []ClientOption
		want        error
	}{
		{"client certificate", "certificatePath=" + url.QueryEscape(valid), nil, nil},
		{"key before the certificate", "certificatePath=" + url.QueryEscape(keyFirst), nil, nil},
		{"missing certificate", "certificatePath=" + url.QueryEscape(filepath.Join(t.TempDir(), "missing.pem")), nil, ErrInvalidConfiguration},
		{"certificate without a key", "certificatePath=" + url.QueryEscape(empty), nil, ErrInvalidConfiguration},
		{"CA certificate", "caCertificatePath=" + url.QueryEscape(valid), nil, nil},
		{"missing CA file even while validation is skipped", "skipTlsValidation=true&caCertificatePath=" + url.QueryEscape(filepath.Join(t.TempDir(), "missing.pem")), nil, ErrInvalidConfiguration},
		{"CA file without certificates", "caCertificatePath=" + url.QueryEscape(empty), nil, ErrInvalidConfiguration},
		{"password-protected certificate", "certificatePath=" + url.QueryEscape(valid) + "&certificatePassword=secret", nil, ErrUnsupported},
		{"certificate with TLS disabled", "disableTls=true&certificatePath=" + url.QueryEscape(valid), nil, ErrInvalidConfiguration},
		{"URI CA beside WithTLS", "caCertificatePath=" + url.QueryEscape(valid), []ClientOption{WithTLS(&tls.Config{MinVersion: tls.VersionTLS12})}, ErrInvalidConfiguration},
		{"disableTls beside WithTLS", "disableTls=true", []ClientOption{WithTLS(&tls.Config{MinVersion: tls.VersionTLS12})}, ErrInvalidConfiguration},
		{"API key beside no authentication", "apiKey=key", []ClientOption{WithNoAuthentication()}, ErrInvalidConfiguration},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient(append([]ClientOption{WithConnectionString("chronicle://localhost?" + test.query)}, test.options...)...)
			if client != nil {
				_ = client.Close()
			}
			if !errors.Is(err, test.want) || (test.want == nil && err != nil) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestConnectionStringTLSMaterialIsApplied(t *testing.T) {
	path := selfSignedPEM(t, false)
	uri, err := ParseConnectionString("chronicle://localhost?caCertificatePath=" + url.QueryEscape(path) + "&certificatePath=" + url.QueryEscape(path))
	if err != nil {
		t.Fatal(err)
	}
	config, err := buildTLSConfig(clientConfig{}, uri)
	if err != nil {
		t.Fatal(err)
	}
	if config.RootCAs == nil || len(config.Certificates) != 1 {
		t.Fatalf("RootCAs set = %v, certificates = %d", config.RootCAs != nil, len(config.Certificates))
	}
	if config.InsecureSkipVerify {
		t.Fatal("validation must stay on by default")
	}
}

func TestParseConnectionStringNewOptions(t *testing.T) {
	for _, test := range []struct {
		name, input string
		invalid     bool
	}{
		{"all options", "chronicle://host?disableTls=true&skipKeepAlive=true&caCertificatePath=%2Fca.pem&certificatePath=%2Fc.pem&certificatePassword=pw&apiKey=k", false},
		{"case insensitive", "chronicle://host?DISABLETLS=TRUE&SkipKeepAlive=False&CACERTIFICATEPATH=%2Fca.pem", false},
		{"bad boolean", "chronicle://host?disableTls=yes", true},
		{"empty certificate path", "chronicle://host?certificatePath=", true},
		{"empty CA path", "chronicle://host?caCertificatePath=", true},
		{"duplicate", "chronicle://host?disableTls=true&DisableTls=false", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseConnectionString(test.input)
			if test.invalid != errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error = %v", err)
			}
			if !test.invalid && err != nil {
				t.Fatal(err)
			}
		})
	}
}
