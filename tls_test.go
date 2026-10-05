// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestOwnedTLSAndOAuthShareTrustPolicy(t *testing.T) {
	var tokenCalls atomic.Int32
	var authenticated atomic.Bool
	kernel := &fakeKernel{compatibility: func(ctx context.Context, _ *clients.CompatibilityRequest) {
		md, _ := metadata.FromIncomingContext(ctx)
		authenticated.Store(len(md.Get("authorization")) == 1 && md.Get("authorization")[0] == "Bearer secret-token")
	}}
	rpc := grpc.NewServer()
	clients.RegisterConnectionServiceServer(rpc, kernel)
	t.Cleanup(rpc.Stop)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			rpc.ServeHTTP(w, r)
			return
		}
		tokenCalls.Add(1)
		if r.URL.Path != "/connect/token" || r.ParseForm() != nil || r.Form.Get("client_id") != "chronicle-dev-client" || r.Form.Get("client_secret") != "chronicle-dev-secret" {
			t.Error("wrong OAuth defaults")
		}
		_, _ = w.Write([]byte(`{"access_token":"secret-token","expires_in":3600}`))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	endpoint := "chronicle://" + strings.TrimPrefix(server.URL, "https://")
	untrusted, err := chronicle.NewClient(chronicle.WithConnectionString(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	if err = untrusted.Connect(testContext(t)); err == nil {
		t.Fatal("self-signed certificate trusted by default")
	}
	if err = untrusted.Close(); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	config := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	client, err := chronicle.NewClient(chronicle.WithConnectionString(endpoint), chronicle.WithTLS(config))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	config.ServerName = "mutated.invalid"
	if tokenCalls.Load() != 0 {
		t.Fatal("constructor performed I/O")
	}
	if err = client.Connect(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if tokenCalls.Load() != 1 || !authenticated.Load() {
		t.Fatal("token flow or TLS trust not shared")
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}
