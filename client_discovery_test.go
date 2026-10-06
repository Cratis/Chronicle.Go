// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rotatingResolver struct {
	calls         atomic.Int32
	first, second *net.SRV
}

func (r *rotatingResolver) LookupSRV(ctx context.Context, _, _, _ string) (string, []*net.SRV, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if r.calls.Add(1) == 1 {
		return "", []*net.SRV{r.first}, nil
	}
	return "", []*net.SRV{r.second}, nil
}

func discoveryServer(t *testing.T) (*supervisedKernel, *net.SRV, *atomic.Int32) {
	t.Helper()
	kernel, address, tokens, _ := discoveryServerWithRPCCount(t)
	return kernel, address, tokens
}

func discoveryServerWithRPCCount(t *testing.T) (*supervisedKernel, *net.SRV, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	kernel := &supervisedKernel{lifecycleKernel: lifecycleKernel{endStream: make(chan error, 1)}, registered: make(chan struct{}, 10), namespaceCounts: make(map[string]int)}
	rpc := grpc.NewServer()
	clients.RegisterConnectionServiceServer(rpc, kernel)
	eventstores.RegisterEventStoresServer(rpc, kernel)
	eventtypes.RegisterEventTypesServer(rpc, kernel)
	namespaces.RegisterNamespacesServer(rpc, kernel)
	tokens, requests := &atomic.Int32{}, &atomic.Int32{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			requests.Add(1)
			rpc.ServeHTTP(w, r)
			return
		}
		if r.URL.Path != "/connect/token" {
			t.Error("unexpected HTTP route", r.URL.Path)
		}
		tokens.Add(1)
		_, _ = w.Write([]byte(`{"access_token":"token","expires_in":3600}`))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(func() { rpc.Stop(); server.Close() })
	host, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	return kernel, &net.SRV{Target: host, Port: uint16(number)}, tokens, requests
}

func TestSRVReconnectRefreshesDiscoveryAndOAuthAuthority(t *testing.T) {
	first, address1, tokens1 := discoveryServer(t)
	second, address2, tokens2 := discoveryServer(t)
	resolver := &rotatingResolver{first: address1, second: address2}
	registry := NewRegistry()
	if _, err := RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithConnectionString("chronicle+srv://cluster"), WithSRVResolver(resolver), WithDevelopmentDefaults(), WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err = client.EventStore(ctx, "store"); err != nil {
		t.Fatal(err)
	}
	first.endStream <- status.Error(codes.Unavailable, "move endpoint")
	awaitSignal(t, ctx, second.registered)
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if resolver.calls.Load() != 2 || tokens1.Load() != 1 || tokens2.Load() != 1 {
		t.Fatalf("discovery=%d tokens=%d/%d", resolver.calls.Load(), tokens1.Load(), tokens2.Load())
	}
}

func TestSilentKeepAliveReplacesGeneration(t *testing.T) {
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel, WithKeepAliveTimeout(200*time.Millisecond))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, kernel.registered)
	// The fake server deliberately leaves a healthy HTTP/2 stream silent.
	awaitSignal(t, ctx, kernel.registered)
	after, err := store.WaitForRegistration(ctx)
	if err != nil || after.Generation <= before.Generation {
		t.Fatalf("stale generation survived: %+v %v", after, err)
	}
}
