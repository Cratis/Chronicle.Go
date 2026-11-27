// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
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
	return discoveryServerWithCount(t, "0")
}

func discoveryServerWithCount(t *testing.T, count string, options ...grpc.ServerOption) (*supervisedKernel, *net.SRV, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	kernel := &supervisedKernel{lifecycleKernel: lifecycleKernel{endStream: make(chan error, 1)}, registered: make(chan struct{}, 10), namespaceCounts: make(map[string]int)}
	rpc := grpc.NewServer(options...)
	clients.RegisterConnectionServiceServer(rpc, kernel)
	eventstores.RegisterEventStoresServer(rpc, kernel)
	eventtypes.RegisterEventTypesServer(rpc, kernel)
	namespaces.RegisterNamespacesServer(rpc, kernel)
	tokens, requests := &atomic.Int32{}, &atomic.Int32{}
	mux := discoveryHTTPRoutes(t, count, tokens)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			requests.Add(1)
			rpc.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
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

func discoveryHTTPRoutes(t *testing.T, count string, tokens *atomic.Int32) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /connections/count", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(count))
	})
	mux.HandleFunc("POST /connections/reserve", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/connect/token", func(w http.ResponseWriter, _ *http.Request) {
		tokens.Add(1)
		_, _ = w.Write([]byte(`{"access_token":"token","expires_in":3600}`))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Error("unexpected HTTP route", r.URL.Path)
		http.NotFound(w, r)
	})
	return mux
}

func builtinReconnectStore(t *testing.T, first, second *net.SRV) (*EventStore, context.Context) {
	t.Helper()
	uri := "chronicle://" + discoveryAddress(first).String() + "," + discoveryAddress(second).String()
	client, err := NewClient(WithConnectionString(uri), WithDevelopmentDefaults(), WithRegistry(balancerRegistry(t)), WithKeepAliveTimeout(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	return store, ctx
}

func reconnectRegistration(t *testing.T, ctx context.Context, store *EventStore) RegistrationOutcome {
	t.Helper()
	outcome, err := store.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}

func TestBuiltinReconnectPrefersPeer(t *testing.T) {
	first, address1, tokens1, _ := discoveryServerWithCount(t, "0")
	second, address2, tokens2, _ := discoveryServerWithCount(t, "5")
	store, ctx := builtinReconnectStore(t, address1, address2)
	before := reconnectRegistration(t, ctx, store)
	awaitSignal(t, ctx, first.registered)
	first.endStream <- status.Error(codes.Unavailable, "drop established session")
	select {
	case <-first.registered:
		t.Fatal("reconnect replayed dropped endpoint A before trying a peer")
	case <-second.registered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if after := reconnectRegistration(t, ctx, store); after.Generation <= before.Generation {
		t.Fatalf("registration generation did not advance from %d: %+v", before.Generation, after)
	}
	if first.registrations.Load() != 1 || second.registrations.Load() != 1 {
		t.Fatalf("registrations A/B = %d/%d, want 1/1", first.registrations.Load(), second.registrations.Load())
	}
	if tokens1.Load() != 1 || tokens2.Load() != 1 {
		t.Fatalf("OAuth exchanges A/B = %d/%d, want 1/1", tokens1.Load(), tokens2.Load())
	}
}

func TestBuiltinReconnectRestoresDroppedCandidate(t *testing.T) {
	first, address1, _, _ := discoveryServerWithCount(t, "0")
	var peerAttempts atomic.Int32
	failConnect := grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if info.FullMethod == clients.ConnectionService_Connect_FullMethodName {
			peerAttempts.Add(1)
			return status.Error(codes.Unavailable, "peer failed before sending a keepalive")
		}
		return next(srv, stream)
	})
	second, address2, _, _ := discoveryServerWithCount(t, "5", failConnect)
	store, ctx := builtinReconnectStore(t, address1, address2)
	before := reconnectRegistration(t, ctx, store)
	awaitSignal(t, ctx, first.registered)
	first.endStream <- status.Error(codes.Unavailable, "drop established session")
	awaitSignal(t, ctx, first.registered)
	if peerAttempts.Load() != 1 {
		t.Fatalf("replayed A after %d peer handshake attempts, want 1", peerAttempts.Load())
	}
	if after := reconnectRegistration(t, ctx, store); after.Generation <= before.Generation {
		t.Fatalf("registration generation did not advance from %d: %+v", before.Generation, after)
	}
	if second.registrations.Load() != 0 {
		t.Fatalf("failed peer registered %d times", second.registrations.Load())
	}
}

func TestBuiltinReconnectPeerCandidates(t *testing.T) {
	for _, tc := range []struct {
		name, dropped string
		addresses     []string
		want          []string
	}{
		{name: "no hint", addresses: []string{"a", "b"}, want: []string{"a", "b"}},
		{name: "absent from refreshed pool", dropped: "a", addresses: []string{"b", "c"}, want: []string{"b", "c"}},
		{name: "single endpoint", dropped: "a", addresses: []string{"a"}, want: []string{"a"}},
		{name: "only duplicates", dropped: "a", addresses: []string{"a", "a"}, want: []string{"a", "a"}},
		{name: "all dropped matches removed", dropped: "a", addresses: []string{"a", "b", "a", "c"}, want: []string{"b", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := slices.Clone(tc.addresses)
			if got := preferReconnectPeers(tc.addresses, tc.dropped); !slices.Equal(got, tc.want) {
				t.Fatalf("candidates = %v, want %v", got, tc.want)
			}
			if !slices.Equal(tc.addresses, original) {
				t.Fatalf("input candidates mutated to %v, want %v", tc.addresses, original)
			}
		})
	}
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
