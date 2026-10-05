// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"net"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
	constraints "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	contracts "github.com/cratis/chronicle.go/contracts/patterns"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/patterns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type patternKernel struct {
	contracts.UnimplementedPatternsServer
	kernel *fakeKernel
	match  func(context.Context, *contracts.MatchingPatternsRequest) (*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse, error)
}

func (k *patternKernel) MatchingPatterns(ctx context.Context, request *contracts.MatchingPatternsRequest) (*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse, error) {
	return k.match(ctx, request)
}

type patternTokens struct{}

func (patternTokens) Token(context.Context) (chronicle.Token, error) {
	return chronicle.Token{AccessToken: "test-pattern-token"}, nil
}

func patternClient(t *testing.T, k *patternKernel) (*chronicle.Client, *grpc.ClientConn) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clients.RegisterConnectionServiceServer(server, k.kernel)
	eventstores.RegisterEventStoresServer(server, k.kernel)
	namespaces.RegisterNamespacesServer(server, k.kernel)
	eventtypes.RegisterEventTypesServer(server, k.kernel)
	constraints.RegisterConstraintsServer(server, k.kernel)
	contracts.RegisterPatternsServer(server, k)
	done := make(chan struct{})
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	go serveKernelFixture(server, listener, done, t.Error)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CustomerRegistered](registry, events.WithID("patterns-registration")); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithGRPCConnection(conn), chronicle.WithTokenSource(patternTokens{}), chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client, conn
}

func TestStorePatternsCoordinatesMetadataAndNoCrossStoreCache(t *testing.T) {
	kernel := &fakeKernel{}
	calls := make(chan string, 8)
	id, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	k := &patternKernel{kernel: kernel, match: func(ctx context.Context, r *contracts.MatchingPatternsRequest) (*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse, error) {
		if kernel.registrations.Load() == 0 {
			t.Error("patterns bypassed registration")
		}
		md, _ := grpcmetadata.FromIncomingContext(ctx)
		if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer test-pattern-token" {
			t.Errorf("authentication=%v", got)
		}
		if got := md.Get("x-correlation-id"); len(got) != 1 || got[0] != id.String() {
			t.Errorf("correlation=%v", got)
		}
		if r.Context["InitiatorId"] != "actor-query-fact" {
			t.Error("actor facet changed")
		}
		coordinate := r.EventStore + "/" + r.Namespace
		calls <- coordinate
		return &contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse{IsAuthorized: true, Data: []*contracts.BehaviorPatternDetailsResponse{{Id: coordinate, GroupingKey: r.GroupingKey, FirstSeen: &contracts.SerializableDateTimeOffset{Value: "2024-01-15T00:00:00.0000000+00:00"}, LastSeen: &contracts.SerializableDateTimeOffset{Value: "2024-01-15T00:00:00.0000000+00:00"}}}}, nil
	}}
	client, conn := patternClient(t, k)
	ctx := metadata.WithCorrelation(testContext(t), id)
	facets := patterns.NewFacetSet(map[patterns.FacetName]patterns.FacetValue{patterns.InitiatorID: "actor-query-fact"})
	for _, coordinate := range []struct {
		store chronicle.StoreName
		ns    chronicle.Namespace
	}{{"one", "a"}, {"one", "b"}, {"two", "a"}, {"one", "a"}} {
		store, err := client.EventStore(ctx, coordinate.store, chronicle.WithNamespace(coordinate.ns))
		if err != nil {
			t.Fatal(err)
		}
		s := store.Patterns()
		if s.Store() != coordinate.store || s.Namespace() != coordinate.ns {
			t.Fatal("facade bound incorrectly")
		}
		for range 2 {
			result, err := s.GetPatterns(ctx, "same-scope", facets, patterns.QueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := string(coordinate.store) + "/" + string(coordinate.ns)
			if <-calls != want || len(result.Data) != 1 || result.Data[0].ID != want {
				t.Fatal("cached or foreign response", result)
			}
		}
	}
	store, err := client.EventStore(ctx, "one", chronicle.WithNamespace("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Patterns().GetPatterns(ctx, "scope", facets, patterns.QueryOptions{}); !errors.Is(err, chronicle.ErrClosed) {
		t.Fatalf("closed client=%v", err)
	}
	// Closing the facade's owner must not close its borrowed raw connection.
	if _, err := clients.NewConnectionServiceClient(conn).CheckCompatibility(ctx, &clients.CompatibilityRequest{}); err != nil {
		t.Fatal("borrowed connection closed", err)
	}
}

func TestStorePatternsGenerationCancellationJoinsInflightQuery(t *testing.T) {
	entered := make(chan struct{})
	k := &patternKernel{kernel: &fakeKernel{}, match: func(ctx context.Context, _ *contracts.MatchingPatternsRequest) (*contracts.QueryResult_IEnumerable_BehaviorPatternDetailsResponse, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	client, _ := patternClient(t, k)
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := store.Patterns().GetPatterns(ctx, "scope", patterns.FacetSet{}, patterns.QueryOptions{})
		result <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("generation cancellation=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("query did not terminate")
	}
}
