// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type supervisedKernel struct {
	lifecycleKernel
	eventstores.UnimplementedEventStoresServer
	eventtypes.UnimplementedEventTypesServer
	namespaces.UnimplementedNamespacesServer
	registrations   atomic.Int32
	registered      chan struct{}
	register        func(context.Context) error
	appendCall      func(context.Context) error
	mu              sync.Mutex
	namespaceCounts map[string]int
}

func (*supervisedKernel) EnsureEventStore(context.Context, *eventstores.EnsureEventStoreRequest) (*eventstores.CommandResult, error) {
	return &eventstores.CommandResult{IsAuthorized: true}, nil
}
func (k *supervisedKernel) EnsureNamespace(_ context.Context, r *namespaces.EnsureNamespaceRequest) (*namespaces.CommandResult, error) {
	k.mu.Lock()
	k.namespaceCounts[r.Namespace]++
	k.mu.Unlock()
	return &namespaces.CommandResult{IsAuthorized: true}, nil
}
func (k *supervisedKernel) RegisterEventTypes(ctx context.Context, _ *eventtypes.RegisterEventTypesRequest) (*eventtypes.CommandResult, error) {
	k.registrations.Add(1)
	if k.register != nil {
		if err := k.register(ctx); err != nil {
			return nil, err
		}
	}
	k.registered <- struct{}{}
	return &eventtypes.CommandResult{IsAuthorized: true}, nil
}
func (k *supervisedKernel) Append(ctx context.Context, r *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
	k.appends.Add(1)
	if k.appendCall != nil {
		if err := k.appendCall(ctx); err != nil {
			return nil, err
		}
	}
	return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, CorrelationId: r.CorrelationId}}, nil
}

func supervisionClient(t *testing.T, k *supervisedKernel, options ...ClientOption) (*Client, context.Context) {
	t.Helper()
	k.endStream = make(chan error, 1)
	k.registered = make(chan struct{}, 20)
	k.namespaceCounts = make(map[string]int)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clients.RegisterConnectionServiceServer(server, k)
	eventstores.RegisterEventStoresServer(server, k)
	eventtypes.RegisterEventTypesServer(server, k)
	namespaces.RegisterNamespacesServer(server, k)
	sequences.RegisterEventSequencesServer(server, k)
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := server.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///supervision", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		server.Stop()
		_ = listener.Close()
		<-served
	})
	registry := NewRegistry()
	if _, err = RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	base := []ClientOption{WithGRPCConnection(conn), WithConnectionString("chronicle://test"), WithRegistry(registry), WithTokenSource(&invalidatingSource{})}
	client, err := NewClient(append(base, options...)...)
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
	return client, ctx
}

func awaitSignal(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestAutomaticReplaySharesStoreDefinitionsAndSeparatesNamespaces(t *testing.T) {
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel)
	first, err := client.EventStore(ctx, "store", WithNamespace("one"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.EventStore(ctx, "store", WithNamespace("two"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := first.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, kernel.registered)
	if kernel.registrations.Load() != 1 {
		t.Fatal("definitions registered once per namespace")
	}
	client.mu.Lock()
	old := client.current
	client.mu.Unlock()
	kernel.endStream <- status.Error(codes.Unavailable, "restart")
	awaitSignal(t, ctx, old.ctx.Done())
	// No Connect/Ready call drives this replay: the supervisor owns it.
	awaitSignal(t, ctx, kernel.registered)
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := second.WaitForRegistration(ctx)
	if err != nil || !after.IsSuccess() || after.Generation <= before.Generation || kernel.registrations.Load() != 2 {
		t.Fatalf("replay: %+v %v", after, err)
	}
	kernel.mu.Lock()
	defer kernel.mu.Unlock()
	if kernel.namespaceCounts["one"] != 2 || kernel.namespaceCounts["two"] != 2 {
		t.Fatal(kernel.namespaceCounts)
	}
}

func TestLostGenerationDoesNotReplayInflightAppend(t *testing.T) {
	entered := make(chan struct{})
	kernel := &supervisedKernel{appendCall: func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }}
	client, ctx := supervisionClient(t, kernel)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := store.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		result <- err
	}()
	awaitSignal(t, ctx, entered)
	kernel.endStream <- status.Error(codes.Unavailable, "lost")
	var unknown *eventsequences.OutcomeUnknownError
	select {
	case err = <-result:
		if !errors.As(err, &unknown) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if kernel.appends.Load() != 1 {
		t.Fatal("ambiguous append replayed")
	}
}
