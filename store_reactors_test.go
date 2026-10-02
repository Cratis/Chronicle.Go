// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type openingReactorServer struct {
	contracts.UnimplementedReactorsServer
	registered chan *contracts.RegisterReactor
}

func (s *openingReactorServer) Observe(stream grpc.BidiStreamingServer[contracts.ReactorMessage, contracts.EventsToObserve]) error {
	message, err := stream.Recv()
	if err != nil {
		return err
	}
	select {
	case s.registered <- message.Content.Value0:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func openingReactorClient(t *testing.T, interceptor grpc.StreamClientInterceptor, options ...ClientOption) (*Client, context.Context, *openingReactorServer) {
	t.Helper()
	registry := NewRegistry()
	if _, err := RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactorHandler(registry, "opening", func(context.Context, lifecycleEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	server := &openingReactorServer{registered: make(chan *contracts.RegisterReactor, 8)}
	options = append(options, WithRegistry(registry))
	client, ctx := supervisionClient(t, &supervisedKernel{reactors: server, streamInterceptor: interceptor}, options...)
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	return client, ctx, server
}

func TestReactorOpenOutlivesCallerAndAppendDeadline(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	client, ctx, server := openingReactorClient(t, func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method == contracts.Reactors_Observe_FullMethodName {
			entered <- ctx
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
			}
		}
		return streamer(ctx, desc, conn, method, opts...)
	})
	caller, cancel := context.WithCancel(ctx)
	defer cancel()
	ready := make(chan error, 1)
	go func() { _, err := client.EventStore(caller, "store"); ready <- err }()
	openCtx := receiveOpening(t, ctx, entered)
	client.mu.Lock()
	store := client.stores[storeKey{name: "store", namespace: DefaultNamespace}]
	generation := client.current
	client.mu.Unlock()
	cancel()
	if err := receiveOpening(t, ctx, ready); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	deadline, expire := context.WithTimeout(ctx, 10*time.Millisecond)
	defer expire()
	if _, err := store.EventLog().Append(deadline, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("append readiness did not respect deadline: %v", err)
	}
	if openCtx.Err() != nil || generation.ctx.Err() != nil {
		t.Fatal("caller cancellation ended the observer or shared generation")
	}
	close(release)
	registration := receiveOpening(t, ctx, server.registered)
	if registration.ConnectionId != generation.id {
		t.Fatal("Open changed connection generation")
	}
	result, err := store.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil || result.Err() != nil {
		t.Fatal(err, result.Err())
	}
}

func TestReactorUnregisterDuringOpenDoesNotPoisonRegistration(t *testing.T) {
	entered := make(chan struct{})
	client, ctx, _ := openingReactorClient(t, func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method == contracts.Reactors_Observe_FullMethodName {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return streamer(ctx, desc, conn, method, opts...)
	})
	ready := make(chan error, 1)
	go func() { _, err := client.EventStore(ctx, "store"); ready <- err }()
	awaitSignal(t, ctx, entered)
	client.mu.Lock()
	store := client.stores[storeKey{name: "store", namespace: DefaultNamespace}]
	generation := client.current
	client.mu.Unlock()
	if err := store.UnregisterReactor(ctx, "opening"); err != nil {
		t.Fatal(err)
	}
	if err := receiveOpening(t, ctx, ready); err != nil {
		t.Fatal(err)
	}
	if generation.ctx.Err() != nil {
		t.Fatal("unregister canceled the generation")
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := store.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil || result.Err() != nil {
		t.Fatal(err, result.Err())
	}
}

func TestReactorOpenFailureRetriesOnSameGeneration(t *testing.T) {
	var attempts atomic.Int32
	waiting, resume := make(chan struct{}, 1), make(chan struct{})
	client, ctx, server := openingReactorClient(t, func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method == contracts.Reactors_Observe_FullMethodName && attempts.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "opening failed")
		}
		return streamer(ctx, desc, conn, method, opts...)
	}, WithReactorRetryWaitForTest(func(ctx context.Context, delay time.Duration) error {
		if delay != 2*time.Second {
			t.Errorf("delay = %v", delay)
		}
		waiting <- struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resume:
			return nil
		}
	}))
	client.mu.Lock()
	generation := client.current
	client.mu.Unlock()
	ready := make(chan error, 1)
	go func() { _, err := client.EventStore(ctx, "store"); ready <- err }()
	awaitSignal(t, ctx, waiting)
	if generation.ctx.Err() != nil {
		t.Fatal("Open failure canceled the generation")
	}
	close(resume)
	registration := receiveOpening(t, ctx, server.registered)
	if err := receiveOpening(t, ctx, ready); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 || registration.ConnectionId != generation.id {
		t.Fatal("Open did not retry on the same generation")
	}
}

func receiveOpening[T any](t *testing.T, ctx context.Context, channel <-chan T) T {
	t.Helper()
	select {
	case result := <-channel:
		return result
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}

func TestRegistryMiddlewareValidationAndStoreIsolation(t *testing.T) {
	if err := RegisterReactorMiddleware(nil, func() {}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := RegisterReactorMiddleware(registry, nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if _, err := RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactorHandler(registry, "opening", func(context.Context, lifecycleEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactorMiddleware(registry, func() string { return "invalid" }); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(WithRegistry(registry)); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid middleware factory accepted: %v", err)
	}
	// The default registry's middleware must not bleed into a store override.
	empty := NewRegistry()
	if err := RegisterReactorMiddleware(empty, func() string { return "unused" }); err != nil {
		t.Fatal(err)
	}
	local := NewRegistry()
	if _, err := RegisterEvent[lifecycleEvent](local); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactorHandlers(local, "local", []reactors.Handler{reactors.On(func(context.Context, lifecycleEvent) error { return nil })}); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(empty), WithRegistryForStore("local", local))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
