// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"strings"
	"sync"
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

type receivingReactorStream struct {
	grpc.ClientStream
	receiving chan struct{}
	once      sync.Once
}

func (s *receivingReactorStream) RecvMsg(message any) error {
	s.once.Do(func() { close(s.receiving) })
	return s.ClientStream.RecvMsg(message)
}

func TestReactorOpenFailureRetriesOnSameGeneration(t *testing.T) {
	var attempts atomic.Int32
	waiting, resume := make(chan struct{}, 1), make(chan struct{})
	receiving := make(chan struct{})
	openError := status.Error(codes.Unavailable, "opening failed")
	client, ctx, server := openingReactorClient(t, func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method != contracts.Reactors_Observe_FullMethodName {
			return streamer(ctx, desc, conn, method, opts...)
		}
		if attempts.Add(1) == 1 {
			return nil, openError
		}
		stream, err := streamer(ctx, desc, conn, method, opts...)
		if err != nil {
			return nil, err
		}
		return &receivingReactorStream{ClientStream: stream, receiving: receiving}, nil
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
	if err := receiveOpening(t, ctx, ready); !errors.Is(err, openError) {
		t.Fatalf("first Open failure was not reported: %v", err)
	}
	client.mu.Lock()
	store := client.stores[storeKey{name: "store", namespace: DefaultNamespace}]
	client.mu.Unlock()
	// No deadline: readiness must report the failed Open, not await recovery.
	outcome, err := store.WaitForRegistration(context.Background())
	var registrationError *RegistrationError
	if !errors.As(err, &registrationError) || !errors.Is(err, openError) || outcome.IsSuccess() || !outcome.RetryPending || !strings.Contains(outcome.Failure.Error(), `"opening"`) {
		t.Fatalf("missing reactor failure outcome: %+v %v", outcome, err)
	}
	if len(outcome.Artifacts) == 0 || !errors.Is(outcome.Artifacts[len(outcome.Artifacts)-1].Failure, openError) {
		t.Fatalf("missing failed artifact: %+v", outcome.Artifacts)
	}
	close(resume)
	registration := receiveOpening(t, ctx, server.registered)
	awaitSignal(t, ctx, receiving) // Run starts only after successful Open readiness.
	if attempts.Load() != 2 || registration.ConnectionId != generation.id {
		t.Fatal("Open did not retry on the same generation")
	}
	if outcome, err := store.WaitForRegistration(ctx); err != nil || !outcome.IsSuccess() {
		t.Fatalf("recovered Open poisoned later readiness: %+v %v", outcome, err)
	}
}

func TestReactorOpenDoesNotBlockBackgroundArtifactReplay(t *testing.T) {
	registry := NewRegistry()
	if _, err := RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactorHandler(registry, "blocked", func(context.Context, lifecycleEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var block atomic.Bool
	entered := make(chan struct{})
	kernel := &supervisedKernel{
		reactors: &openingReactorServer{registered: make(chan *contracts.RegisterReactor, 8)},
		streamInterceptor: func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			if method == contracts.Reactors_Observe_FullMethodName && block.Load() {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return streamer(ctx, desc, conn, method, opts...)
		},
	}
	client, ctx := supervisionClient(t, kernel, WithRegistryForStore("a", registry))
	for _, name := range []StoreName{"a", "b"} {
		if _, err := client.EventStore(ctx, name); err != nil {
			t.Fatal(err)
		}
		awaitSignal(t, ctx, kernel.registered)
	}
	block.Store(true)
	kernel.endStream <- status.Error(codes.Unavailable, "restart")
	awaitSignal(t, ctx, entered)
	// Both stores' definitions must replay while a's Open is still blocked.
	awaitSignal(t, ctx, kernel.registered)
	awaitSignal(t, ctx, kernel.registered)
	if kernel.registrations.Load() != 4 {
		t.Fatalf("artifact replay stopped at reactor readiness: %d", kernel.registrations.Load())
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
