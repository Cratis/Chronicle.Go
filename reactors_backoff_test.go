// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/reactors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordingRetryClock reports every retry delay and lets the test release it.
func recordingRetryClock() (<-chan time.Duration, chan<- struct{}, chronicle.ClientOption) {
	delays, resume := make(chan time.Duration, 8), make(chan struct{}, 8)
	return delays, resume, chronicle.WithObserverRetryWaitForTest(func(ctx context.Context, delay time.Duration) error {
		delays <- delay
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resume:
			return nil
		}
	})
}

func TestReactorStreamBackoffGrowsAndResetsAfterDelivery(t *testing.T) {
	registry := reactorRegistry(t)
	if err := chronicle.RegisterReactorHandler(registry, "backing-off", func(context.Context, ReactorInput) error { return nil }); err != nil {
		t.Fatal(err)
	}
	delays, resume, clock := recordingRetryClock()
	kernel := &reactorKernel{}
	_, _, ctx := reactorClient(t, kernel, registry, clock)

	endAndExpect := func(session *observerSession, end error, want time.Duration) {
		t.Helper()
		session.end <- end
		if delay := receive(t, ctx, delays); delay != want {
			t.Fatalf("retry delay = %s, want %s", delay, want)
		}
		resume <- struct{}{}
	}
	endAndExpect(receive(t, ctx, kernel.sessions), nil, 2*time.Second)
	endAndExpect(receive(t, ctx, kernel.sessions), status.Error(codes.Unavailable, "busy"), 4*time.Second)
	endAndExpect(receive(t, ctx, kernel.sessions), nil, 8*time.Second)

	delivering := receive(t, ctx, kernel.sessions)
	delivering.batches <- batch(1)
	if result := receive(t, ctx, delivering.results); result.GetState() != contracts.ObservationState_Success {
		t.Fatalf("delivery result = %v", result)
	}
	endAndExpect(delivering, nil, 2*time.Second)
}

func TestReactorSessionLostReconnectsOnceUnderNewConnectionID(t *testing.T) {
	registry := reactorRegistry(t)
	for _, id := range []reactors.ID{"first", "second"} {
		if err := chronicle.RegisterReactorHandler(registry, id, func(context.Context, ReactorInput) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	delays, _, clock := recordingRetryClock()
	disconnected := make(chan error, 4)
	kernel := &reactorKernel{}
	client, _, ctx := reactorClient(t, kernel, registry, clock, chronicle.WithOnDisconnected(func(_ context.Context, event chronicle.ConnectionEvent) {
		disconnected <- event.Err
	}))
	old := []*observerSession{receive(t, ctx, kernel.sessions), receive(t, ctx, kernel.sessions)}
	for _, session := range old {
		session.end <- status.Error(codes.Internal, "Observer subscription failed: ClientIsNotConnected")
	}
	if err := receive(t, ctx, disconnected); !errors.Is(err, chronicle.ErrSessionLost) {
		t.Fatalf("disconnected with %v, want ErrSessionLost", err)
	}
	renewed := []*observerSession{receive(t, ctx, kernel.sessions), receive(t, ctx, kernel.sessions)}
	for _, session := range renewed {
		if session.registration.ConnectionId == old[0].registration.ConnectionId {
			t.Fatalf("reactor reregistered under the lost connection ID %q", session.registration.ConnectionId)
		}
		if session.registration.ConnectionId != renewed[0].registration.ConnectionId {
			t.Fatal("reactors reregistered under different connection IDs")
		}
	}
	// Close joins every worker of both connections, so a stale retry or a second
	// reconnect would show by now.
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if calls := kernel.connectCalls.Load(); calls != 2 {
		t.Fatalf("connect calls = %d, want one reconnect", calls)
	}
	select {
	case delay := <-delays:
		t.Fatalf("a lost session was retried on the stale connection after %s", delay)
	default:
	}
}

func TestReactorSessionLostWhileHandlingReconnectsWithoutRetrying(t *testing.T) {
	registry := reactorRegistry(t)
	handling, release := make(chan struct{}, 1), make(chan struct{})
	if err := chronicle.RegisterReactorHandler(registry, "rejected", func(context.Context, ReactorInput) error {
		handling <- struct{}{}
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	delays, _, clock := recordingRetryClock()
	disconnected := make(chan error, 4)
	kernel := &reactorKernel{}
	client, _, ctx := reactorClient(t, kernel, registry, clock, chronicle.WithOnDisconnected(func(_ context.Context, event chronicle.ConnectionEvent) {
		disconnected <- event.Err
	}))
	old := receive(t, ctx, kernel.sessions)
	old.rejectAfter <- rejection{batch: batch(1), err: status.Error(codes.Internal, "Observer subscription failed: ClientIsNotConnected")}
	receive(t, ctx, handling)
	receive(t, ctx, old.done)
	close(release)
	if err := receive(t, ctx, disconnected); !errors.Is(err, chronicle.ErrSessionLost) {
		t.Fatalf("disconnected with %v, want ErrSessionLost", err)
	}
	if renewed := receive(t, ctx, kernel.sessions); renewed.registration.ConnectionId == old.registration.ConnectionId {
		t.Fatalf("reactor reregistered under the lost connection ID %q", renewed.registration.ConnectionId)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case delay := <-delays:
		t.Fatalf("a lost session was retried on the stale connection after %s", delay)
	default:
	}
}
