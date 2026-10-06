// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConnectionHooksConcurrentEventSerialTransitionsAndCoalescing(t *testing.T) {
	notifications := make(chan hookNotification, 8)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	hook := func(ctx context.Context, event ConnectionEvent) {
		if event.Generation == 1 {
			entered <- struct{}{}
			<-release
		}
		notifications <- hookNotification{event, ctx}
	}
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel, WithKeepAliveTimeout(time.Minute), WithOnConnected(hook), WithOnConnected(hook),
		WithOnDisconnected(func(ctx context.Context, e ConnectionEvent) { notifications <- hookNotification{e, ctx} }))
	// Ensure release happens before the fixture's Close cleanup, even on failure.
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, entered)
	awaitSignal(t, ctx, entered) // Both hooks run concurrently.
	awaitSignal(t, ctx, kernel.registered)
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	} // Hooks do not gate Ready.
	for range 2 {
		kernel.endStream <- status.Error(codes.Unavailable, "restart")
		awaitSignal(t, ctx, kernel.registered) // Automatic reconnect, still blocked hooks.
	}
	outcome, err := store.WaitForRegistration(ctx)
	if err != nil || outcome.Generation != 3 {
		t.Fatalf("registration=%+v %v", outcome, err)
	}
	client.hooks.mu.Lock()
	pending := len(client.hooks.queue)
	client.hooks.mu.Unlock()
	if pending != 2 {
		t.Fatalf("pending=%d want D1,C3", pending)
	}
	select {
	case extra := <-notifications:
		t.Fatalf("serial dispatch violated: %+v", extra.event)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	for i, generation := range []uint64{1, 1, 1, 3, 3} {
		got := awaitHook(t, ctx, notifications)
		if got.event.Generation != generation {
			t.Fatalf("generation=%d want=%d", got.event.Generation, generation)
		}
		if (i == 2) != (got.event.Err != nil) {
			t.Fatalf("event %d has unexpected cause: %v", i, got.event.Err)
		}
		if i == 2 && status.Code(got.event.Err) != codes.Unavailable {
			t.Fatal(got.event.Err)
		}
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	last := awaitHook(t, ctx, notifications)
	if last.event.Generation != 3 || !errors.Is(last.event.Err, ErrClosed) {
		t.Fatalf("last=%+v", last.event)
	}
	select {
	case extra := <-notifications:
		t.Fatalf("extra=%+v", extra.event)
	default:
	}
}

func TestConnectionHooksCloseContextBoundsJoin(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	client, ctx := supervisionClient(t, &supervisedKernel{}, WithKeepAliveTimeout(time.Minute), WithOnConnected(func(context.Context, ConnectionEvent) { close(entered); <-release }))
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, entered)
	short, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	if err := client.CloseContext(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-client.closeDone:
		t.Fatal("Close did not join blocked hook")
	default:
	}
	once.Do(func() { close(release) })
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
