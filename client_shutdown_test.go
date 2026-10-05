// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockingTokenKey struct{}
type blockingTokenSource struct {
	entered chan struct{}
	release chan struct{}
}

func (s *blockingTokenSource) Token(ctx context.Context) (Token, error) {
	if ctx.Value(blockingTokenKey{}) != nil {
		close(s.entered)
		<-s.release
	}
	return Token{AccessToken: "valid"}, nil
}

func TestCloseContextReportsIncompleteCallbackAndLaterJoins(t *testing.T) {
	source := &blockingTokenSource{make(chan struct{}), make(chan struct{})}
	client, ctx := supervisionClient(t, &supervisedKernel{}, WithTokenSource(source))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() {
		_, err := store.EventLog().Append(context.WithValue(ctx, blockingTokenKey{}, true), "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		completed <- err
	}()
	awaitSignal(t, ctx, source.entered)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = client.CloseContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("incomplete cleanup claimed success: %v", err)
	}
	select {
	case <-client.closeDone:
		t.Fatal("uncooperative callback was not joined")
	default:
	}
	close(source.release)
	if err = client.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-completed; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestGenerationReplacementJoinsAdmittedCallbacks(t *testing.T) {
	source := &blockingTokenSource{make(chan struct{}), make(chan struct{})}
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel, WithTokenSource(source))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	old := client.current
	client.mu.Unlock()
	completed := make(chan error, 1)
	go func() {
		_, err := store.EventLog().Append(context.WithValue(ctx, blockingTokenKey{}, true), "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		completed <- err
	}()
	awaitSignal(t, ctx, source.entered)
	kernel.endStream <- status.Error(codes.Unavailable, "lost")
	awaitSignal(t, ctx, old.ctx.Done())
	// A canceled but unjoined callback keeps the old generation alive. No newer
	// transport can be published until it releases its admission lease.
	client.mu.Lock()
	current := client.current
	client.mu.Unlock()
	if current != nil && current != old {
		t.Fatal("replacement raced old worker")
	}
	close(source.release)
	if err = <-completed; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	current = client.current
	client.mu.Unlock()
	if current == old {
		t.Fatal("generation did not change")
	}
}

func TestShutdownDrainsAdmittedAppend(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{appendCall: func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	client, ctx := supervisionClient(t, kernel)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() {
		_, err := store.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		completed <- err
	}()
	awaitSignal(t, ctx, entered)
	shutdown := make(chan error, 1)
	client.mu.Lock()
	changed := client.changed
	client.mu.Unlock()
	go func() { shutdown <- client.Shutdown(ctx) }()
	awaitSignal(t, ctx, changed)
	if err = client.Connect(ctx); !errors.Is(err, ErrClosed) {
		t.Fatal("shutdown admitted new work", err)
	}
	close(release)
	if err = <-completed; err != nil {
		t.Fatal("graceful shutdown canceled admitted append", err)
	}
	if err = <-shutdown; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadlineCancelsRemainingAppend(t *testing.T) {
	entered := make(chan struct{})
	kernel := &supervisedKernel{appendCall: func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }}
	client, ctx := supervisionClient(t, kernel)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() {
		_, err := store.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		completed <- err
	}()
	awaitSignal(t, ctx, entered)
	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	if err = client.Shutdown(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	var unknown *eventsequences.OutcomeUnknownError
	if err = <-completed; !errors.As(err, &unknown) {
		t.Fatal(err)
	}
	if err = client.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
}
