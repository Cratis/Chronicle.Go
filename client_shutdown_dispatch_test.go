// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc"
)

// delayedShutdownContext holds AfterFunc delivery after its parent is canceled.
// This makes the asynchronous generation-to-call cancellation gap deterministic,
// rather than depending on whether its goroutine runs before a token returns.
// Hiding Value prevents context's private cancelCtx optimization from bypassing
// this test-owned AfterFunc implementation.
type delayedShutdownContext struct {
	context.Context
	release chan struct{}
	done    chan struct{}
}

func (*delayedShutdownContext) Value(any) any { return nil }
func (c *delayedShutdownContext) AfterFunc(callback func()) func() bool {
	var active atomic.Bool
	active.Store(true)
	go func() {
		defer close(c.done)
		<-c.Done()
		<-c.release
		if active.CompareAndSwap(true, false) {
			callback()
		}
	}()
	return func() bool { return active.CompareAndSwap(true, false) }
}

type shutdownDispatchConn struct {
	grpc.ClientConnInterface
	calls atomic.Int32
}

func (c *shutdownDispatchConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	c.calls.Add(1)
	return nil
}

func TestCloseContextRejectsDispatchBeforeAsyncCallCancellation(t *testing.T) {
	client, err := NewClient(WithNoAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	generationCtx, cancelGeneration := context.WithCancel(client.life)
	bridge := &delayedShutdownContext{Context: generationCtx, release: make(chan struct{}), done: make(chan struct{})}
	conn := &shutdownDispatchConn{}
	source := &blockingTokenSource{entered: make(chan struct{}), release: make(chan struct{})}
	g := &generation{ctx: bridge, cancel: cancelGeneration, raw: conn, tokens: source}
	g.transport = &generationTransport{generation: g}
	client.current = g // No supervisor or other generation readers in this fixture.
	releaseToken := sync.OnceFunc(func() { close(source.release) })
	releaseBridge := sync.OnceFunc(func() { close(bridge.release) })
	t.Cleanup(func() {
		releaseToken()
		releaseBridge()
		if err := client.Close(); err != nil {
			t.Error(err)
		}
		<-bridge.done
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		completed <- (&clientTransport{client: client}).Invoke(context.WithValue(ctx, blockingTokenKey{}, true), "/test/append", nil, nil)
	}()
	awaitSignal(t, ctx, source.entered)
	canceled, cancelClose := context.WithCancel(ctx)
	cancelClose()
	if err := client.CloseContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("incomplete shutdown = %v, want cancellation", err)
	}
	if g.ctx.Err() != context.Canceled {
		t.Fatal("shutdown did not cancel generation")
	}
	select {
	case <-client.closeDone:
		t.Fatal("shutdown joined a still-blocked token callback")
	default:
	}
	// The generation is canceled, but its AfterFunc has deliberately not run.
	// Returning from caller code must not dispatch onto that retired generation.
	releaseToken()
	select {
	case err := <-completed:
		var before *faults.BeforeDispatch
		if !errors.Is(err, context.Canceled) || !errors.As(err, &before) {
			t.Errorf("call after generation cancellation = %v, want pre-dispatch cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if conn.calls.Load() != 0 {
		t.Errorf("canceled generation dispatched %d RPCs", conn.calls.Load())
	}
	if err := client.CloseContext(ctx); err != nil {
		t.Fatal("later shutdown did not join released callback", err)
	}
}
