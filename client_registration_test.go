// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFailedPassReportsProgressAndRetriesInBackground(t *testing.T) {
	var attempts atomic.Int32
	kernel := &supervisedKernel{register: func(context.Context) error {
		if attempts.Add(1) == 1 {
			return status.Error(codes.Unavailable, "busy")
		}
		return nil
	}}
	client, ctx := supervisionClient(t, kernel, WithRegistrationRetry(RegistrationRetry{MaxAttempts: 1, InitialDelay: time.Millisecond, MaximumDelay: 20 * time.Millisecond, AttemptTimeout: time.Second}))
	_, err := client.EventStore(ctx, "store")
	var registration *RegistrationError
	if !errors.As(err, &registration) || !registration.Outcome.RetryPending || registration.Outcome.Attempts != 1 || len(registration.Outcome.Artifacts) != 3 || registration.Outcome.Artifacts[0].Failure != nil {
		t.Fatalf("lost partial outcome: %v", err)
	}
	// No caller retries registration; the owned worker must recover the cached handle.
	awaitSignal(t, ctx, kernel.registered)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := store.WaitForRegistration(ctx)
	if err != nil || !outcome.IsSuccess() || outcome.Pass != 2 || outcome.Generation == 0 || kernel.registrations.Load() != 2 {
		t.Fatalf("retry: %+v %v", outcome, err)
	}
}

func TestReplayBarrierBlocksAppendUntilAcknowledged(t *testing.T) {
	var attempts atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{register: func(ctx context.Context) error {
		if attempts.Add(1) > 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	client, ctx := supervisionClient(t, kernel)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	kernel.endStream <- status.Error(codes.Unavailable, "reconnect")
	awaitSignal(t, ctx, entered)
	// Connection preflight has completed, but no registration acknowledgement yet.
	client.mu.Lock()
	g := client.current
	client.mu.Unlock()
	if g == nil {
		t.Fatal("registration conflated with connection construction")
	}
	appendCtx, cancel := context.WithCancel(ctx)
	completed := make(chan error, 1)
	go func() {
		_, err := store.EventLog().Append(appendCtx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		completed <- err
	}()
	cancel()
	if err = <-completed; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if kernel.appends.Load() != 0 {
		t.Fatal("append bypassed registration barrier")
	}
	close(release)
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrationRetryAndDiscoveryOptionsValidate(t *testing.T) {
	for _, option := range []ClientOption{WithSRVResolver(nil), WithKeepAliveTimeout(0), WithRegistrationRetry(RegistrationRetry{}), WithRegistrationRetry(RegistrationRetry{MaxAttempts: 2, InitialDelay: time.Second, MaximumDelay: time.Millisecond, AttemptTimeout: time.Second})} {
		client, err := NewClient(option)
		if err == nil {
			_ = client.Close()
			t.Fatal("invalid option accepted")
		}
		if !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatal(err)
		}
	}
	for _, uri := range []string{"chronicle://one,two?loadBalancer=round-robin", "chronicle+srv://cluster?srvNameServer=127.0.0.1:53"} {
		client, err := NewClient(WithConnectionString(uri))
		if err != nil {
			t.Fatal(err)
		}
		if err = client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
