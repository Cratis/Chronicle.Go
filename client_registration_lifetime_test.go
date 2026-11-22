// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cratis/chronicle.go/contracts/namespaces"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Eviction prevents automatic replay from winning the race in the new generation:
// explicit use of the retained facade must start the next shared pass itself.
func reconnectedRegistrationStore(t *testing.T, invoke func(context.Context, string) error) (*Client, *EventStore, <-chan *lifecycleStream) {
	t.Helper()
	client, streams := memoryLifecycleClient(t, invoke, WithRegistrationRetry(RegistrationRetry{
		MaxAttempts: 1, InitialDelay: time.Second, MaximumDelay: time.Second, AttemptTimeout: 20 * time.Second,
	}))
	store, err := client.EventStore(t.Context(), "store")
	if err != nil {
		t.Fatal(err)
	}
	stream := <-streams
	evictStores(t, client)
	stream.end <- status.Error(codes.Unavailable, "reconnect")
	synctest.Wait()
	if err := client.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	return client, store, streams
}

func TestRegistrationPassCallerCancellationOnlyStopsWaiting(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				armed, calls := false, 0
				var passCtx context.Context
				client, store, _ := reconnectedRegistrationStore(t, func(ctx context.Context, method string) error {
					if !armed || method != namespaces.Namespaces_EnsureNamespace_FullMethodName {
						return nil
					}
					calls++
					passCtx = ctx
					close(entered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				armed = true
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				first := make(chan error, 1)
				go func() { _, err := store.WaitForRegistration(ctx); first <- err }()
				<-entered
				long := make(chan error, 1)
				go func() { long <- client.transport.Invoke(t.Context(), "/read", nil, nil) }()
				joinCtx, cancelJoin := context.WithCancel(t.Context())
				join := make(chan error, 1)
				go func() { _, err := store.WaitForRegistration(joinCtx); join <- err }()
				synctest.Wait()
				cancelJoin()
				if err := <-join; !errors.Is(err, context.Canceled) {
					t.Fatal("canceled joiner", err)
				}
				want := context.Canceled
				if deadline {
					time.Sleep(time.Second) // Advance fake time to the caller's deadline.
					want = context.DeadlineExceeded
				} else {
					cancel()
				}
				if err := <-first; !errors.Is(err, want) {
					t.Fatal("starter", err)
				}
				if passCtx.Err() != nil || calls != 1 {
					t.Fatal("caller changed owned pass", passCtx.Err(), calls)
				}
				close(release)
				if err := <-long; err != nil {
					t.Fatal("long operation", err)
				}
				outcome, err := store.WaitForRegistration(t.Context())
				if err != nil || !outcome.IsSuccess() || outcome.Generation < 2 || outcome.Pass != 1 || outcome.Attempts != 1 || calls != 1 {
					t.Fatal("shared acknowledgement", outcome, err, calls)
				}
			})
		})
	}
}

func TestRegistrationPassFailureIsSharedWithoutJoinerRetries(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "terminal", true: "attempt_timeout"}[timeout], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				armed, calls := false, 0
				failure := status.Error(codes.InvalidArgument, "registration refused")
				if timeout {
					failure = context.DeadlineExceeded
				}
				_, store, _ := reconnectedRegistrationStore(t, func(ctx context.Context, method string) error {
					if !armed || method != namespaces.Namespaces_EnsureNamespace_FullMethodName {
						return nil
					}
					calls++
					close(entered)
					if timeout {
						<-ctx.Done()
					} else {
						<-release
					}
					return failure
				})
				armed = true
				results := make(chan RegistrationOutcome, 3)
				wait := func() { outcome, _ := store.WaitForRegistration(t.Context()); results <- outcome }
				go wait()
				<-entered
				go wait()
				go wait()
				synctest.Wait()
				close(release)
				for range 3 {
					outcome := <-results
					if !errors.Is(outcome.Failure, failure) || !outcome.HasRun || outcome.Pass != 1 || outcome.Attempts != 1 || outcome.RetryPending != timeout {
						t.Fatal("shared failure", outcome)
					}
				}
				if calls != 1 {
					t.Fatal("waiters started extra attempts", calls)
				}
			})
		})
	}
}

func TestRegistrationPassRetirementCancelsAndJoins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		armed := false
		client, store, streams := reconnectedRegistrationStore(t, func(ctx context.Context, method string) error {
			if armed && method == namespaces.Namespaces_EnsureNamespace_FullMethodName {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release
				return ctx.Err()
			}
			return nil
		})
		armed = true
		result := make(chan error, 1)
		go func() { _, err := store.WaitForRegistration(t.Context()); result <- err }()
		<-entered
		stream := <-streams
		stream.end <- status.Error(codes.Unavailable, "retire blocked pass")
		<-canceled
		synctest.Wait()
		client.mu.Lock()
		current := client.current
		client.mu.Unlock()
		if current != nil {
			t.Fatal("new generation published before old registration joined")
		}
		armed = false
		close(release)
		// A public readiness wait follows retirement and succeeds in the next
		// generation, without admitting this detached store to automatic replay.
		if err := client.Ready(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal("retained waiter did not follow generation", err)
		}
	})
}
