// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/reactors"
)

func reactorRetryClock(t *testing.T) (<-chan struct{}, chan<- struct{}, chronicle.ClientOption) {
	t.Helper()
	waiting, resume := make(chan struct{}, 8), make(chan struct{}, 8)
	return waiting, resume, chronicle.WithReactorRetryWaitForTest(func(ctx context.Context, delay time.Duration) error {
		if delay != 2*time.Second {
			t.Errorf("retry delay = %s, want 2s", delay)
		}
		waiting <- struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resume:
			return nil
		}
	})
}

func TestReactorUnregisterCancelsResubscriptionDelay(t *testing.T) {
	registry := reactorRegistry(t)
	if err := chronicle.RegisterReactorHandler(registry, "waiting", func(context.Context, ReactorInput) error { return nil }); err != nil {
		t.Fatal(err)
	}
	waiting, _, clock := reactorRetryClock(t)
	kernel := &reactorKernel{}
	client, store, ctx := reactorClient(t, kernel, registry, clock)
	session := receive(t, ctx, kernel.sessions)
	session.end <- nil
	receive(t, ctx, waiting)
	if err := store.UnregisterReactor(ctx, "waiting"); err != nil {
		t.Fatal(err)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case unexpected := <-kernel.sessions:
		t.Fatalf("restarted removed reactor: %v", unexpected.registration)
	default:
	}
}

func TestReactorIgnoringCancellationDoesNotBlockReconnect(t *testing.T) {
	registry := reactorRegistry(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	if err := chronicle.RegisterReactorHandler(registry, "slow", func(context.Context, ReactorInput) error {
		close(entered)
		<-release // Deliberately uncooperative application code.
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{endConnection: make(chan error, 1)}
	client, store, ctx := reactorClient(t, kernel, registry)
	first := receive(t, ctx, kernel.sessions)
	first.batches <- batch(0)
	receive(t, ctx, entered)
	kernel.endConnection <- errors.New("connection lost")
	second := receive(t, ctx, kernel.sessions)
	if first.registration.ConnectionId == second.registration.ConnectionId {
		t.Fatal("connection loss did not replace the generation")
	}
	result, err := store.EventLog().Append(ctx, "other", ReactorOutput{42})
	if err != nil || result.Err() != nil {
		t.Fatal(err, result.Err())
	}
	deadline, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := client.CloseContext(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined observer cleanup reported %v", err)
	}
	unblock()
	if err := client.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
}

type registryHook struct {
	name  string
	trace *[]string
}

func (h *registryHook) Before(_ context.Context, invocation reactors.Invocation) error {
	*h.trace = append(*h.trace, "before "+h.name+" "+string(invocation.Delivery.Reactor))
	return nil
}
func (h *registryHook) After(_ context.Context, invocation reactors.Invocation) error {
	*h.trace = append(*h.trace, "after "+h.name+" "+string(invocation.Delivery.Reactor))
	return nil
}

func TestRegistryMiddlewareRunsForEveryReactorBeforeLocalMiddleware(t *testing.T) {
	registry := reactorRegistry(t)
	var trace []string
	for _, name := range []string{"global-a", "global-b"} {
		if err := chronicle.RegisterReactorMiddleware(registry, func() *registryHook { return &registryHook{name, &trace} }); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []reactors.ID{"first", "second"} {
		if err := chronicle.RegisterReactorHandler(registry, id, func(context.Context, ReactorInput) error {
			trace = append(trace, "handler "+string(id))
			return nil
		}, reactors.WithMiddleware(func() *registryHook { return &registryHook{"local", &trace} })); err != nil {
			t.Fatal(err)
		}
	}
	kernel := &reactorKernel{}
	_, _, ctx := reactorClient(t, kernel, registry)
	if err := chronicle.RegisterReactorMiddleware(registry, func() *registryHook { return &registryHook{"too-late", &trace} }); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		session := receive(t, ctx, kernel.sessions)
		id := session.registration.Reactor.ReactorId
		session.batches <- batch(0)
		receive(t, ctx, session.results)
		expected := []string{"before global-a " + id, "before global-b " + id, "before local " + id, "handler " + id, "after global-a " + id, "after global-b " + id, "after local " + id}
		if !slices.Equal(trace, expected) {
			t.Fatal(trace)
		}
		trace = nil
	}
}
