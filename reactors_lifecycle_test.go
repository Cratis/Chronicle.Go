// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
)

func TestReactorPerEventCreatesAndClosesEachScope(t *testing.T) {
	registry := reactorRegistry(t)
	scopes := &countingScopes{}
	if err := chronicle.RegisterReactorHandler(registry, "event-scopes", func(context.Context, ReactorInput) error { return nil }, reactors.PerEvent()); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	_, _, ctx := reactorClient(t, kernel, registry, chronicle.WithServices(scopes))
	session := receive(t, ctx, kernel.sessions)
	session.batches <- batch(0, 1, 2)
	result := receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Success || scopes.opened.Load() != 3 || scopes.closed.Load() != 3 {
		t.Fatal(result, scopes.opened.Load(), scopes.closed.Load())
	}
}
func TestReactorActivationFailureIsReportedWithoutAdvancement(t *testing.T) {
	registry := reactorRegistry(t)
	scopes := &countingScopes{}
	if err := chronicle.RegisterReactor[*BatchReactor](registry, func() (*BatchReactor, error) { return nil, errors.New("activation failed") }); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	_, _, ctx := reactorClient(t, kernel, registry, chronicle.WithServices(scopes))
	session := receive(t, ctx, kernel.sessions)
	session.batches <- batch(0, 1)
	result := receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Failed || result.LastSuccessfulObservation != uint64(events.Unavailable) || scopes.closed.Load() != 1 || kernel.appendCalls.Load() != 0 {
		t.Fatal(result)
	}
}
func TestReactorUnregisterIsRetainedAcrossReconnect(t *testing.T) {
	registry := reactorRegistry(t)
	for _, id := range []reactors.ID{"removed", "retained"} {
		if err := chronicle.RegisterReactorHandler(registry, id, func(context.Context, ReactorInput) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	kernel := &reactorKernel{}
	_, store, ctx := reactorClient(t, kernel, registry)
	a, b := receive(t, ctx, kernel.sessions), receive(t, ctx, kernel.sessions)
	if a.registration.Reactor.ReactorId == "retained" {
		a, b = b, a
	}
	if err := store.UnregisterReactor(ctx, "removed"); err != nil {
		t.Fatal(err)
	}
	receive(t, ctx, a.done)
	close(b.end)
	receive(t, ctx, b.done)
	next := receive(t, ctx, kernel.sessions)
	if next.registration.Reactor.ReactorId != "retained" {
		t.Fatal(next.registration)
	}
	// Ready joins replay of the complete frozen set, including the removal check.
	// A retained reactor's result is also evidence its generation finished startup.
	next.batches <- batch(0)
	receive(t, ctx, next.results)
	select {
	case extra := <-kernel.sessions:
		t.Fatalf("unregistered reactor restarted: %v", extra.registration)
	default:
	}
}
