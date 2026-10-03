// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

// Reuse the existing bufconn production-observer fixture: scope/provider disposal
// must not terminate an active SDK stream, not merely an idle borrowed pointer.
func TestBorrowedFacadeScopeDisposalKeepsActiveObserverStream(t *testing.T) {
	registry := reactorRegistry(t)
	var handled atomic.Int32
	if err := chronicle.RegisterReactorHandler(registry, "borrowed-facade", func(context.Context, ReactorInput) error {
		handled.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	client, store, ctx := reactorClient(t, kernel, registry)
	session := receive(t, ctx, kernel.sessions)
	var bindings container.Registry
	if err := services.BindClient(&bindings, client); err != nil {
		t.Fatal(err)
	}
	if err := services.BindEventStore(&bindings, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		return store.Name(), store.Namespace(), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := services.BindEventLog(&bindings); err != nil {
		t.Fatal(err)
	}
	provider, err := bindings.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := provider.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	scope, err := provider.NewScope(ctx)
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := dependencyinjection.Resolve[*eventsequences.Sequence](ctx, scope)
	if err != nil || borrowed != store.EventLog() {
		t.Fatal("borrowed facade identity", err)
	}
	if err := scope.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Deliberately reverse ordinary shutdown only to prove borrowed ownership.
	// Receiving a second acknowledgment on this exact session rules out silent
	// client closure/observer replacement after provider disposal.
	for _, number := range []int{1, 2} {
		session.batches <- batch(number)
		result := receive(t, ctx, session.results)
		if result.State != contracts.ObservationState_Success || result.LastSuccessfulObservation != uint64(number) {
			t.Fatal(result)
		}
	}
	if handled.Load() != 2 || kernel.active.Load() != 1 {
		t.Fatal("active observer lost after facade disposal")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	receive(t, ctx, session.done)
	// reactorClient's fixture cleanup joins server handlers and checks active=0.
}
