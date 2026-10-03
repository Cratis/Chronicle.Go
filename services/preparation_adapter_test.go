// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/seeding"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type nilMapFactory map[string]string

func (nilMapFactory) NewScope(context.Context) (dependencyinjection.Scope, error) {
	panic("nil map factory invoked")
}

type directClientSeeder struct{}

func (*directClientSeeder) Seed(*seeding.Builder) error { return nil }

type runtimeIdentityEvent struct{ Value string }
type runtimeIdentityReactor struct{}

func (*runtimeIdentityReactor) Handle(context.Context, runtimeIdentityEvent) error { return nil }

func TestPreparationAdapterPreservesCatalogDirectIdentityAndRuntimeScopeAccess(t *testing.T) {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[runtimeIdentityEvent](registry); err != nil {
		t.Fatal(err)
	}
	var p *chronicle.ClientPreparation
	constructed := 0
	if err := chronicle.RegisterSeederFactory[*directClientSeeder](registry, func(client *chronicle.Client) *directClientSeeder {
		if client != p.Client() {
			t.Error("direct dependency identity differs")
		}
		constructed++
		return &directClientSeeder{}
	}); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactor[*runtimeIdentityReactor](registry, func(ctx context.Context, scope reactors.Scope, client *chronicle.Client) *runtimeIdentityReactor {
		if client != p.Client() {
			t.Error("runtime dependency identity differs")
		}
		borrowed, ok := services.Scope(scope)
		if !ok {
			t.Error("identity check hid Fundamentals scope")
			return nil
		}
		resolved, err := dependencyinjection.Resolve[*chronicle.Client](ctx, borrowed)
		if err != nil || resolved != client {
			t.Error("scope lost provider", err)
		}
		constructed++
		return &runtimeIdentityReactor{}
	}); err != nil {
		t.Fatal(err)
	}
	var err error
	p, err = chronicle.CaptureClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	var bindings container.Registry
	if err := dependencyinjection.BindValue(&bindings, p.Client()); err != nil {
		t.Fatal(err)
	}
	provider, err := bindings.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close(context.Background()) }()
	defer func() { _ = p.Client().Close() }()
	if _, err := services.PrepareClient(t.Context(), p, nilMapFactory(nil)); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	client, err := services.PrepareClient(t.Context(), p, provider)
	if err != nil || client != p.Client() {
		t.Fatal(client, err)
	}
	if constructed != 1 {
		t.Fatal("capture/runtime construction timing changed", constructed)
	}
	plans, err := client.Artifacts("store")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := plans.Reactors[0].Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if constructed != 2 {
		t.Fatal("runtime constructor did not run")
	}
	if again, err := services.PrepareClient(nil, p, nilMapFactory(nil)); err != nil || again != client { //nolint:staticcheck // SA1012: completed outcomes ignore invalid arguments.
		t.Fatal("completed outcome lost", err)
	}
}

func TestPreparationAdapterRejectsConfiguredScopesConflictWithoutConsumingAttempt(t *testing.T) {
	var bindings container.Registry
	provider, err := bindings.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close(context.Background()) }()
	p, err := chronicle.CaptureClient(services.WithServices(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Client().Close() }()
	if _, err := services.PrepareClient(t.Context(), p, provider); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if _, err := services.PrepareClient(t.Context(), p, nil); err != nil {
		t.Fatal(err)
	}
}
