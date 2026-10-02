// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type ReactorDependency struct {
	Calls  atomic.Int32
	Closed atomic.Int32
}

func (d *ReactorDependency) Close() error { d.Closed.Add(1); return nil }

type ServiceReactor struct{ dependency *ReactorDependency }

func (r *ServiceReactor) On(_ ReactorInput, dep *ReactorDependency) error {
	if dep != r.dependency {
		return errors.New("constructor and method got different scoped dependencies")
	}
	dep.Calls.Add(1)
	return nil
}
func TestReactorFundamentalsScopeCatalogAndBorrowedSingletonOwnership(t *testing.T) {
	registry := reactorRegistry(t)
	var bindings container.Registry
	dependency := &ReactorDependency{}
	if err := dependencyinjection.Bind[*ReactorDependency](&bindings, dependencyinjection.Singleton, func(context.Context, dependencyinjection.Resolver) (*ReactorDependency, error) {
		return dependency, nil
	}); err != nil {
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
	if err := chronicle.RegisterReactor[*ServiceReactor](registry, func(dep *ReactorDependency) *ServiceReactor { return &ServiceReactor{dep} }); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	client, _, ctx := reactorClient(t, kernel, registry, services.WithServices(provider))
	session := receive(t, ctx, kernel.sessions)
	session.batches <- batch(0, 1)
	result := receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Success || dependency.Calls.Load() != 2 || dependency.Closed.Load() != 0 {
		t.Fatal(result, dependency.Calls.Load(), dependency.Closed.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if dependency.Closed.Load() != 0 {
		t.Fatal("client disposed provider singleton")
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if dependency.Closed.Load() != 1 {
		t.Fatal("provider did not own singleton")
	}
}
func TestReactorCatalogValidationAtNewClient(t *testing.T) {
	registry := reactorRegistry(t)
	if err := chronicle.RegisterReactor[*ServiceReactor](registry, func(dep *ReactorDependency) *ServiceReactor { return &ServiceReactor{dep} }); err != nil {
		t.Fatal(err)
	}
	var bindings container.Registry
	provider, err := bindings.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := provider.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), services.WithServices(provider))
	var declaration *reactors.DeclarationError
	if client != nil || !errors.As(err, &declaration) {
		t.Fatalf("client %v error %v", client, err)
	}
	if _, err := chronicle.NewClient(services.WithServices(nil)); err == nil {
		t.Fatal("nil provider accepted")
	}
}
func TestReactorDuplicateAndClientCatalogIsolation(t *testing.T) {
	registry := reactorRegistry(t)
	register := func() error {
		return chronicle.RegisterReactorHandler(registry, "one", func(context.Context, ReactorInput) error { return nil })
	}
	if err := register(); err != nil {
		t.Fatal(err)
	}
	if err := register(); err == nil {
		t.Fatal("duplicate admitted")
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	empty := chronicle.NewRegistry()
	if err := chronicle.RegisterReactorHandler(empty, "one", func(context.Context, ReactorInput) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = chronicle.NewClient(chronicle.WithRegistryForStore("isolated", empty), chronicle.WithRegistry(registry)); err == nil {
		t.Fatal("event leaked from default catalog into isolated registry")
	}
}
