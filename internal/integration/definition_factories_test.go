//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/services"
	di "github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type FactoryCustomerV1 struct{ Name string }
type FactoryCustomer struct{ DisplayName string }
type FactoryCustomerView struct {
	ID          string `chronicle:"key"`
	DisplayName string
}
type kernelDefinitionArtifact struct {
	label  string
	closed *int
}

func (a *kernelDefinitionArtifact) Close() error { (*a.closed)++; return nil }

type kernelDefinitionProvider struct {
	di.Provider
	opened, closed, disposed int
}

func (p *kernelDefinitionProvider) NewScope(ctx context.Context) (di.Scope, error) {
	scope, err := p.Provider.NewScope(ctx)
	if scope != nil {
		p.opened++
		return kernelDefinitionScope{scope, &p.closed}, err
	}
	return nil, err
}
func (p *kernelDefinitionProvider) Close(ctx context.Context) error {
	p.disposed++
	return p.Provider.Close(ctx)
}

type kernelDefinitionScope struct {
	di.Scope
	closed *int
}

func (s kernelDefinitionScope) Close(ctx context.Context) error {
	(*s.closed)++
	return s.Scope.Close(ctx)
}

func TestKernelDefinitionFactoriesMaterializeConstrainAndEvolve(t *testing.T) {
	for _, mode := range []string{"plain", "provider-scoped", "provider-singleton"} {
		t.Run(mode, func(t *testing.T) { testKernelDefinitionFactories(t, mode) })
	}
}

func testKernelDefinitionFactories(t *testing.T, mode string) {
	t.Helper()
	f := newKernelFixture(t)
	original := f.client(integrationRegistry[FactoryCustomerV1](t, events.WithID("factory-customer")), chronicle.WithEventTypeGenerationValidation(true))
	oldStore, err := original.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, f.ctx, oldStore, "historical", FactoryCustomerV1{Name: "original"})
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}

	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[FactoryCustomer](registry, events.WithID("factory-customer"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := chronicle.RegisterEventGeneration[FactoryCustomerV1](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[FactoryCustomerView](registry)
	if err != nil {
		t.Fatal(err)
	}
	constructed, closed, defined, providerConstructed := 0, 0, 0, 0
	factory := func() *kernelDefinitionArtifact {
		constructed++
		return &kernelDefinitionArtifact{label: "configured", closed: &closed}
	}
	if err := chronicle.RegisterProjectionFactory(registry, "factory-customers", model.Descriptor(), factory, func(_ context.Context, artifact *kernelDefinitionArtifact) (projections.Declaration, error) {
		defined++
		return projections.ModelBound(model, projections.WithIdentifier("factory-customers"), projections.FromEvent(current), projections.WithLabels(artifact.label)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterConstraintFactory(registry, []string{"factory-display-name"}, factory, func(_ context.Context, artifact *kernelDefinitionArtifact) ([]constraints.Definition, error) {
		defined++
		definition, err := constraints.UniqueValues("factory-display-name").On(current.Descriptor(), "DisplayName").WithMessage(artifact.label + ": already used").Build()
		return []constraints.Definition{definition}, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterEventMigrationFactory(registry, current.Descriptor(), previous.Descriptor(), factory, func(context.Context, *kernelDefinitionArtifact) (events.MigrationDeclaration, error) {
		defined++
		return events.DefineMigration(current, previous, events.Migration[FactoryCustomer, FactoryCustomerV1]{
			Upcast: func(b *events.MigrationBuilder[FactoryCustomer, FactoryCustomerV1]) {
				b.RenamedFrom("DisplayName", "Name")
			},
			Downcast: func(b *events.MigrationBuilder[FactoryCustomerV1, FactoryCustomer]) {
				b.RenamedFrom("Name", "DisplayName")
			},
		})
	}); err != nil {
		t.Fatal(err)
	}
	options := []chronicle.ClientOption{chronicle.WithEventTypeGenerationValidation(true)}
	var provider *kernelDefinitionProvider
	wantConstructed, wantClosed := 3, 3
	if mode != "plain" {
		lifetime := di.Scoped
		if mode == "provider-singleton" {
			lifetime = di.Singleton
			wantConstructed, wantClosed = 1, 0
		}
		var bindings container.Registry
		if err := di.Bind(&bindings, lifetime, func(context.Context, di.Resolver) (*kernelDefinitionArtifact, error) {
			providerConstructed++
			return factory(), nil
		}); err != nil {
			t.Fatal(err)
		}
		built, err := bindings.Build()
		if err != nil {
			t.Fatal(err)
		}
		provider = &kernelDefinitionProvider{Provider: built}
		t.Cleanup(func() {
			if provider.disposed == 0 {
				if err := provider.Close(context.Background()); err != nil {
					t.Error(err)
				}
			}
		})
		options = append(options, services.WithServices(provider))
	}
	client := f.client(registry, options...)
	assertOwnership := func() {
		t.Helper()
		if constructed != wantConstructed || closed != wantClosed || defined != 3 {
			t.Fatalf("constructed=%d closed=%d defined=%d", constructed, closed, defined)
		}
		if provider != nil && (provider.opened != 3 || provider.closed != 3 || provider.disposed != 0 || providerConstructed != wantConstructed) {
			t.Fatalf("scopes=%d/%d provider closes=%d provider constructions=%d", provider.opened, provider.closed, provider.disposed, providerConstructed)
		}
	}
	assertOwnership()
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	catalog, _, err := client.Catalogs(f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		history, err := store.EventLog().ReadSource(ctx, "historical", eventsequences.SourceFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(history) != 1 {
			t.Fatalf("historical count=%d", len(history))
		}
		upcast, err := events.Decode[FactoryCustomer](catalog, history[0])
		if err != nil {
			t.Fatal(err)
		}
		if upcast.DisplayName == "original" {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("factory migration did not evolve history", ctx.Err())
		}
	}
	appendSuccessfully(t, f.ctx, store, "owner", FactoryCustomer{DisplayName: "unique"})
	instance := awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), model), "owner", func(model FactoryCustomerView) bool { return model.DisplayName == "unique" })
	if instance.Value.ID != "owner" {
		t.Fatal("projection lost key", instance)
	}
	rejected := appendConstraintEvent(t, f.ctx, store.EventLog(), "competitor", FactoryCustomer{DisplayName: "unique"})
	requireConstraintRejection(t, rejected, "factory-display-name")
	if rejected.ConstraintViolations[0].Message != "configured: already used" {
		t.Fatal("static factory message lost", rejected.ConstraintViolations)
	}
	history, err := store.EventLog().ReadSource(f.ctx, "owner", eventsequences.SourceFilter{})
	if err != nil || len(history) != 1 {
		t.Fatal("new history missing", err)
	}
	downcast, err := events.Decode[FactoryCustomerV1](catalog, history[0])
	if err != nil || downcast.Name != "unique" {
		t.Fatal("factory downcast missing", downcast, err)
	}
	other, err := client.EventStore(f.ctx, f.storeName, chronicle.WithNamespace("other"))
	if err != nil {
		t.Fatal(err)
	}
	absent, err := readmodels.For(other.ReadModels(), model).Get(f.ctx, "owner")
	if err != nil || absent.Exists {
		t.Fatal("factory model crossed namespaces", absent, err)
	}
	assertOwnership()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	// Client shutdown neither closes the borrowed provider nor disposes a
	// resolved Singleton (and never closes scoped results a second time).
	assertOwnership()
	if provider != nil {
		if err := provider.Close(f.ctx); err != nil {
			t.Fatal(err)
		}
		if closed != wantConstructed || provider.closed != 3 || provider.disposed != 1 {
			t.Fatalf("provider ownership lost: artifacts=%d scopes=%d provider=%d", closed, provider.closed, provider.disposed)
		}
	}
}
