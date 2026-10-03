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

func TestKernelDefinitionFactoriesMaterializeConstrainAndEvolve(t *testing.T) {
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
	constructed, closed, defined := 0, 0, 0
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
	client := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
	if constructed != 3 || closed != 3 || defined != 3 {
		t.Fatalf("constructed=%d closed=%d defined=%d", constructed, closed, defined)
	}
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
	if constructed != 3 || closed != 3 || defined != 3 {
		t.Fatal("store/namespace binding reran factories")
	}
}
