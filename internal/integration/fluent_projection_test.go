//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type ProjectionSummary struct {
	ID          string `json:"id" chronicle:"key"`
	Name        string `json:"name"`
	ProductName string `json:"productName"`
	State       string `json:"state"`
}

func TestKernelFluentProjectionConstantKeyAndSequence(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[ProjectionAccountOpened](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[ProjectionSummary](registry)
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("product-summary", model, projections.WithEventSequence("products"), projections.NoAutoMap())
	projections.From(builder, event, func(from *projections.FromBuilder[ProjectionSummary, ProjectionAccountOpened]) {
		projections.Map(from, projections.Path[ProjectionSummary, string]("name"), projections.Path[ProjectionAccountOpened, string]("fullName"))
		projections.Value(from, projections.Path[ProjectionSummary, string]("state"), "summary")
	}, projections.UsingConstantKey("catalog"))
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := store.EventSequence("products")
	if err != nil {
		t.Fatal(err)
	}
	result, err := sequence.Append(fixture.ctx, "product-1", ProjectionAccountOpened{FullName: "Notebook", ProductName: "must not AutoMap"})
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Err(); err != nil {
		t.Fatal(err)
	}
	instance := awaitProjection(t, fixture.ctx, readmodels.For(store.ReadModels(), model), "catalog", func(value ProjectionSummary) bool { return value.Name == "Notebook" && value.State == "summary" })
	if instance.Value.ProductName != "" {
		t.Fatal("NoAutoMap was ignored")
	}
	missing, err := readmodels.For(store.ReadModels(), model).Get(fixture.ctx, "product-1")
	if err != nil || missing.Exists {
		t.Fatalf("constant key ignored: %+v %v", missing, err)
	}
}
