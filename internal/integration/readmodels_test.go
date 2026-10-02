//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
)

type CatalogPerson struct {
	ID      string         `json:"id"`
	Name    string         `json:"name"`
	Address CatalogAddress `json:"address"`
}
type CatalogAddress struct {
	City string `json:"city"`
}

func TestKernelReadModelRegistrationAndAbsence(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	model, err := chronicle.RegisterReadModel[CatalogPerson](registry, readmodels.WithIdentifier("Example.CatalogPerson"), readmodels.WithIndexes("name", "address.city"))
	if err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	for _, namespace := range []chronicle.Namespace{"one", "two"} {
		store, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace(namespace))
		if err != nil {
			t.Fatal(err)
		}
		typed, err := readmodels.For(store.ReadModels(), model).Get(fixture.ctx, "never-created")
		if err != nil || typed.Exists || typed.LastHandled != nil {
			t.Fatalf("typed absence in %s: %+v %v", namespace, typed, err)
		}
		raw, err := store.ReadModels().Get(fixture.ctx, model.Identifier(), "never-created")
		if err != nil || raw.Exists || raw.Value != nil || raw.LastHandled != nil {
			t.Fatalf("raw absence in %s: %+v %v", namespace, raw, err)
		}
	}
	definitions, err := contracts.NewReadModelsClient(fixture.conn).GetDefinitions(fixture.ctx, &contracts.GetDefinitionsRequest{EventStore: string(fixture.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	// The kernel also installs its own system read models in each store.
	var definition *contracts.ReadModelDefinition
	for _, candidate := range definitions.ReadModels {
		if candidate.Type.Identifier != string(model.Identifier()) {
			continue
		}
		if definition != nil {
			t.Fatal("duplicate client model definition")
		}
		definition = candidate
	}
	if definition == nil {
		t.Fatal("registered client model is missing")
	}
	if definition.Type.Identifier != string(model.Identifier()) || definition.Type.Generation != 1 || definition.ContainerName != "CatalogPeople" || definition.Owner != contracts.ReadModelOwner_Client || definition.Source != contracts.ReadModelSource_Code || definition.Sink.TypeId != "MongoDB" || len(definition.Indexes) != 2 || definition.Indexes[1].PropertyPath != "address.city" {
		t.Fatalf("registered definition: %+v", definition)
	}
	// Registration alone creates no instance. Presence, released data and session
	// behavior use bufconn until slice 7 provides a real projection producer.
}
