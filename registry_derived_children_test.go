// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/internal/derivedchildrenfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type registryFluentLine struct {
	ItemID string `json:"itemId" chronicle:"key"`
	Name   string
}

func (*registryFluentLine) Child() {}

type registryFluentCatalog struct {
	ID    string `json:"Id"`
	Items []derivedchildrenfixtures.Child
}

type registryFamilyHistory struct {
	ID    string `json:"Id"`
	Items []derivedchildrenfixtures.Child
}

// A fluent derivative needs a key tag. Another registered read model holding
// the same family as a whole property is not auto-discovered as a projection,
// so NewClient admits the registry.
func TestNewClientAdmitsFluentDerivativeReusedAsWholeFamilyProperty(t *testing.T) {
	registry := chronicle.NewRegistry()
	added, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemAdded](registry)
	if err != nil {
		t.Fatal(err)
	}
	codecs, err := serialization.NewCodecs(serialization.Derived[derivedchildrenfixtures.Child, *registryFluentLine]("line"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := chronicle.RegisterReadModel[registryFluentCatalog](registry, readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterReadModel[registryFamilyHistory](registry, readmodels.WithCodecs(codecs)); err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("catalog", catalog)
	projections.Children(builder, projections.Path[registryFluentCatalog, []derivedchildrenfixtures.Child]("Items"), func(child *projections.Builder[registryFluentLine]) {
		projections.From(child, added, func(from *projections.FromBuilder[registryFluentLine, derivedchildrenfixtures.ItemAdded]) {
			projections.Map(from, projections.Path[registryFluentLine, string]("name"), projections.Path[derivedchildrenfixtures.ItemAdded, string]("Name"))
		}, projections.UsingKey(projections.Path[derivedchildrenfixtures.ItemAdded, string]("ItemId")), projections.UsingParentKey(projections.Path[derivedchildrenfixtures.ItemAdded, string]("OrderId")))
	})
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithNoAuthentication())
	if err != nil {
		t.Fatalf("registry with a reused fluent derivative refused: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
