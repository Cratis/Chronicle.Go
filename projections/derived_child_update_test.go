// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedchildrenfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

// The kernel adds a child for any non-join child From whose identity is absent,
// not only for children(...) creators. Every model-bound From of a derived node
// therefore carries the discriminator, so a recreated child still decodes.
func TestDerivedChildKeyedUpdatesCarryTheDiscriminator(t *testing.T) {
	added, removed, renamed, _ := derivedEvents(t)
	updated := mustEvent[derivedchildrenfixtures.ItemUpdated](t)
	catalog, err := events.NewCatalog(added.Descriptor(), removed.Descriptor(), renamed.Descriptor(), updated.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	codecs, err := derivedchildrenfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedchildrenfixtures.Catalog](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	definition := compileCatalog(t, projections.ModelBound(model, projections.WithNodes(projections.Node[derivedchildrenfixtures.Line](
		projections.FromEvent(updated,
			projections.UsingKey(projections.Path[derivedchildrenfixtures.ItemUpdated, string]("ItemId")),
			projections.UsingParentKey(projections.Path[derivedchildrenfixtures.ItemUpdated, string]("OrderId"))),
	))), catalog)
	child := definition.KernelDefinition().Children["Items"]
	if child == nil || len(child.From) != 2 {
		t.Fatalf("child subscriptions: %s", child)
	}
	for _, from := range child.From {
		if from.Value.Properties["_derivedTypeId"] != "$value(line)" {
			t.Fatalf("%s From lacks the discriminator: %s", from.Key.Id, from)
		}
	}
	for _, join := range child.Join {
		if _, ok := join.Value.Properties["_derivedTypeId"]; ok {
			t.Fatal("join never creates a child and must not stamp the discriminator", join)
		}
	}
}
