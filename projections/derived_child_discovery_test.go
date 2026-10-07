// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/internal/derivedchildrenfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type derivedFamilyHolder struct {
	ID    string `json:"Id"`
	Name  string
	Items []derivedchildrenfixtures.Child
}

type derivedFamilyBound struct {
	ID    string `json:"Id"`
	Name  string `chronicle:"set(ItemAdded,from=Name)"`
	Items []derivedchildrenfixtures.Child
}

// A fluent derivative needs a key tag. Reusing its family as a whole property
// elsewhere neither creates a projection nor refuses one: key, no-auto and
// not-projected are type metadata, not subscriptions.
func TestFluentDerivativeKeyTagIsNotAProjectionDeclaration(t *testing.T) {
	added, _, _, catalog := derivedEvents(t)
	codecs, err := serialization.NewCodecs(serialization.Derived[derivedchildrenfixtures.Child, *fluentDerivedLine]("line"))
	if err != nil {
		t.Fatal(err)
	}
	holder, err := readmodels.Define[derivedFamilyHolder](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	if projections.HasMappings(holder.Descriptor()) {
		t.Fatal("a derivative key tag was discovered as a projection")
	}
	builder := projections.NewBuilder("holder", holder)
	projections.From(builder, added, func(from *projections.FromBuilder[derivedFamilyHolder, derivedchildrenfixtures.ItemAdded]) {
		projections.Map(from, projections.Path[derivedFamilyHolder, string]("Name"), projections.Path[derivedchildrenfixtures.ItemAdded, string]("Name"))
	})
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projections.Compile(declaration, catalog); err != nil {
		t.Fatalf("fluent whole-family property refused: %v", err)
	}
	bound, err := readmodels.Define[derivedFamilyBound](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	if !projections.HasMappings(bound.Descriptor()) {
		t.Fatal("ordinary model-bound mapping not discovered")
	}
	if _, err := projections.Compile(projections.ModelBound(bound), catalog); err != nil {
		t.Fatalf("model-bound whole-family property refused: %v", err)
	}
}
