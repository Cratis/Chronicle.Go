// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"strings"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedchildrenfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type treeFamily interface{ Tree() }
type derivedTree struct {
	ID       ConceptOrderID `chronicle:"key"`
	Name     string         `chronicle:"no-auto"`
	Branches []treeFamily   `chronicle:"children(DerivedBranchAdded,key=ItemID)"`
}

func (*derivedTree) Tree() {}

type derivedTreeRoot struct {
	ID    ConceptOrderID
	Trees [2]treeFamily `chronicle:"children(DerivedTreeAdded,key=ItemID)"`
	Other []treeFamily  `chronicle:"children(DerivedTreeAdded,key=ItemID)"`
}
type DerivedTreeAdded struct {
	ItemID ConceptOrderID
	Wrong  ConceptLineID
	Parent ConceptOrderID
	Second ConceptOrderID
}
type DerivedBranchAdded struct {
	ItemID ConceptLineID
	Wrong  ConceptLineID
	Parent ConceptOrderID
	Second ConceptOrderID
}
type DerivedTreeUpdated struct {
	ItemID ConceptOrderID
	Name   string
}

func TestDerivedChildrenUseConcreteDeclaredIdentityAndPathLocalRecursion(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[treeFamily, *derivedTree]("tree"))
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedTreeRoot](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	root, branch, update := mustEvent[DerivedTreeAdded](t), mustEvent[DerivedBranchAdded](t), mustEvent[DerivedTreeUpdated](t)
	catalog, err := events.NewCatalog(root.Descriptor(), branch.Descriptor(), update.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	definition := compileCatalog(t, projections.ModelBound(model, projections.NoAutoMap(), projections.WithNodes(projections.Node[derivedTree](
		projections.FromEvent(root),
		projections.FromEvent(branch),
		projections.FromEvent(update, projections.UsingKey(projections.Path[DerivedTreeUpdated, ConceptOrderID]("ItemID"))),
	))), catalog)
	wire := definition.KernelDefinition()
	if len(wire.From) != 0 {
		t.Fatal("child subscriptions escaped")
	}
	for _, path := range []string{"Trees", "Other"} {
		child := wire.Children[path]
		if child.IdentifiedBy != "ID" || child.AutoMap != contracts.AutoMap_Disabled || len(child.NoAutoMapProperties) != 1 || child.NoAutoMapProperties[0] != "name" {
			t.Fatalf("child %s: %s", path, child)
		}
		for _, from := range child.From {
			if from.Key.Id == "DerivedTreeAdded" && (from.Value.ParentKey != "Parent" || from.Value.Properties["_derivedTypeId"] != "$value(tree)") {
				t.Fatal("root creator", from)
			}
			if from.Key.Id == "DerivedBranchAdded" {
				t.Fatal("descendant creator propagated into ancestor")
			}
		}
		grandchild := child.Children["branches"]
		if grandchild == nil || len(grandchild.Children) != 0 || grandchild.AutoMap != contracts.AutoMap_Enabled {
			t.Fatal("recursion or immediate NoAuto inheritance", grandchild)
		}
		if len(grandchild.From) != 1 || grandchild.From[0].Key.Id != "DerivedBranchAdded" || grandchild.From[0].Value.ParentKey != "Parent" || grandchild.From[0].Value.Properties["_derivedTypeId"] != "$value(tree)" {
			t.Fatal("ancestor creator or keyed update escaped recursive filter", grandchild)
		}
	}
	if len(definition.Diagnostics()) < 4 {
		t.Fatal("missing first declared type-match diagnostics", definition.Diagnostics())
	}
}

type nestedFamily interface{ Nested() }
type nestedVariant struct {
	Title string `chronicle:"set(DerivedNestedChanged,from=Title)"`
}

func (*nestedVariant) Nested() {}

type nestedDerivedRoot struct {
	ID       string
	Selected nestedFamily `chronicle:"nested"`
}
type untaggedFamilyRoot struct {
	ID       string `chronicle:"set(DerivedNestedChanged,from=Title)"`
	Selected nestedFamily
}
type DerivedNestedChanged struct{ Title string }

// C# resolves a derived type only for children collections; nested objects
// keep the declared family and stamp no discriminator. Go refuses both shapes
// instead of silently ignoring the derivative's declarations.
func TestDerivedDeclarationsOutsideChildrenCollectionsAreRefused(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[nestedFamily, *nestedVariant]("nested"))
	if err != nil {
		t.Fatal(err)
	}
	event := mustEvent[DerivedNestedChanged](t)
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	nested, err := readmodels.Define[nestedDerivedRoot](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	var declaration *projections.DeclarationError
	if _, err := projections.Compile(projections.ModelBound(nested), catalog); !errors.As(err, &declaration) || !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("nested derived family accepted: %v", err)
	}
	untagged, err := readmodels.Define[untaggedFamilyRoot](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	if !projections.HasMappings(untagged.Descriptor()) {
		t.Fatal("derivative declarations were not discovered")
	}
	_, err = projections.Compile(projections.ModelBound(untagged), catalog)
	if !errors.As(err, &declaration) || !errors.Is(err, faults.ErrInvalidConfiguration) || declaration.Path != "Selected" {
		t.Fatalf("derivative declarations outside children accepted: %v", err)
	}
}

type derivedGlobalCollision struct {
	Items    []derivedchildrenfixtures.Child `chronicle:"children(ItemAdded,key=ItemId,parent-key=OrderId)"`
	Metadata string                          `json:"_DERIVEDTYPEID" chronicle:"every(from=Name)"`
}

func TestDerivedDiscriminatorCannotBeWrittenThroughFluentOrGlobals(t *testing.T) {
	added, _, _, catalog := derivedEvents(t)
	codecs, _ := serialization.NewCodecs(serialization.Derived[derivedchildrenfixtures.Child, *fluentDerivedLine]("line"))
	model, err := readmodels.Define[fluentDerivedCatalog](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"_derivedTypeId", "_DerivedTypeId", "_DERIVEDTYPEID"} {
		builder := projections.NewBuilder("bad-write", model)
		projections.Children(builder, projections.Path[fluentDerivedCatalog, []derivedchildrenfixtures.Child]("Items"), func(child *projections.Builder[fluentDerivedLine]) {
			projections.From(child, added, func(from *projections.FromBuilder[fluentDerivedLine, derivedchildrenfixtures.ItemAdded]) {
				projections.Value(from, projections.Path[fluentDerivedLine, string](path), "line")
			})
		})
		_, err := builder.Build()
		var declaration *projections.DeclarationError
		if !errors.As(err, &declaration) || !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatal("reserved write accepted", err)
		}
	}
	collision, err := readmodels.Define[derivedGlobalCollision](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projections.Compile(projections.ModelBound(collision), catalog)
	if !errors.Is(err, faults.ErrInvalidConfiguration) || !strings.Contains(err.Error(), "discriminator") {
		t.Fatalf("global bypass: %v", err)
	}
}
