// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type derivedRebindRoot struct {
	ID    ConceptOrderID
	Trees []treeFamily `chronicle:"children(DerivedTreeAdded,key=ItemID)"`
	Other []treeFamily `chronicle:"children(DerivedTreeAdded,key=ItemID)"`
	More  []treeFamily `chronicle:"children(DerivedTreeAdded,key=ItemID)"`
}

// Rebinding against a catalog that lost a referenced event reports an error and
// never resolves the shape of a sibling or nested derived node from an empty path.
func TestDerivedChildRebindWithMissingEventFailsWithoutPanicking(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[treeFamily, *derivedTree]("tree"))
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedRebindRoot](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	root, branch, update := mustEvent[DerivedTreeAdded](t), mustEvent[DerivedBranchAdded](t), mustEvent[DerivedTreeUpdated](t)
	before, err := events.NewCatalog(root.Descriptor(), branch.Descriptor(), update.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	definition := compileCatalog(t, projections.ModelBound(model, projections.WithNodes(projections.Node[derivedTree](
		projections.FromEvent(update, projections.UsingKey(projections.Path[DerivedTreeUpdated, ConceptOrderID]("ItemID"))),
	))), before)
	nextModel, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	for name, missing := range map[string]events.Descriptor{"root creator": root.Descriptor(), "nested creator": branch.Descriptor(), "keyed update": update.Descriptor()} {
		t.Run(name, func(t *testing.T) {
			var remaining []events.Descriptor
			for _, descriptor := range before.Descriptors() {
				if descriptor.Ref() == missing.Ref() {
					continue
				}
				next, err := descriptor.WithNamingPolicy(serialization.CamelCase)
				if err != nil {
					t.Fatal(err)
				}
				remaining = append(remaining, next)
			}
			after, err := events.NewCatalog(remaining...)
			if err != nil {
				t.Fatal(err)
			}
			_, err = definition.Rebind(nextModel, before, after)
			if !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("missing event rebound: %v", err)
			}
		})
	}
}
