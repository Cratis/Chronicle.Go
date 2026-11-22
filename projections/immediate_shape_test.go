// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

type noSinkState struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}

type immediateCreated struct{ Name string }
type immediateMoved struct{ Name string }
type immediateVariantBase struct{}
type immediateGlobal struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}
type immediateSibling struct {
	ID string `json:"id" chronicle:"key"`
}

type immediateVariantFixture struct {
	event   events.Type[immediateCreated]
	moved   events.Type[immediateMoved]
	catalog *events.Catalog
	model   readmodels.Model[noSinkState]
	variant Declaration
}

func newImmediateVariantFixture(t *testing.T) immediateVariantFixture {
	t.Helper()
	event, err := events.Define[immediateCreated]()
	if err != nil {
		t.Fatal(err)
	}
	moved, err := events.Define[immediateMoved]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor(), moved.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[noSinkState]()
	if err != nil {
		t.Fatal(err)
	}
	variant := ModelBound(model, VariantOf[immediateVariantBase](), EntersOn(event))
	variant.data.model, err = variant.data.model.WithDefaultSinkType(readmodels.NoSink)
	if err != nil {
		t.Fatal(err)
	}
	return immediateVariantFixture{event, moved, catalog, model, variant}
}

func (f immediateVariantFixture) global(t *testing.T) Declaration {
	t.Helper()
	globalModel, err := readmodels.Define[immediateGlobal]()
	if err != nil {
		t.Fatal(err)
	}
	globalBuilder := NewBuilder("global", globalModel, GlobalFor[immediateVariantBase]())
	From(globalBuilder, f.moved, func(from *FromBuilder[immediateGlobal, immediateMoved]) {
		Map(from, Path[immediateGlobal, string]("name"), Path[immediateMoved, string]("Name"))
	})
	global, err := globalBuilder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return global
}

func TestNoSinkVariantValidationUsesFinalMergedGraph(t *testing.T) {
	f := newImmediateVariantFixture(t)
	if _, err := CompileGroup([]Declaration{f.variant}, f.catalog); err != nil {
		t.Fatal("source-keyed variant rejected", err)
	}
	global := f.global(t)
	if _, err := CompileGroup([]Declaration{f.variant, global}, f.catalog); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal("post-merge join accepted", err)
	}
	control := ModelBound(f.model, VariantOf[immediateVariantBase](), EntersOn(f.event))
	if _, err := CompileGroup([]Declaration{control, global}, f.catalog); err != nil {
		t.Fatal("materialized merged control rejected", err)
	}
	custom := ModelBound(f.model, VariantOf[immediateVariantBase](), EntersOn(f.event, UsingKey(Path[immediateCreated, string]("Name"))))
	custom.data.model = f.variant.data.model
	if _, err := CompileGroup([]Declaration{custom}, f.catalog); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal("custom entering key accepted", err)
	}
}

func noSinkDefinition(t *testing.T, kind readmodels.SinkType, node nodeDefinition) *definition {
	t.Helper()
	model, err := readmodels.Define[noSinkState]()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := model.Descriptor().WithDefaultSinkType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return &definition{model: descriptor, nodeDefinition: node}
}

func TestNoSinkGraphAcceptsOnlySourceIdentityExpressions(t *testing.T) {
	for _, kind := range []expressionKind{emptyExpression, sourceExpression, pathExpression, contextExpression, literalExpression, compositeExpression} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			node := nodeDefinition{from: []fromDefinition{{key: expression{kind: kind}}}}
			err := validateNoSinkGraph(noSinkDefinition(t, readmodels.NoSink, node))
			wantFailure := kind != emptyExpression && kind != sourceExpression
			if errors.Is(err, faults.ErrInvalidConfiguration) != wantFailure {
				t.Fatalf("kind=%d error=%v", kind, err)
			}
		})
	}
}

func TestNoSinkGraphRefusesCorrelationAtEveryNode(t *testing.T) {
	property := expression{kind: pathExpression, text: "parentID"}
	shapes := map[string]nodeDefinition{
		"from key":      {from: []fromDefinition{{key: property}}},
		"from parent":   {from: []fromDefinition{{parent: property}}},
		"join":          {joins: []joinDefinition{{}}},
		"remove key":    {removals: []removalDefinition{{key: property}}},
		"remove parent": {removals: []removalDefinition{{parent: property}}},
		"remove join":   {removals: []removalDefinition{{join: true}}},
	}
	for name, shape := range shapes {
		for _, location := range []string{"root", "children", "nested"} {
			t.Run(name+"/"+location, func(t *testing.T) {
				node := noSinkNodeAt(shape, location)
				if err := validateNoSinkGraph(noSinkDefinition(t, readmodels.NoSink, node)); !errors.Is(err, faults.ErrInvalidConfiguration) {
					t.Fatalf("unsupported correlation accepted: %v", err)
				}
				for _, materialized := range []readmodels.SinkType{readmodels.MongoDB, readmodels.SQL, readmodels.InMemory} {
					if err := validateNoSinkGraph(noSinkDefinition(t, materialized, node)); err != nil {
						t.Fatal("materialized shape restricted", err)
					}
				}
			})
		}
	}
}

func noSinkNodeAt(node nodeDefinition, location string) nodeDefinition {
	switch location {
	case "children":
		return nodeDefinition{children: map[string]*nodeDefinition{"items": &node}}
	case "nested":
		return nodeDefinition{nested: map[string]*nodeDefinition{"details": &node}}
	default:
		return node
	}
}

func TestNoSinkRebindRefusesWithoutPublishingPartialDefinition(t *testing.T) {
	original, catalog := immediateCustomDefinition(t)
	none, err := original.Model().WithDefaultSinkType(readmodels.NoSink)
	if err != nil {
		t.Fatal(err)
	}
	refused, err := original.Rebind(none, catalog, catalog)
	if !errors.Is(err, faults.ErrInvalidConfiguration) || refused.KernelDefinition() != nil {
		t.Fatal("invalid rebind published a definition", err)
	}
	if original.Model().Sink().Type != readmodels.MongoDB {
		t.Fatal("refused rebind mutated the original")
	}
	if _, err := original.Rebind(original.Model(), catalog, catalog); err != nil {
		t.Fatal("materialized rebind rejected", err)
	}
}

func immediateCustomDefinition(t *testing.T) (Definition, *events.Catalog) {
	t.Helper()
	model, err := readmodels.Define[noSinkState]()
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[immediateCreated]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	builder := NewBuilder("custom", model)
	From(builder, event, func(from *FromBuilder[noSinkState, immediateCreated]) {
		Map(from, Path[noSinkState, string]("name"), Path[immediateCreated, string]("Name"))
	}, UsingKey(Path[immediateCreated, string]("Name")))
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return compiled, catalog
}

func TestNoSinkSourceVariantsKeepSynthesizedSiblingRemovals(t *testing.T) {
	first, created := immediateVariant[noSinkState, immediateCreated](t)
	second, moved := immediateVariant[immediateSibling, immediateMoved](t)
	catalog, err := events.NewCatalog(created, moved)
	if err != nil {
		t.Fatal(err)
	}
	definitions, err := CompileGroup([]Declaration{first, second}, catalog)
	if err != nil || len(definitions) != 2 {
		t.Fatal("source-keyed variants rejected", err)
	}
	for _, definition := range definitions {
		removals := definition.KernelDefinition().RemovedWith
		if len(removals) != 1 || removals[0].Value.Key != "$eventSourceId" {
			t.Fatal("source-identity sibling removal lost", removals)
		}
	}
}

func immediateVariant[M, E any](t *testing.T) (Declaration, events.Descriptor) {
	t.Helper()
	model, err := readmodels.Define[M]()
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[E]()
	if err != nil {
		t.Fatal(err)
	}
	declaration := ModelBound(model, VariantOf[immediateVariantBase](), EntersOn(event))
	declaration.data.model, err = declaration.data.model.WithDefaultSinkType(readmodels.NoSink)
	if err != nil {
		t.Fatal(err)
	}
	return declaration, event.Descriptor()
}

func TestNoSinkGraphPreservesExistingPassiveCorrelation(t *testing.T) {
	definition := noSinkDefinition(t, readmodels.NoSink, nodeDefinition{joins: []joinDefinition{{}}})
	definition.passive = true
	if err := validateNoSinkGraph(definition); err != nil {
		t.Fatal("existing passive graph restricted", err)
	}
}

func TestNoSinkGraphAcceptsRecursiveSourceIdentityAndRemovals(t *testing.T) {
	source := expression{kind: sourceExpression}
	leaf := nodeDefinition{from: []fromDefinition{{key: source, parent: source}}, removals: []removalDefinition{{key: source, parent: source}}}
	for _, location := range []string{"root", "children", "nested"} {
		if err := validateNoSinkGraph(noSinkDefinition(t, readmodels.NoSink, noSinkNodeAt(leaf, location))); err != nil {
			t.Fatal(location, err)
		}
	}
}
