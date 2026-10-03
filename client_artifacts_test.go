// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"google.golang.org/protobuf/proto"
)

type artifactModel struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}

func artifactRegistry(t *testing.T) (*Registry, readmodels.Model[artifactModel], projections.Declaration) {
	t.Helper()
	registry := NewRegistry()
	model, err := RegisterReadModel[artifactModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	event := declareEvent[ProjectionOpened](t, registry)
	builder := projections.NewBuilder("original", model)
	projections.From(builder, event, nil)
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	return registry, model, declaration
}

func TestWithProjectionPreservesEveryRegistryFieldExceptReplacedProducer(t *testing.T) {
	registry, model, old := artifactRegistry(t)
	event := declareEvent[DeclaredEmail](t, registry)
	builder := projections.NewBuilder("replacement", model)
	projections.From(builder, event, nil)
	replacement, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	removed, err := reducers.DefineHandlers(model, "removed", []reducers.Handler{reducers.On(func(context.Context, ProjectionOpened, *artifactModel, events.Context) (*artifactModel, error) {
		return nil, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	// Nonempty canaries exercise every copied field independently of compilation.
	// Composition behavior is covered separately through the production compiler.
	registry.constraints = []constraints.Definition{{}}
	registry.constraintCompositions = []constraintComposition{{name: "canary"}}
	registry.projections = []projections.Declaration{old, {}}
	registry.reactors = []reactorDeclaration{{}}
	registry.reducers = []reducers.Declaration{removed, {}}
	registry.reactorMiddlewares = []any{"middleware"}
	registry.reactorSideEffects = []reactorSideEffectHandler{nil}
	registry.migrations = []events.MigrationDeclaration{{}}
	got, err := registry.WithProjection(replacement)
	if err != nil {
		t.Fatal(err)
	}
	want := &Registry{
		descriptors: registry.descriptors, constraints: registry.constraints,
		constraintCompositions: registry.constraintCompositions, readModels: registry.readModels,
		projections: []projections.Declaration{{}, replacement}, reactors: registry.reactors,
		reducers: []reducers.Declaration{{}}, reactorMiddlewares: registry.reactorMiddlewares,
		reactorSideEffects: registry.reactorSideEffects, migrations: registry.migrations,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("replacement lost or changed unrelated registrations")
	}
	fields := map[string]bool{"mu": true, "descriptors": true, "constraints": true, "constraintCompositions": true, "readModels": true, "projections": true, "reactors": true, "reducers": true, "reactorMiddlewares": true, "reactorSideEffects": true, "migrations": true}
	typ := reflect.TypeFor[Registry]()
	original, detached := reflect.ValueOf(registry).Elem(), reflect.ValueOf(got).Elem()
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if !fields[name] {
			t.Fatalf("new registry field %s needs a preservation assertion", name)
		}
		if name == "mu" {
			continue
		}
		if original.Field(i).Pointer() == detached.Field(i).Pointer() {
			t.Errorf("%s retains the original slice backing storage", name)
		}
	}
	if len(registry.projections) != 2 || registry.projections[0].Identifier() != old.Identifier() || len(registry.reducers) != 2 {
		t.Fatal("original producers mutated")
	}
}

func TestWithProjectionPreservesComposedConstraints(t *testing.T) {
	registry, model, _ := artifactRegistry(t)
	declareEvent[DeclaredEmail](t, registry)
	removed := declareEvent[DeclaredRemoval](t, registry)
	if err := registry.ConfigureDeclaredConstraint("email", func(builder *constraints.Builder) {
		builder.IgnoreCasing().PerEventSourceType().PerEventStreamID().RemovedWith(removed.Descriptor())
	}); err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("replacement", model)
	event, err := events.Define[ProjectionOpened]()
	if err != nil {
		t.Fatal(err)
	}
	projections.From(builder, event, nil)
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	detached, err := registry.WithProjection(declaration)
	if err != nil {
		t.Fatal(err)
	}
	original, replacement := declarationClient(t, registry), declarationClient(t, detached)
	if len(original.constraints) != 1 || len(replacement.constraints) != 1 {
		t.Fatal("constraint missing")
	}
	want, got := original.constraints[0], replacement.constraints[0]
	if !got.IgnoresCasing() || !got.Scope().PerEventSourceType || !got.Scope().PerEventStreamID || len(got.RemovalTypes()) != 1 || !proto.Equal(constraintContract(got), constraintContract(want)) {
		t.Fatalf("composed constraint lost: %v", constraintContract(got))
	}
	if len(detached.readModels) != len(registry.readModels) || detached.readModels[0] != model.Descriptor() {
		t.Fatal("model declaration changed")
	}
}
