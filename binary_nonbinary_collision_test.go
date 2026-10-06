// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"testing"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type ordinaryDottedAliasFirst struct {
	Alias string `json:"data.payload"`
	Data  struct{ Payload string }
}
type ordinaryDottedAliasLast struct {
	Data  struct{ Payload string }
	Alias string `json:"data.payload"`
}

func TestNonBinaryCollisionKeepsArtifactNamingBehavior(t *testing.T) {
	t.Run("alias first", testNonBinaryCollisionArtifacts[ordinaryDottedAliasFirst])
	t.Run("alias last", testNonBinaryCollisionArtifacts[ordinaryDottedAliasLast])
}
func testNonBinaryCollisionArtifacts[T any](t *testing.T) {
	t.Helper()
	event, err := events.Define[T]()
	if err != nil {
		t.Fatal(err)
	}
	named, err := event.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	before, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	after, err := events.NewCatalog(named)
	if err != nil {
		t.Fatal(err)
	}
	constraint, err := constraints.UniqueValues("ordinary-dotted").On(event.Descriptor(), "Data.Payload").Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := constraint.Rebind(after); err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[T](readmodels.WithIndexes("Data.Payload"))
	if err != nil {
		t.Fatal(err)
	}
	namedModel, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("ordinary-dotted-key", model, projections.NoAutoMap())
	projections.From(b, event, nil, projections.UsingKey(projections.Path[T, string]("data.payload")))
	declaration, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	definition, err := projections.Compile(declaration, before)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definition.Rebind(namedModel, before, after); err != nil {
		t.Fatal(err)
	}
	// Binary-free alias matching retains its existing AutoMap behavior.
	auto := projections.NewBuilder("ordinary-dotted-automap", model)
	projections.From(auto, event, nil)
	d, err := auto.Build()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := projections.Compile(d, before)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Rebind(namedModel, before, after); err != nil {
		t.Fatal(err)
	}
}
