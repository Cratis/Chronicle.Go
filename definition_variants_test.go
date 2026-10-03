// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

type factoryVariantOne struct {
	ID    string `chronicle:"key"`
	Title string
}
type factoryVariantTwo struct {
	ID    string `chronicle:"key"`
	Title string
}
type definitionTitleChanged struct{ Title string }
type definitionShared struct {
	Title string `chronicle:"set(definitionTitleChanged)"`
}

func TestFactoryVariantsAndDirectGlobalsUseOneCanonicalGroupCompiler(t *testing.T) {
	registry := NewRegistry()
	first := declareEvent[catalogEvent](t, registry)
	second := declareEvent[catalogReplacement](t, registry)
	declareEvent[definitionTitleChanged](t, registry)
	one, err := RegisterReadModel[factoryVariantOne](registry)
	if err != nil {
		t.Fatal(err)
	}
	two, err := RegisterReadModel[factoryVariantTwo](registry)
	if err != nil {
		t.Fatal(err)
	}
	factoryCalls := 0
	definition := projections.ModelBound(one, projections.WithIdentifier("one"), projections.VariantOf[projectionGlobalIdentity](), projections.EntersOn(first), projections.WithLabels("variant"), projections.WithInitialValues(factoryVariantOne{Title: "initial"}))
	if err := RegisterProjectionFactory(registry, "one", one.Descriptor(), nil, func(context.Context, preparationDependency) (projections.Declaration, error) {
		factoryCalls++
		return definition, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(projections.ModelBound(two, projections.WithIdentifier("two"), projections.VariantOf[projectionGlobalIdentity](), projections.EntersOn(second))); err != nil {
		t.Fatal(err)
	}
	global, err := projections.Global[definitionShared](projections.GlobalFor[projectionGlobalIdentity]())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(global); err != nil {
		t.Fatal(err)
	}
	direct, err := registry.WithProjection(definition)
	if err != nil {
		t.Fatal(err)
	}
	factoryClient, err := NewClient(WithRegistry(registry), WithRegistryForStore("shared", registry), WithNamingPolicy(serialization.CamelCase))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factoryClient.Close() }()
	directClient, err := NewClient(WithRegistry(direct), WithNamingPolicy(serialization.CamelCase))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = directClient.Close() }()
	prepared, err := factoryClient.Artifacts("shared")
	if err != nil {
		t.Fatal(err)
	}
	golden, err := directClient.Artifacts("shared")
	if err != nil {
		t.Fatal(err)
	}
	if factoryCalls != 1 || len(prepared.Projections) != 2 || len(golden.Projections) != 2 {
		t.Fatal("global materialized or factory repeated")
	}
	byID := map[string]proto.Message{}
	for _, definition := range golden.Projections {
		byID[definition.Identifier()] = definition.KernelDefinition()
	}
	for _, definition := range prepared.Projections {
		if !proto.Equal(definition.KernelDefinition(), byID[definition.Identifier()]) {
			t.Fatal("factory bypassed group compilation")
		}
		wire := definition.KernelDefinition()
		if len(wire.RemovedWith) != 1 || len(wire.Join) != 1 {
			t.Fatal("missing sibling removal or shared global join")
		}
	}
}
