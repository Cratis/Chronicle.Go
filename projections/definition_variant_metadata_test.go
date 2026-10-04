// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

func compiledVariant(t *testing.T, d projections.Definition) bool {
	t.Helper()
	classification, ok := any(d).(interface{ IsVariant() bool })
	if !ok {
		t.Fatal("compiled definition does not expose retained variant classification")
	}
	return classification.IsVariant()
}

func TestCompiledVariantMetadataSurvivesIdenticalWireAndClientSnapshots(t *testing.T) {
	event := mustEvent[IssueCreated](t)
	model := mustModel[PullRequestItem](t)
	ordinary := projections.ModelBound(model, projections.FromEvent(event))
	variant := projections.ModelBound(model, projections.VariantOf[WorkItem](), projections.EntersOn(event))
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	a, err := projections.Compile(ordinary, catalog)
	if err != nil {
		t.Fatal(err)
	}
	b, err := projections.Compile(variant, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(a.KernelDefinition(), b.KernelDefinition()) {
		t.Fatal("ordinary and variant witness must have identical wire shapes")
	}
	if compiledVariant(t, projections.Definition{}) || compiledVariant(t, a) || !compiledVariant(t, b) {
		t.Fatal("classification inferred from wire shape or lost in compilation")
	}
	copy := b
	copy.KernelDefinition().Identifier = "detached-mutation"
	if !compiledVariant(t, copy) || !compiledVariant(t, b) {
		t.Fatal("copy lost classification")
	}
	for _, isVariant := range []bool{false, true} {
		for _, naming := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
			r := chronicle.NewRegistry()
			e, err := chronicle.RegisterEvent[IssueCreated](r)
			if err != nil {
				t.Fatal(err)
			}
			m, err := chronicle.RegisterReadModel[PullRequestItem](r, readmodels.WithIdentifier("metadata"))
			if err != nil {
				t.Fatal(err)
			}
			d := projections.ModelBound(m, projections.FromEvent(e))
			if isVariant {
				d = projections.ModelBound(m, projections.VariantOf[WorkItem](), projections.EntersOn(e))
			}
			if err := r.AddProjection(d); err != nil {
				t.Fatal(err)
			}
			client, err := chronicle.NewClient(chronicle.WithRegistry(r), chronicle.WithNamingPolicy(naming))
			if err != nil {
				t.Fatal(err)
			}
			artifacts, err := client.Artifacts("metadata-store")
			if err != nil {
				t.Fatal(err)
			}
			if len(artifacts.Projections) != 1 || compiledVariant(t, artifacts.Projections[0]) != isVariant {
				t.Fatal("registry naming/store snapshot lost classification")
			}
			artifacts.Projections[0] = projections.Definition{}
			again, err := client.Artifacts("metadata-store")
			if err != nil || len(again.Projections) != 1 || compiledVariant(t, again.Projections[0]) != isVariant {
				t.Fatal("artifact collection mutation affected retained classification")
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
