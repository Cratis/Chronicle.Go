// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"google.golang.org/protobuf/proto"
)

func TestVariantMetadataSurvivesIdenticalOrdinaryWireShape(t *testing.T) {
	event := mustEvent[IssueCreated](t)
	model := mustModel[PullRequestItem](t)
	ordinary := projections.ModelBound(model, projections.FromEvent(event))
	variant := projections.ModelBound(model, projections.VariantOf[WorkItem](), projections.EntersOn(event))
	if (projections.Declaration{}).IsVariant() || ordinary.IsVariant() || !variant.IsVariant() {
		t.Fatal("variant metadata lost or inferred")
	}
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
		t.Fatal("fixture no longer demonstrates identical wire shape")
	}
	builder := projections.NewBuilder("variant", model, projections.VariantOf[WorkItem](), projections.VariantKey(projections.Path[PullRequestItem, string]("id")), projections.EntersOn(event))
	frozen, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if !frozen.IsVariant() || !variant.IsVariant() {
		t.Fatal("snapshot lost variant metadata")
	}
}
