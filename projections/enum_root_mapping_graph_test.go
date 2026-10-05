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
)

type enumDottedOrdinaryNode struct {
	ID     string
	Status int32 `json:"State.Value"`
}
type enumDottedNodeOwner struct {
	ID     string
	Items  []enumDottedOrdinaryNode
	Detail *enumDottedOrdinaryNode
}
type enumDottedBoundNodeOwner struct {
	ID     string
	Items  []enumDottedPlain `chronicle:"children(enumDottedEvent)"`
	Detail *enumDottedPlain  `chronicle:"nested"`
}
type enumDottedGlobal struct {
	Ready bool `chronicle:"set(enumDottedEvent)"`
}
type enumDottedVariant struct {
	ID     string         `json:"ID" chronicle:"key"`
	Status projectionEnum `json:"State.Value"`
	Ready  bool
}

func TestEnumDottedSourceGuardsCollectionAndNestedFinalBindings(t *testing.T) {
	event := mustEvent[enumDottedEvent](t, events.WithCodecs(projectionEnumCodecs(t, false)))
	for _, node := range []string{"collection", "nested"} {
		for _, disabled := range []bool{false, true} {
			name := node + "/enabled"
			if disabled {
				name = node + "/disabled"
			}
			t.Run(name, func(t *testing.T) {
				b := projections.NewBuilder("enum-dotted-node", mustModel[enumDottedNodeOwner](t))
				define := func(child *projections.Builder[enumDottedOrdinaryNode]) {
					if disabled {
						child.Configure(projections.NoAutoMap())
					}
					projections.From(child, event, nil)
				}
				if node == "collection" {
					projections.Children(b, projections.Path[enumDottedNodeOwner, []enumDottedOrdinaryNode]("Items"), define)
				} else {
					projections.Nested(b, projections.Path[enumDottedNodeOwner, *enumDottedOrdinaryNode]("Detail"), define)
				}
				_, err := b.Build()
				if disabled {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					enumBoundaryFailure(t, err, "State.Value")
				}
			})
		}
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = projections.Compile(projections.ModelBound(mustModel[enumDottedBoundNodeOwner](t), projections.FromEvent(event)), catalog)
	enumBoundaryFailure(t, err, "State.Value")
}

func TestEnumDottedRootGlobalGeneratedJoinRefusesBeforeRPC(t *testing.T) {
	r := chronicle.NewRegistry()
	codecs := projectionEnumCodecs(t, false)
	if _, err := chronicle.RegisterEvent[enumDottedEvent](r, events.WithCodecs(codecs)); err != nil {
		t.Fatal(err)
	}
	tick, err := chronicle.RegisterEvent[enumSparseTick](r)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[enumDottedVariant](r, readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(model, projections.VariantOf[WorkItem](), projections.EntersOn(tick))); err != nil {
		t.Fatal(err)
	}
	global, err := projections.Global[enumDottedGlobal](projections.GlobalFor[WorkItem]())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(global); err != nil {
		t.Fatal(err)
	}
	enumRegistrationNoRPC(t, r, "State.Value", serialization.PreservePropertyNames)
}
