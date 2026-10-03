// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/proto"
)

type NestedOwner struct {
	Address *OrderAddress `chronicle:"nested;clear(AddressCleared)"`
}

func TestNestedNodeAndMemberClearMerge(t *testing.T) {
	changed, cleared := mustEvent[AddressChanged](t), mustEvent[AddressCleared](t)
	catalog, err := events.NewCatalog(changed.Descriptor(), cleared.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	d := compileCatalog(t, projections.ModelBound(mustModel[NestedOwner](t), projections.WithNodes(projections.Node[OrderAddress](projections.FromEvent(changed), projections.ClearWith(cleared)))), catalog).KernelDefinition()
	if len(d.Nested["Address"].RemovedWith) != 1 || d.Nested["Address"].IdentifiedBy != "*NotSet*" || len(d.From) != 0 {
		t.Fatal(d)
	}
}

type FluentNestedOwner struct {
	Address *OrderAddress `json:"address"`
}

func TestFluentWithNodesSharesTypedMetadata(t *testing.T) {
	changed, cleared := mustEvent[AddressChanged](t), mustEvent[AddressCleared](t)
	b := projections.NewBuilder("nodes", mustModel[FluentNestedOwner](t), projections.WithNodes(projections.Node[OrderAddress](projections.FromEvent(changed), projections.ClearWith(cleared), projections.NoAutoMap())))
	projections.Nested(b, projections.Path[FluentNestedOwner, *OrderAddress]("address"), nil)
	d, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(changed.Descriptor(), cleared.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	wire := compileCatalog(t, d, catalog).KernelDefinition().Nested["address"]
	if len(wire.From) != 1 || len(wire.RemovedWith) != 1 || wire.AutoMap.String() != "Disabled" {
		t.Fatal(wire)
	}
}

func TestNodeFromEventDoesNotEraseCreatorKeys(t *testing.T) {
	event := mustEvent[ItemAdded](t)
	d := mustCompile(t, projections.ModelBound(mustModel[OnlyItems](t), projections.WithNodes(projections.Node[OrderLine](projections.FromEvent(event)))), event.Descriptor()).KernelDefinition()
	if d.Children["Items"].From[0].Value.Key != "itemId" || d.Children["Items"].From[0].Value.ParentKey != "orderId" {
		t.Fatal(d)
	}
}

type OnlyItems struct {
	Items []OrderLine `chronicle:"children(ItemAdded,key=itemId,parent-key=orderId)"`
}

func TestUnreachableNodeAndDuplicateFluentNodeFail(t *testing.T) {
	e := mustEvent[ItemAdded](t)
	catalog, err := events.NewCatalog(e.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = projections.Compile(projections.ModelBound(mustModel[OnlyItems](t), projections.WithNodes(projections.Node[OrderAddress](projections.FromEvent(e)))), catalog)
	if err == nil {
		t.Fatal("unreachable node ignored")
	}
	b := projections.NewBuilder("duplicate", mustModel[FluentOrder](t))
	for range 2 {
		projections.Children(b, projections.Path[FluentOrder, []OrderLine]("items"), func(c *projections.Builder[OrderLine]) { projections.From(c, e, nil) })
	}
	if _, err = b.Build(); err == nil {
		t.Fatal("duplicate node accepted")
	}
}

type ConceptAmountAdded struct{ Amount conceptfixtures.Number }
type ConceptArithmetic struct {
	Total conceptfixtures.Number `chronicle:"add(ConceptAmountAdded,from=Amount)"`
}

func TestArithmeticUsesConceptScalarRepresentation(t *testing.T) {
	e := mustEvent[ConceptAmountAdded](t)
	d := mustCompile(t, projections.ModelBound(mustModel[ConceptArithmetic](t)), e.Descriptor()).KernelDefinition()
	if d.From[0].Value.Properties["Total"] != "$add(Amount)" {
		t.Fatal(d)
	}
}

type RestrictedArithmeticEvent struct {
	Amount int32 `json:"amount_delta"`
}
type RestrictedArithmetic struct {
	Total int32 `chronicle:"add(RestrictedArithmeticEvent,from=amount_delta)"`
}

func TestArithmeticRejectsKernelUnrepresentablePath(t *testing.T) {
	e := mustEvent[RestrictedArithmeticEvent](t)
	catalog, err := events.NewCatalog(e.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = projections.Compile(projections.ModelBound(mustModel[RestrictedArithmetic](t)), catalog)
	if err == nil {
		t.Fatal("arithmetic kernel regex limitation ignored")
	}
}

type EveryPayload struct {
	Name string `chronicle:"every(from=name);set(CustomerRenamed,from=name)"`
}
type FluentEveryPayload struct{ Name string }

func TestGlobalPayloadAndIncludeChildrenFrontEndEquality(t *testing.T) {
	e := mustEvent[CustomerRenamed](t)
	mb := mustCompile(t, projections.ModelBound(mustModel[EveryPayload](t, readmodels.WithIdentifier("global")), projections.WithIdentifier("global")), e.Descriptor()).KernelDefinition()
	b := projections.NewBuilder("global", mustModel[FluentEveryPayload](t, readmodels.WithIdentifier("global")))
	projections.From(b, e, func(f *projections.FromBuilder[FluentEveryPayload, CustomerRenamed]) {
		projections.Map(f, projections.Path[FluentEveryPayload, string]("Name"), projections.Path[CustomerRenamed, string]("name"))
	})
	projections.Every(b, func(g *projections.EveryBuilder[FluentEveryPayload]) {
		projections.EveryMap(g, projections.Path[FluentEveryPayload, string]("Name"), "name")
	})
	d, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(mb, mustCompile(t, d, e.Descriptor()).KernelDefinition()) {
		t.Fatal("every payload mismatch")
	}
	b = projections.NewBuilder("exclude", mustModel[FluentEveryPayload](t))
	projections.From(b, e, nil)
	projections.Every(b, func(g *projections.EveryBuilder[FluentEveryPayload]) {
		g.IncludeChildren(false)
		projections.EveryMap(g, projections.Path[FluentEveryPayload, string]("Name"), "name")
	})
	d, err = b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if mustCompile(t, d, e.Descriptor()).KernelDefinition().All.IncludeChildren {
		t.Fatal("IncludeChildren false ignored")
	}
}

type AllChild struct {
	Name string `chronicle:"all(from=name)"`
}
type AllChildParent struct {
	Items []AllChild `chronicle:"children(ItemAdded,key=itemId)"`
}

func TestChildFromAllDoesNotBroadenRootSubscription(t *testing.T) {
	e := mustEvent[ItemAdded](t)
	d := mustCompile(t, projections.ModelBound(mustModel[AllChildParent](t)), e.Descriptor()).KernelDefinition()
	if d.SubscribesToAllEvents || d.All.Properties["Name"] != "name" {
		t.Fatal("child global did not match C# root-only subscription flag")
	}
}
