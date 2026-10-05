// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

type OrderID string
type LineID string
type InferredItemAdded struct {
	ItemID LineID
	First  OrderID `json:"first_order"`
	Second OrderID
	Name   string
}
type InferredLine struct {
	Explicit LineID
	Key      LineID `chronicle:"key"`
	ID       LineID
	ItemID   LineID
}
type ConventionalLine struct{ ID LineID }
type MatchingLine struct{ ItemID LineID }
type SourceLine struct{ Name string }
type InferenceOrder struct {
	ID           OrderID
	Explicit     []InferredLine     `chronicle:"children(InferredItemAdded,key=ItemID,identified-by=Explicit)"`
	Key          []InferredLine     `chronicle:"children(InferredItemAdded,key=ItemID)"`
	IDConvention []ConventionalLine `chronicle:"children(InferredItemAdded,key=ItemID)"`
	Matching     []MatchingLine     `chronicle:"children(InferredItemAdded,key=ItemID)"`
	Source       []SourceLine       `chronicle:"children(InferredItemAdded,key=ItemID)"`
}

// Hand-derived from ChildrenDefinitionExtensions:798-895: Go has no public
// constructor-parameter attributes, so its property Key precedes Id directly.
func TestChildIdentityPrecedenceAndAmbiguousParentKey(t *testing.T) {
	event := mustEvent[InferredItemAdded](t)
	definition := mustCompile(t, projections.ModelBound(mustModel[InferenceOrder](t)), event.Descriptor())
	wire := definition.KernelDefinition()
	if len(wire.From) != 0 {
		t.Fatal("child-only event leaked into root From")
	}
	for path, expected := range map[string]string{"Explicit": "Explicit", "Key": "Key", "IDConvention": "ID", "Matching": "ItemID", "Source": "$eventSourceId"} {
		child := wire.Children[path]
		if child.IdentifiedBy != expected || child.From[0].Value.ParentKey != "first_order" {
			t.Fatalf("%s: %+v", path, child)
		}
	}
	if len(wire.Children["Matching"].From[0].Value.Properties) != 0 {
		t.Fatal("matching key should rely on AutoMap")
	}
	if wire.Children["Key"].From[0].Value.Properties["Key"] != "ItemID" {
		t.Fatal("child identity not mapped")
	}
	if len(definition.Diagnostics()) != 5 {
		t.Fatalf("missing ambiguity diagnostics: %+v", definition.Diagnostics())
	}
}

type SameIDEvent struct{ ID OrderID }
type OnlyChildKeyOrder struct {
	ID    OrderID
	Items []OrderLine `chronicle:"children(SameIDEvent,key=ID)"`
}

func TestParentInferenceExcludesChildKey(t *testing.T) {
	e := mustEvent[SameIDEvent](t)
	d := mustCompile(t, projections.ModelBound(mustModel[OnlyChildKeyOrder](t)), e.Descriptor())
	if d.KernelDefinition().Children["Items"].From[0].Value.ParentKey != "$eventSourceId" {
		t.Fatal("child key reused as parent")
	}
}

type domainID[T ~string] struct{ Value T }

func (domainID[T]) ConceptValue() string               { panic("metadata discovery must not execute concepts") }
func (v domainID[T]) MarshalJSON() ([]byte, error)     { return json.Marshal(v.Value) }
func (v *domainID[T]) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &v.Value) }
func (v domainID[T]) MarshalText() ([]byte, error)     { return []byte(v.Value), nil }
func (v *domainID[T]) UnmarshalText(data []byte) error { v.Value = T(data); return nil }

type ConceptOrderID = domainID[OrderID]
type ConceptLineID = domainID[LineID]
type ConceptItemEvent struct {
	ItemID ConceptLineID
	Wrong  ConceptLineID
	Parent ConceptOrderID
}
type ConceptChild struct{ ID ConceptLineID }
type ConceptParent struct {
	ID    ConceptOrderID
	Items []ConceptChild `chronicle:"children(ConceptItemEvent,key=ItemID)"`
}

func TestParentInferencePreservesDeclaredConceptTypes(t *testing.T) {
	e := mustEvent[ConceptItemEvent](t)
	d := mustCompile(t, projections.ModelBound(mustModel[ConceptParent](t)), e.Descriptor())
	if d.KernelDefinition().Children["Items"].From[0].Value.ParentKey != "Parent" {
		t.Fatal("underlying concept types used for identity inference")
	}
}

type SubItemAdded struct {
	ID     string
	Parent string
}
type SubItemRemoved struct {
	ID     string
	Parent string
}
type Grandchild struct {
	ID string `chronicle:"key"`
}
type ParentChild struct {
	ID       string       `chronicle:"key"`
	SubItems []Grandchild `chronicle:"children(SubItemAdded,key=ID,parent-key=Parent);remove(SubItemRemoved,key=ID,parent-key=Parent);remove-join(CustomerRemoved,key=source)"`
}
type Grandparent struct {
	Items []ParentChild `chronicle:"children(ItemAdded,key=itemId)"`
}

func TestPropertyLevelGrandchildRemovalAndChildScopes(t *testing.T) {
	catalog, err := events.NewCatalog(mustEvent[ItemAdded](t).Descriptor(), mustEvent[SubItemAdded](t).Descriptor(), mustEvent[SubItemRemoved](t).Descriptor(), mustEvent[CustomerRemoved](t).Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	d := compileCatalog(t, projections.ModelBound(mustModel[Grandparent](t)), catalog).KernelDefinition()
	child := d.Children["Items"]
	grandchild := child.Children["SubItems"]
	if len(d.RemovedWith) != 0 || len(child.RemovedWith) != 0 || len(child.From) != 1 || len(grandchild.RemovedWith) != 1 || len(grandchild.RemovedWithJoin) != 1 || grandchild.RemovedWith[0].Value.Key != "ID" || grandchild.RemovedWith[0].Value.ParentKey != "Parent" {
		t.Fatalf("misrouted grandchild removal: %+v", d)
	}
}

type ChildUpdate struct{ Name string }
type TaggedChild struct {
	ID      string    `chronicle:"key"`
	Name    string    `chronicle:"no-auto;set(ChildUpdate)"`
	Updated time.Time `chronicle:"every(context=occurred)"`
}
type NodeParent struct {
	Name    string        `chronicle:"no-auto"`
	Updated time.Time     `chronicle:"every(context=occurred)"`
	Items   []TaggedChild `chronicle:"children(ItemAdded,key=itemId)"`
	Other   []TaggedChild `chronicle:"children(ItemAdded,key=itemId)"`
}

func TestNodesKeepExclusionsAndSubscriptionsLocalAndShareEveryNames(t *testing.T) {
	item, update := mustEvent[ItemAdded](t), mustEvent[ChildUpdate](t)
	catalog, err := events.NewCatalog(item.Descriptor(), update.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	definition := compileCatalog(t, projections.ModelBound(mustModel[NodeParent](t), projections.NoAutoMap(), projections.WithNodes(projections.Node[TaggedChild](projections.FromEvent(update)))), catalog)
	d := definition.KernelDefinition()
	if len(d.From) != 0 || len(d.All.Properties) != 1 || !d.All.IncludeChildren || len(definition.Diagnostics()) != 2 {
		t.Fatalf("globals: %+v diagnostics: %+v", d, definition.Diagnostics())
	}
	for _, name := range []string{"Items", "Other"} {
		child := d.Children[name]
		if child.AutoMap.String() != "Disabled" || len(child.From) != 2 || len(child.NoAutoMapProperties) != 1 || child.NoAutoMapProperties[0] != "Name" || child.From[0].Value.Properties["Name"] != "Name" {
			t.Fatalf("%s: %+v", name, child)
		}
	}
}

type JoinOverlap struct {
	CustomerID string
	Name       string `chronicle:"set(ChildUpdate);join(CustomerRenamed,on=CustomerID,from=name)"`
	Other      string `chronicle:"join(CustomerRenamed,on=Name,from=name)"`
}

func TestJoinFirstOnAndLocalWriteDiagnostics(t *testing.T) {
	catalog, err := events.NewCatalog(mustEvent[ChildUpdate](t).Descriptor(), mustEvent[CustomerRenamed](t).Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	d := compileCatalog(t, projections.ModelBound(mustModel[JoinOverlap](t)), catalog)
	wire := d.KernelDefinition()
	if wire.Join[0].Value.On != "CustomerID" || len(wire.Join[0].Value.Properties) != 2 || len(d.Diagnostics()) != 2 {
		t.Fatalf("%+v diagnostics %+v", wire, d.Diagnostics())
	}
}

type EveryAll struct {
	Updated  *time.Time `chronicle:"all(context=occurred)"`
	Observed *time.Time `chronicle:"every(context=occurred)"`
}

func TestFromAllAndFromEveryWithoutExplicitSubscriptions(t *testing.T) {
	catalog, err := events.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	d := compileCatalog(t, projections.ModelBound(mustModel[EveryAll](t)), catalog).KernelDefinition()
	if !d.SubscribesToAllEvents || len(d.FromEvery) != 0 || len(d.From) != 0 || !d.All.IncludeChildren || len(d.All.Properties) != 2 {
		t.Fatalf("%+v", d)
	}
}

type DefaultNamesLine struct {
	ID   string `chronicle:"key"`
	Name string
}
type DefaultNamesAddress struct {
	Street string `chronicle:"set(DefaultNamesEvent)"`
}
type DefaultNamesEvent struct {
	ItemID     string
	OrderID    string
	CustomerID string
	Name       string
	Amount     int32
	Street     string
}
type DefaultNamesOrder struct {
	ID         string
	CustomerID string
	Name       string               `chronicle:"join(DefaultNamesEvent,on=CustomerID,from=Name)"`
	Amount     int32                `chronicle:"add(DefaultNamesEvent)"`
	Items      []DefaultNamesLine   `chronicle:"children(DefaultNamesEvent,key=ItemID,parent-key=OrderID);remove(DefaultNamesEvent,key=ItemID,parent-key=OrderID)"`
	Address    *DefaultNamesAddress `chronicle:"nested;clear(DefaultNamesEvent)"`
	Stamp      time.Time            `chronicle:"every(context=occurred)"`
}

func TestNodeRebindingUsesSerializationMetadataEverywhere(t *testing.T) {
	event := mustEvent[DefaultNamesEvent](t)
	model := mustModel[DefaultNamesOrder](t)
	before, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	d := compileCatalog(t, projections.ModelBound(model), before)
	camelEvent, err := event.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	camelModel, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	after, err := events.NewCatalog(camelEvent)
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := d.Rebind(camelModel, before, after)
	if err != nil {
		t.Fatal(err)
	}
	wire := rebound.KernelDefinition()
	if wire.From[0].Value.Properties["amount"] != "$add(amount)" || wire.Join[0].Value.On != "customerID" || wire.Join[0].Value.Properties["name"] != "name" || wire.Children["items"].From[0].Value.ParentKey != "orderID" || wire.Children["items"].RemovedWith[0].Value.Key != "itemID" || wire.Nested["address"].From[0].Value.Properties["street"] != "street" || wire.All.Properties["stamp"] != "$eventContext(occurred)" {
		t.Fatalf("%+v", wire)
	}
	if d.KernelDefinition().Children["Items"].From[0].Value.ParentKey != "OrderID" {
		t.Fatal("rebind mutated original")
	}
	twice, err := d.Rebind(camelModel, before, after)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(twice.KernelDefinition(), wire) {
		t.Fatal("nondeterministic rebind")
	}
	for _, p := range rebound.Provenance() {
		if strings.Contains(p.Path, "Items") {
			t.Fatalf("stale provenance: %+v", p)
		}
	}
}
