// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"os"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type OrderOpened struct {
	CustomerID string `json:"customerId"`
}
type QuantityAdded struct {
	Amount int32 `json:"amount"`
}
type QuantityRemoved struct {
	Amount int32 `json:"amount"`
}
type ItemAdded struct {
	ItemID  string `json:"itemId"`
	OrderID string `json:"orderId"`
	Name    string `json:"name"`
}
type ItemRemoved struct {
	ItemID  string `json:"itemId"`
	OrderID string `json:"orderId"`
}
type CustomerRenamed struct {
	Name string `json:"name"`
}
type AddressChanged struct {
	Street string `json:"street"`
}
type AddressCleared struct{}
type OrderRemoved struct{}
type CustomerRemoved struct{}

type OrderLine struct {
	ID       string `json:"id" chronicle:"key"`
	Name     string `json:"name"`
	Internal string `json:"internal" chronicle:"no-auto"`
}
type OrderAddress struct {
	Street string `json:"street"`
}
type AdvancedOrder struct {
	ID           string        `json:"id" chronicle:"key"`
	CustomerID   string        `json:"customerId"`
	CustomerName string        `json:"customerName" chronicle:"join(CustomerRenamed,on=customerId,from=name)"`
	Quantity     int32         `json:"quantity" chronicle:"add(QuantityAdded,from=amount);subtract(QuantityRemoved,from=amount)"`
	Increases    int32         `json:"increases" chronicle:"increment(QuantityAdded)"`
	Decreases    int32         `json:"decreases" chronicle:"decrement(QuantityRemoved)"`
	Count        int64         `json:"count" chronicle:"count(QuantityAdded)"`
	Note         *string       `json:"note" chronicle:"clear(QuantityRemoved)"`
	Items        []OrderLine   `json:"items" chronicle:"children(ItemAdded,key=itemId,identified-by=id,parent-key=orderId);remove(ItemRemoved,key=itemId,parent-key=orderId);remove-join(CustomerRemoved)"`
	Address      *OrderAddress `json:"address" chronicle:"nested;clear(AddressCleared)"`
	Observed     time.Time     `json:"observed" chronicle:"every(context=occurred)"`
	Updated      time.Time     `json:"updated" chronicle:"all(context=occurred)"`
}
type FluentOrder struct {
	ID           string        `json:"id" chronicle:"key"`
	CustomerID   string        `json:"customerId"`
	CustomerName string        `json:"customerName"`
	Quantity     int32         `json:"quantity"`
	Increases    int32         `json:"increases"`
	Decreases    int32         `json:"decreases"`
	Count        int64         `json:"count"`
	Note         *string       `json:"note"`
	Items        []OrderLine   `json:"items"`
	Address      *OrderAddress `json:"address"`
	Observed     time.Time     `json:"observed"`
	Updated      time.Time     `json:"updated"`
}

func advancedEvents(t *testing.T) *events.Catalog {
	t.Helper()
	catalog, err := events.NewCatalog(
		mustEvent[OrderOpened](t, events.WithID("opened")).Descriptor(),
		mustEvent[QuantityAdded](t, events.WithID("added")).Descriptor(),
		mustEvent[QuantityRemoved](t, events.WithID("subtracted")).Descriptor(),
		mustEvent[ItemAdded](t, events.WithID("item-added")).Descriptor(),
		mustEvent[ItemRemoved](t, events.WithID("item-removed")).Descriptor(),
		mustEvent[CustomerRenamed](t, events.WithID("customer-renamed")).Descriptor(),
		mustEvent[AddressChanged](t, events.WithID("address-changed")).Descriptor(),
		mustEvent[AddressCleared](t, events.WithID("address-cleared")).Descriptor(),
		mustEvent[OrderRemoved](t, events.WithID("order-removed")).Descriptor(),
		mustEvent[CustomerRemoved](t, events.WithID("customer-removed")).Descriptor(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
func compileCatalog(t *testing.T, d projections.Declaration, catalog *events.Catalog) projections.Definition {
	t.Helper()
	definition, err := projections.Compile(d, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}
func advancedBound(t *testing.T) projections.Declaration {
	t.Helper()
	model := mustModel[AdvancedOrder](t, readmodels.WithIdentifier("Example.Order"))
	return projections.ModelBound(model, projections.WithIdentifier("Example.OrderProjection"),
		projections.FromEvent(mustEvent[OrderOpened](t, events.WithID("opened"))),
		projections.RemovedWith(mustEvent[OrderRemoved](t, events.WithID("order-removed"))),
		projections.RemovedWithJoin(mustEvent[CustomerRemoved](t, events.WithID("customer-removed"))),
		projections.WithNodes(projections.Node[OrderAddress](projections.FromEvent(mustEvent[AddressChanged](t, events.WithID("address-changed"))))))
}
func advancedFluent(t *testing.T) projections.Declaration {
	t.Helper()
	model := mustModel[FluentOrder](t, readmodels.WithIdentifier("Example.Order"))
	builder := projections.NewBuilder("Example.OrderProjection", model,
		projections.RemovedWith(mustEvent[OrderRemoved](t, events.WithID("order-removed"))),
		projections.RemovedWithJoin(mustEvent[CustomerRemoved](t, events.WithID("customer-removed"))))
	projections.From(builder, mustEvent[OrderOpened](t, events.WithID("opened")), nil)
	projections.From(builder, mustEvent[QuantityAdded](t, events.WithID("added")), func(f *projections.FromBuilder[FluentOrder, QuantityAdded]) {
		projections.Add(f, projections.Path[FluentOrder, int32]("quantity"), projections.Path[QuantityAdded, int32]("amount"))
		projections.Increment(f, projections.Path[FluentOrder, int32]("increases"))
		projections.Count(f, projections.Path[FluentOrder, int64]("count"))
	})
	projections.From(builder, mustEvent[QuantityRemoved](t, events.WithID("subtracted")), func(f *projections.FromBuilder[FluentOrder, QuantityRemoved]) {
		projections.Subtract(f, projections.Path[FluentOrder, int32]("quantity"), projections.Path[QuantityRemoved, int32]("amount"))
		projections.Decrement(f, projections.Path[FluentOrder, int32]("decreases"))
		projections.Clear(f, projections.Path[FluentOrder, *string]("note"))
	})
	projections.Join(builder, mustEvent[CustomerRenamed](t, events.WithID("customer-renamed")), projections.Path[FluentOrder, string]("customerId"), func(f *projections.FromBuilder[FluentOrder, CustomerRenamed]) {
		projections.Map(f, projections.Path[FluentOrder, string]("customerName"), projections.Path[CustomerRenamed, string]("name"))
	})
	projections.Children(builder, projections.Path[FluentOrder, []OrderLine]("items"), func(child *projections.Builder[OrderLine]) {
		projections.From(child, mustEvent[ItemAdded](t, events.WithID("item-added")), nil, projections.UsingKey(projections.Path[ItemAdded, string]("itemId")), projections.UsingParentKey(projections.Path[ItemAdded, string]("orderId")))
		child.Configure(projections.RemovedWith(mustEvent[ItemRemoved](t, events.WithID("item-removed")), projections.UsingKey(projections.Path[ItemRemoved, string]("itemId")), projections.UsingParentKey(projections.Path[ItemRemoved, string]("orderId"))), projections.RemovedWithJoin(mustEvent[CustomerRemoved](t, events.WithID("customer-removed"))))
	}, projections.IdentifiedBy(projections.Path[OrderLine, string]("id")))
	projections.Nested(builder, projections.Path[FluentOrder, *OrderAddress]("address"), func(child *projections.Builder[OrderAddress]) {
		projections.From(child, mustEvent[AddressChanged](t, events.WithID("address-changed")), nil)
	}, projections.ClearWith(mustEvent[AddressCleared](t, events.WithID("address-cleared"))))
	projections.Every(builder, func(e *projections.EveryBuilder[FluentOrder]) {
		projections.EveryContext(e, projections.Path[FluentOrder, time.Time]("observed"), "occurred")
	})
	projections.All(builder, func(e *projections.EveryBuilder[FluentOrder]) {
		projections.EveryContext(e, projections.Path[FluentOrder, time.Time]("updated"), "occurred")
	})
	d, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestFrontEndsMatchTheirCSharpAdvancedShapes(t *testing.T) {
	catalog := advancedEvents(t)
	data, err := os.ReadFile("testdata/advanced.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := &contracts.ProjectionDefinition{}
	if err = protojson.Unmarshal(data, expected); err != nil {
		t.Fatal(err)
	}
	for i, declaration := range []projections.Declaration{advancedBound(t), advancedFluent(t)} {
		want := proto.Clone(expected).(*contracts.ProjectionDefinition)
		if i == 1 {
			// C# fluent Children/Nested default to Inherit; only model-bound
			// ChildrenFrom synthesizes an identity mapping.
			want.Children["items"].AutoMap = contracts.AutoMap_Inherit
			want.Children["items"].From[0].Value.Properties = nil
			want.Nested["address"].AutoMap = contracts.AutoMap_Inherit
		}
		definition := compileCatalog(t, declaration, catalog)
		actual := definition.KernelDefinition()
		if !proto.Equal(actual, want) {
			t.Fatalf("got %s\nwant %s", protojson.Format(actual), protojson.Format(want))
		}
		actual.Children["items"].From[0].Value.Properties = map[string]string{"id": "corrupted"}
		actual.Join[0].Value.Properties["customerName"] = "corrupted"
		actual.Nested["address"].RemovedWith = nil
		actual.All.Properties["observed"] = "corrupted"
		if !proto.Equal(definition.KernelDefinition(), want) {
			t.Fatal("mutable node definition")
		}
	}
}
