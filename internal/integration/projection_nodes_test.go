//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type NodeOrderPlaced struct {
	CustomerID string `json:"customerId"`
}
type NodeQuantityAdded struct {
	Amount int32 `json:"amount"`
}
type NodeQuantityRemoved struct {
	Amount int32 `json:"amount"`
}
type NodeItemAdded struct {
	ItemID  string `json:"itemId"`
	OrderID string `json:"orderId"`
	Name    string `json:"name"`
}
type NodeItemRemoved struct {
	ItemID  string `json:"itemId"`
	OrderID string `json:"orderId"`
}
type NodeItemRemovedEverywhere struct {
	ItemID string `json:"itemId"`
}
type NodeCustomerRenamed struct {
	Name string `json:"name"`
}
type NodeAddressChanged struct {
	Street string `json:"street"`
}
type NodeAddressCleared struct{}
type NodeOrderRemoved struct{}
type NodeLine struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}
type NodeAddress struct {
	Street string `json:"street" chronicle:"set(NodeAddressChanged)"`
}
type NodeOrder struct {
	ID           string       `json:"id"`
	CustomerID   string       `json:"customerId"`
	CustomerName string       `json:"customerName" chronicle:"join(NodeCustomerRenamed,on=customerId,from=name);value(NodeOrderPlaced,value=\"local\")"`
	Quantity     int32        `json:"quantity" chronicle:"add(NodeQuantityAdded,from=amount);subtract(NodeQuantityRemoved,from=amount)"`
	Increases    int32        `json:"increases" chronicle:"increment(NodeQuantityAdded)"`
	Decreases    int32        `json:"decreases" chronicle:"decrement(NodeQuantityRemoved)"`
	Count        int32        `json:"count" chronicle:"count(NodeQuantityAdded)"`
	Note         *string      `json:"note" chronicle:"value(NodeOrderPlaced,value=\"present\");clear(NodeQuantityRemoved)"`
	Items        []NodeLine   `json:"items" chronicle:"children(NodeItemAdded,key=itemId,parent-key=orderId);remove(NodeItemRemoved,key=itemId,parent-key=orderId);remove-join(NodeItemRemovedEverywhere,key=itemId)"`
	Address      *NodeAddress `json:"address" chronicle:"nested;clear(NodeAddressCleared)"`
	Updated      time.Time    `json:"updated" chronicle:"every(context=occurred)"`
}

func TestKernelProjectionArithmeticChildrenJoinNestedAndClear(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	for _, register := range []func() error{
		func() error { _, e := chronicle.RegisterEvent[NodeOrderPlaced](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[NodeQuantityAdded](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[NodeQuantityRemoved](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[NodeItemAdded](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[NodeItemRemoved](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[NodeItemRemovedEverywhere](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[NodeCustomerRenamed](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[NodeAddressChanged](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[NodeAddressCleared](registry); return e },
	} {
		if err := register(); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := chronicle.RegisterEvent[NodeOrderRemoved](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[NodeOrder](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(projections.ModelBound(model, projections.RemovedWith(removed))); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	order, customer, item := uuid.NewString(), uuid.NewString(), uuid.NewString()
	occurred := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	appendEvent := func(source string, event any) {
		t.Helper()
		result, err := store.EventLog().Append(fixture.ctx, events.SourceID(source), event, eventsequences.WithOccurred(occurred))
		if err != nil {
			t.Fatal(err)
		}
		if err = result.Err(); err != nil {
			t.Fatal(err)
		}
	}
	reader := readmodels.For(store.ReadModels(), model)
	appendEvent(customer, NodeCustomerRenamed{Name: "Ada"})
	appendEvent(order, NodeOrderPlaced{CustomerID: customer})
	first := awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return o.CustomerName == "Ada" && o.Note != nil })
	if first.Value.ID != order || !first.Value.Updated.Equal(occurred) {
		t.Fatalf("initial: %+v", first.Value)
	}
	appendEvent(order, NodeQuantityAdded{Amount: 4})
	appendEvent(order, NodeQuantityRemoved{Amount: 1})
	arithmetic := awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return o.Quantity == 3 && o.Note == nil })
	if arithmetic.Value.Count != 1 || arithmetic.Value.Increases != 1 || arithmetic.Value.Decreases != -1 || arithmetic.Value.CustomerName != "Ada" {
		t.Fatalf("arithmetic and join: %+v", arithmetic.Value)
	}
	appendEvent(order, NodeItemAdded{ItemID: item, OrderID: order, Name: "Book"})
	child := awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return len(o.Items) == 1 })
	if child.Value.Items[0].ID != item || child.Value.Items[0].Name != "Book" {
		t.Fatalf("child: %+v", child.Value)
	}
	appendEvent(customer, NodeCustomerRenamed{Name: "Grace"})
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return o.CustomerName == "Grace" })
	appendEvent(order, NodeAddressChanged{Street: "Main Street"})
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return o.Address != nil && o.Address.Street == "Main Street" })
	appendEvent(order, NodeAddressCleared{})
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return o.Address == nil })
	appendEvent(order, NodeItemRemoved{ItemID: item, OrderID: order})
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return len(o.Items) == 0 && o.CustomerName == "Grace" })
	appendEvent(order, NodeItemAdded{ItemID: item, OrderID: order, Name: "Book"})
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return len(o.Items) == 1 })
	t.Run("remove_join_of_readded_id_child", func(t *testing.T) {
		t.Skip("MongoDB join removal does not translate child id to _id: https://github.com/Cratis/Chronicle/issues/4538")
		result, err := store.EventLog().Append(fixture.ctx, events.SourceID(item), NodeItemRemovedEverywhere{ItemID: item}, eventsequences.WithOccurred(occurred))
		if err != nil {
			t.Fatal(err)
		}
		if err = result.Err(); err != nil {
			t.Fatal(err)
		}
		awaitProjection(t, fixture.ctx, reader, readmodels.Key(order), func(o NodeOrder) bool { return len(o.Items) == 0 })
	})
	appendEvent(order, NodeOrderRemoved{})
	ctx, cancel := context.WithTimeout(fixture.ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		instance, err := reader.Get(ctx, readmodels.Key(order))
		if err != nil {
			t.Fatal(err)
		}
		if !instance.Exists {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("root removal did not materialize", ctx.Err())
		case <-ticker.C:
		}
	}
}

type NodeRemovableOrder struct {
	ID         string `json:"id"`
	CustomerID string `json:"customerId"`
}

func TestKernelProjectionRootRemoval(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	placed, err := chronicle.RegisterEvent[NodeOrderPlaced](registry)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := chronicle.RegisterEvent[NodeOrderRemoved](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[NodeRemovableOrder](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(projections.ModelBound(model, projections.FromEvent(placed), projections.RemovedWith(removed))); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := uuid.NewString()
	reader := readmodels.For(store.ReadModels(), model)
	result, err := store.EventLog().Append(fixture.ctx, events.SourceID(source), NodeOrderPlaced{CustomerID: "customer"})
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Err(); err != nil {
		t.Fatal(err)
	}
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(source), func(v NodeRemovableOrder) bool { return v.CustomerID == "customer" })
	result, err = store.EventLog().Append(fixture.ctx, events.SourceID(source), NodeOrderRemoved{})
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(fixture.ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, err := reader.Get(ctx, readmodels.Key(source))
		if err != nil {
			t.Fatal(err)
		}
		if !value.Exists {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("root removal did not materialize", ctx.Err())
		case <-ticker.C:
		}
	}
}

type NodeAudit struct {
	ID      string    `json:"id"`
	AllAt   time.Time `json:"allAt" chronicle:"all(context=occurred)"`
	EveryAt time.Time `json:"everyAt" chronicle:"every(context=occurred)"`
}

func TestKernelProjectionFromAllAndFromEvery(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[NodeQuantityAdded](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[NodeAudit](registry)
	if err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := uuid.NewString()
	occurred := time.Date(2026, 10, 3, 13, 0, 0, 0, time.UTC)
	result, err := store.EventLog().Append(fixture.ctx, events.SourceID(source), NodeQuantityAdded{Amount: 1}, eventsequences.WithOccurred(occurred))
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Err(); err != nil {
		t.Fatal(err)
	}
	audit := awaitProjection(t, fixture.ctx, readmodels.For(store.ReadModels(), model), readmodels.Key(source), func(a NodeAudit) bool { return a.AllAt.Equal(occurred) && a.EveryAt.Equal(occurred) })
	if audit.Value.ID != source {
		t.Fatal(audit.Value)
	}
}
