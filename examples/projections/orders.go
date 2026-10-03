// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

// These declarations are exercised by orders_test.go without a running kernel.
// The materialization workflow is covered by internal/integration/projection_nodes_test.go.

// orders-events:start
type OrderPlaced struct {
	CustomerID string `json:"customerId"`
}

type LineAdded struct {
	OrderID string `json:"orderId"`
	LineID  string `json:"lineId"`
	Name    string `json:"name"`
	Amount  int32  `json:"amount"`
}

type CustomerRenamed struct {
	Name string `json:"name"`
}

// orders-events:end

// orders-model-bound:start
type Line struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}

type Order struct {
	ID           string `json:"id" chronicle:"key"`
	CustomerID   string `json:"customerId"`
	CustomerName string `json:"customerName" chronicle:"join(CustomerRenamed,on=customerId,from=name)"`
	Total        int32  `json:"total" chronicle:"add(LineAdded,from=amount)"`
	Lines        []Line `json:"lines" chronicle:"children(LineAdded,key=lineId,identified-by=id,parent-key=orderId)"`
}

func modelBoundOrder(model readmodels.Model[Order], placed events.Type[OrderPlaced]) projections.Declaration {
	return projections.ModelBound(model, projections.WithIdentifier("orders"), projections.FromEvent(placed))
}

// orders-model-bound:end

// orders-fluent:start
type FluentOrder struct {
	ID           string `json:"id" chronicle:"key"`
	CustomerID   string `json:"customerId"`
	CustomerName string `json:"customerName"`
	Total        int32  `json:"total"`
	Lines        []Line `json:"lines"`
}

func fluentOrder(model readmodels.Model[FluentOrder], placed events.Type[OrderPlaced], added events.Type[LineAdded], renamed events.Type[CustomerRenamed]) (projections.Declaration, error) {
	builder := projections.NewBuilder("orders", model)
	projections.From(builder, placed, nil)
	projections.From(builder, added, func(from *projections.FromBuilder[FluentOrder, LineAdded]) {
		projections.Add(from, projections.Path[FluentOrder, int32]("total"), projections.Path[LineAdded, int32]("amount"))
	})
	projections.Children(builder, projections.Path[FluentOrder, []Line]("lines"), func(child *projections.Builder[Line]) {
		projections.From(child, added, nil,
			projections.UsingKey(projections.Path[LineAdded, string]("lineId")),
			projections.UsingParentKey(projections.Path[LineAdded, string]("orderId")))
	}, projections.IdentifiedBy(projections.Path[Line, string]("id")))
	projections.Join(builder, renamed, projections.Path[FluentOrder, string]("customerId"), func(join *projections.FromBuilder[FluentOrder, CustomerRenamed]) {
		projections.Map(join, projections.Path[FluentOrder, string]("customerName"), projections.Path[CustomerRenamed, string]("name"))
	})
	return builder.Build()
}

// orders-fluent:end
