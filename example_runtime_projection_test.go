// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type StockReceived struct {
	Quantity int `json:"quantity"`
}
type StockView struct {
	ID       string `json:"id" chronicle:"key"`
	Quantity int    `json:"quantity" chronicle:"set(StockReceived)"`
}

// This example requires a local development kernel. Tests compile it; the
// kernel integration suite separately exercises registration/materialization.
func ExampleEventStore_RegisterProjection() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[StockReceived](registry); err != nil {
		panic(err)
	}
	client, err := chronicle.Dial(ctx, chronicle.WithRegistry(registry), chronicle.WithDevelopmentDefaults())
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}
	}()
	store, err := client.EventStore(ctx, "stock")
	if err != nil {
		panic(err)
	}

	model, err := readmodels.Define[StockView]()
	if err != nil {
		panic(err)
	}
	registration, err := store.RegisterProjection(ctx, projections.ModelBound(model))
	if err != nil {
		// Published=true means the addition is retained, not rolled back.
		// Retry registration with store.WaitForRegistration using a fresh context;
		// do not resubmit this declaration or blindly retry application appends.
		fmt.Println("published:", registration.Published, "registration error:", err)
		return
	}
	if !registration.Outcome.IsSuccess() {
		panic("registration not acknowledged")
	}
	result, err := store.EventLog().Append(ctx, "item-1", StockReceived{Quantity: 3})
	if err != nil {
		panic(err)
	}
	if err := result.Err(); err != nil {
		panic(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	instance, err := reader.Get(ctx, "item-1")
	if err != nil {
		panic(err)
	}
	// Projection sinks are asynchronous: one read is not an observer SLA.
	fmt.Println("materialized:", instance.Exists)
}
