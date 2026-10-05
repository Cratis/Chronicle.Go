// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command projections materializes an inventory read model from an event.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

// model-bound:start
// ProductRegistered records the initial product description.
type ProductRegistered struct {
	ProductName string `json:"productName"`
	Description string `json:"description"`
}

// Inventory is maintained by Chronicle, not by an in-process event handler.
type Inventory struct {
	ID          string    `json:"id" chronicle:"key"`
	ProductName string    `json:"productName"`
	Description string    `json:"description" chronicle:"set(@registered)"`
	State       string    `json:"state" chronicle:"value(@registered,value=\"available\")"`
	Registered  time.Time `json:"registered" chronicle:"context(@registered,from=occurred)"`
}

func declarations() (*chronicle.Registry, readmodels.Model[Inventory], error) {
	registry := chronicle.NewRegistry()
	registered, err := chronicle.RegisterEvent[ProductRegistered](registry, events.WithID("product-registered"))
	if err != nil {
		return nil, readmodels.Model[Inventory]{}, err
	}
	model, err := chronicle.RegisterReadModel[Inventory](registry)
	if err != nil {
		return nil, model, err
	}
	projection := projections.ModelBound(model, projections.BindEvent("registered", registered), projections.FromEvent(registered))
	return registry, model, registry.AddProjection(projection)
}

// model-bound:end

// fluent:start
// FluentInventory uses the same mappings without model-bound mapping tags.
type FluentInventory struct {
	ID          string    `json:"id" chronicle:"key"`
	ProductName string    `json:"productName"`
	Description string    `json:"description"`
	State       string    `json:"state"`
	Registered  time.Time `json:"registered"`
}

func fluentDeclarations() (*chronicle.Registry, readmodels.Model[FluentInventory], error) {
	registry := chronicle.NewRegistry()
	registered, err := chronicle.RegisterEvent[ProductRegistered](registry, events.WithID("product-registered"))
	if err != nil {
		return nil, readmodels.Model[FluentInventory]{}, err
	}
	model, err := chronicle.RegisterReadModel[FluentInventory](registry)
	if err != nil {
		return nil, model, err
	}
	builder := projections.NewBuilder("inventory", model)
	projections.From(builder, registered, func(from *projections.FromBuilder[FluentInventory, ProductRegistered]) {
		projections.Map(from, projections.Path[FluentInventory, string]("description"), projections.Path[ProductRegistered, string]("description"))
		projections.Context(from, projections.Path[FluentInventory, time.Time]("registered"), "occurred")
		projections.Value(from, projections.Path[FluentInventory, string]("state"), "available")
	})
	declaration, err := builder.Build()
	if err != nil {
		return nil, model, err
	}
	return registry, model, registry.AddProjection(declaration)
}

// fluent:end

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}
func run(ctx context.Context) (err error) {
	registry, model, err := declarations()
	if err != nil {
		return err
	}
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	// Development only. Use validated TLS and production credentials elsewhere.
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	id, err := metadata.NewCorrelationID()
	if err != nil {
		return err
	}
	store, err := client.EventStore(ctx, chronicle.StoreName("projections-"+id.String()))
	if err != nil {
		return err
	}
	appended, err := store.EventLog().Append(ctx, "item-1", ProductRegistered{ProductName: "Notebook", Description: "Plain paper"})
	if err != nil {
		return err
	}
	if err = appended.Err(); err != nil {
		return err
	}
	reader := readmodels.For(store.ReadModels(), model)
	wait, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		instance, err := reader.Get(wait, "item-1")
		if err != nil {
			return err
		}
		if instance.Exists && instance.Value.State == "available" {
			fmt.Printf("%s: %s (%s)\n", instance.Value.ID, instance.Value.ProductName, instance.Value.State)
			return nil
		}
		select {
		case <-wait.Done():
			return wait.Err()
		case <-ticker.C:
		}
	}
}
