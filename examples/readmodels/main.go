// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command readmodels registers a catalog and distinguishes absent model state.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
)

// InventoryItem is materialized by a separately registered projection or reducer.
type InventoryItem struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Quantity    int    `json:"quantity"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (err error) {
	registry := chronicle.NewRegistry()
	model, err := chronicle.RegisterReadModel[InventoryItem](registry, readmodels.WithIdentifier("Example.InventoryItem"), readmodels.WithIndexes("description"))
	if err != nil {
		return err
	}
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	// Development only: use WithTLS with validated certificates in production.
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	id, err := metadata.NewCorrelationID()
	if err != nil {
		return err
	}
	store, err := client.EventStore(ctx, chronicle.StoreName("models-"+id.String()), chronicle.WithNamespace("demo"))
	if err != nil {
		return err
	}
	reader := readmodels.For(store.ReadModels(), model)
	instance, err := reader.Get(ctx, "item-1")
	if err != nil {
		return err
	}
	if instance.Exists {
		return fmt.Errorf("new example store unexpectedly contains item-1")
	}
	fmt.Printf("%s registered; item-1 exists: %t\n", model.Identifier(), instance.Exists)
	// Registering a model does not populate it. Do not mistake instance.Value's
	// zero value for an existing inventory item with zero quantity.
	return nil
}
