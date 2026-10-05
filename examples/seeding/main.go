// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// The seeding example writes reference data to a development kernel. Running it
// again keeps the same seed entries; it does not append duplicate startup events.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/seeding"
)

// begin-seeding-example

type ProductAdded struct {
	Name string
}

type CatalogSeeds struct{}

func (CatalogSeeds) Seed(builder *seeding.Builder) error {
	seeding.For(builder, "catalog-1", ProductAdded{Name: "Notebook"})
	seeding.For(builder.ForNamespace("demo"), "demo-1", ProductAdded{Name: "Demo notebook"})
	return nil
}

func declarations() (*chronicle.Registry, error) {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ProductAdded](registry,
		events.WithID("seeding-example-product-added"), events.WithTags("reference-data")); err != nil {
		return nil, err
	}
	if err := chronicle.RegisterSeeder(registry, CatalogSeeds{}); err != nil {
		return nil, err
	}
	return registry, nil
}

// end-seeding-example

func run(ctx context.Context, endpoint string) (err error) {
	registry, err := declarations()
	if err != nil {
		return err
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	store, err := client.EventStore(ctx, "go-seeding-example", chronicle.WithNamespace("demo"))
	if err != nil {
		return err
	}
	global, err := store.EventLog().ReadSource(ctx, "catalog-1", eventsequences.SourceFilter{})
	if err != nil {
		return err
	}
	local, err := store.EventLog().ReadSource(ctx, "demo-1", eventsequences.SourceFilter{})
	if err != nil {
		return err
	}
	if len(global) != 1 || len(local) != 1 {
		return fmt.Errorf("unexpected seed counts: global=%d demo-only=%d", len(global), len(local))
	}
	fmt.Printf("global=%d demo-only=%d\n", len(global), len(local))
	return nil
}

func main() {
	endpoint := os.Getenv("CHRONICLE_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx, endpoint); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
