// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services_test

import (
	"context"
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/seeding"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type exampleClientConfiguration struct{ client *chronicle.Client }

func (*exampleClientConfiguration) Seed(*seeding.Builder) error { return nil }

func ExamplePrepareClient() {
	ctx := context.Background()
	registry := chronicle.NewRegistry()
	if err := chronicle.RegisterSeederFactory[*exampleClientConfiguration](registry, nil); err != nil {
		panic(err)
	}
	p, err := chronicle.CaptureClient(chronicle.WithRegistry(registry))
	if err != nil {
		panic(err)
	}
	var provider dependencyinjection.Provider
	defer func() {
		if err := p.Client().Close(); err != nil {
			panic(err)
		}
		if provider != nil {
			if err := provider.Close(ctx); err != nil {
				panic(err)
			}
		}
	}()
	// This identity must be borrowed, never constructed or owned by the provider.
	var bindings container.Registry
	if err := dependencyinjection.BindValue(&bindings, p.Client()); err != nil {
		panic(err)
	}
	if err := dependencyinjection.BindFunc1(&bindings, dependencyinjection.Scoped,
		func(_ context.Context, client *chronicle.Client) (*exampleClientConfiguration, error) {
			return &exampleClientConfiguration{client: client}, nil
		}); err != nil {
		panic(err)
	}
	provider, err = bindings.Build()
	if err != nil {
		panic(err)
	}
	// This synchronous call joins preparation, including cleanup, even on error.
	// Shutdown above closes the client first, then the provider.
	client, err := services.PrepareClient(ctx, p, provider)
	if err != nil {
		panic(err)
	}
	catalog, _, err := client.Catalogs("orders")
	if err != nil {
		panic(err)
	}
	fmt.Println(client == p.Client(), len(catalog.Descriptors()))
	// Output: true 0
}
