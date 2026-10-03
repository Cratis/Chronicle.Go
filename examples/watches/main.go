// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command watches demonstrates best-effort changes and convention-based callbacks.
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
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

// ProductNamed provides the display name for one product.
type ProductNamed struct {
	Name string `json:"name"`
}

// Product is maintained by its model-bound projection, not by a reducer.
type Product struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(ProductNamed)"`
}

// AnnounceProduct demonstrates C#'s name-based Added convention without a container.
type AnnounceProduct struct{ names chan string }

// Added is invoked in a fresh scope after the projection change is received.
func (r *AnnounceProduct) Added(ctx context.Context, product *Product) error {
	select {
	case r.names <- product.Name:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func registry(names chan string, failures chan error) (*chronicle.Registry, readmodels.Model[Product], error) {
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ProductNamed](r); err != nil {
		return nil, readmodels.Model[Product]{}, err
	}
	model, err := chronicle.RegisterReadModel[Product](r)
	if err != nil {
		return nil, model, err
	}
	err = chronicle.RegisterReadModelReactor[*AnnounceProduct](r, model, func() *AnnounceProduct { return &AnnounceProduct{names} }, reactors.WithReadModelErrorHandler(func(ctx context.Context, err error) {
		select {
		case failures <- err:
		case <-ctx.Done():
		}
	}))
	return r, model, err
}
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}
func run(ctx context.Context) (err error) {
	names, failures := make(chan string, 1), make(chan error, 1)
	declarations, model, err := registry(names, failures)
	if err != nil {
		return err
	}
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	// Development credentials and unverified TLS are only for your local kernel.
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(declarations))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	id, err := metadata.NewCorrelationID()
	if err != nil {
		return err
	}
	store, err := client.EventStore(ctx, chronicle.StoreName("watches-"+id.String()), chronicle.WithNamespace("demo"))
	if err != nil {
		return err
	}
	reader := readmodels.For(store.ReadModels(), model)
	watch, err := reader.Watch(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, watch.Close()) }()
	// Subscribed is a barrier for future changes, NOT a current model snapshot.
	result, err := store.EventLog().Append(ctx, events.SourceID("product-1"), ProductNamed{Name: "Coffee"})
	if err != nil {
		return err
	}
	if err = result.Err(); err != nil {
		return err
	}
	change, err := watch.Recv()
	if err != nil {
		return err
	}
	if change.Type != readmodels.Added || !change.HasValue || change.Value.Name != "Coffee" {
		return fmt.Errorf("unexpected model change")
	}
	fmt.Printf("watch: %s = %s\n", change.Key, change.Value.Name)
	select {
	case name := <-names:
		fmt.Printf("reactor: %s\n", name)
	case err := <-failures:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
	window, err := reader.Materialized().ObserveInstances(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, window.Close()) }()
	page, err := window.Recv()
	if err != nil {
		return err
	}
	fmt.Printf("materialized window: %d product\n", len(page))
	return nil
}
