// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Run against a local development kernel: go run ./examples/reactors.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

type OrderPlaced struct {
	Product string `json:"product"`
}

func run(ctx context.Context) (err error) {
	registry := chronicle.NewRegistry()
	if _, err = chronicle.RegisterEvent[OrderPlaced](registry); err != nil {
		return err
	}
	handled := make(chan string, 1)
	if err = chronicle.RegisterReactorHandler(registry, "plain-go-orders", func(ctx context.Context, event OrderPlaced) error {
		select {
		case handled <- event.Product:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		return err
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithDevelopmentDefaults())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	store, err := client.EventStore(ctx, "plain-go-reactors", chronicle.WithNamespace(chronicle.Namespace(fmt.Sprintf("example-%d", time.Now().UnixNano()))))
	if err != nil {
		return err
	}
	result, err := store.EventLog().Append(ctx, events.SourceID("order-1"), OrderPlaced{Product: "Chronicle"})
	if err != nil {
		return err
	}
	if err = result.Err(); err != nil {
		return err
	}
	select {
	case product := <-handled:
		fmt.Println("Handled order for", product)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
