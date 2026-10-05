// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command getting-started appends one event to a local development kernel.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

// CustomerRegistered records a customer's display name. The source ID is metadata,
// not a payload field. Use explicit JSON names for cross-language persistence.
type CustomerRegistered struct {
	Name string `json:"name"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := appendCustomer(ctx); err != nil {
		log.Fatal(err)
	}
}

func appendCustomer(ctx context.Context) (err error) {
	registry := chronicle.NewRegistry()
	if _, err = chronicle.RegisterEvent[CustomerRegistered](registry,
		events.WithID("customer-registered")); err != nil {
		return err
	}
	client, err := chronicle.NewClient(chronicle.WithDevelopmentDefaults(),
		chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		return err
	}
	result, err := store.EventLog().Append(ctx, "customer-42", CustomerRegistered{Name: "Ada"})
	if err != nil {
		return err
	} // A transport failure may have committed. Do not blindly retry.
	if err = result.Err(); err != nil {
		return err
	}
	fmt.Printf("Appended event at position %d\n", *result.Position)
	return nil
}
