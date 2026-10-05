// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command constraints reserves an address, rejects a competing claim, then releases it.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

// EmailReserved claims an address for the event source.
type EmailReserved struct {
	Email string `json:"email"`
}

// EmailReleased frees the appending source's address claim.
type EmailReleased struct{}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func declarations() (*chronicle.Registry, error) {
	registry := chronicle.NewRegistry()
	reserved, err := chronicle.RegisterEvent[EmailReserved](registry, events.WithID("email-reserved"))
	if err != nil {
		return nil, err
	}
	released, err := chronicle.RegisterEvent[EmailReleased](registry, events.WithID("email-released"))
	if err != nil {
		return nil, err
	}
	unique, err := constraints.UniqueValues("UniqueEmail").
		On(reserved.Descriptor(), "email").
		IgnoreCasing().
		RemovedWith(released.Descriptor()).
		WithMessage("That address is already reserved ({PropertyName}).").
		Build()
	if err != nil {
		return nil, err
	}
	if err = registry.AddConstraint(unique); err != nil {
		return nil, err
	}
	return registry, nil
}

func run(ctx context.Context) (err error) {
	registry, err := declarations()
	if err != nil {
		return err
	}
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint),
		chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	store, err := client.EventStore(ctx, chronicle.StoreName("constraints-example-"+rand.Text()))
	if err != nil {
		return err
	}
	sequence := store.EventLog()
	first, err := sequence.Append(ctx, "owner", EmailReserved{Email: "ada@example.test"})
	if err != nil {
		return err // An unknown outcome must not be retried blindly.
	}
	if err = first.Err(); err != nil {
		return err
	}
	second, err := sequence.Append(ctx, "competitor", EmailReserved{Email: "ADA@example.test"})
	if err != nil {
		return err
	}
	var violation *eventsequences.ConstraintError
	if second.Disposition != eventsequences.Rejected || !errors.As(second.Err(), &violation) || len(violation.Violations) != 1 || violation.Violations[0].ConstraintName != "UniqueEmail" {
		return fmt.Errorf("expected UniqueEmail rejection")
	}
	fmt.Println("Competing claim rejected: UniqueEmail")
	history, err := sequence.ReadSource(ctx, "competitor", eventsequences.SourceFilter{})
	if err != nil {
		return err
	}
	if len(history) != 0 {
		return fmt.Errorf("rejected event was persisted")
	}
	removal, err := sequence.Append(ctx, "owner", EmailReleased{})
	if err != nil {
		return err
	}
	if err = removal.Err(); err != nil {
		return err
	}
	reclaimed, err := sequence.Append(ctx, "competitor", EmailReserved{Email: "ADA@example.test"})
	if err != nil {
		return err
	}
	if err = reclaimed.Err(); err != nil {
		return err
	}
	fmt.Println("Owner released; new claim committed")
	return nil
}
