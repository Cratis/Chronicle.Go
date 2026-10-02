// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command unitofwork stages nested work in one ordered atomic transaction.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

// ItemRecorded describes one item update.
type ItemRecorded struct {
	Value string `json:"value"`
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
	if _, err = chronicle.RegisterEvent[ItemRecorded](registry, events.WithID("item-recorded")); err != nil {
		return err
	}
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	id, err := metadata.NewCorrelationID()
	if err != nil {
		return err
	}
	store, err := client.EventStore(ctx, chronicle.StoreName("unit-example-"+id.String()))
	if err != nil {
		return err
	}
	sequence := store.EventLog()
	ctx = metadata.WithCorrelation(ctx, id)
	unit, owner, err := transactions.Begin(ctx, sequence)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, owner.Rollback()) }()
	ctx = transactions.WithUnitOfWork(ctx, unit)
	history, err := sequence.ReadHistory(ctx, "A", eventsequences.SourceFilter{})
	if err != nil {
		return err
	}
	if err = unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: ItemRecorded{Value: "A1"}}}, eventsequences.LabeledScope{Label: "A", Scope: history.Scope()}); err != nil {
		return err
	}
	if err = recordNested(ctx); err != nil {
		return err
	}
	if err = unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: ItemRecorded{Value: "A2"}}}); err != nil {
		return err
	}
	result, err := owner.Commit(ctx)
	if err != nil {
		return err // OutcomeUnknown means reconciliation, never an automatic retry.
	}
	if err = result.Err(); err != nil {
		return err
	}
	loaded, err := sequence.ReadFrom(ctx, 0, eventsequences.FromFilter{})
	if err != nil {
		return err
	}
	if len(loaded) != 3 {
		return fmt.Errorf("expected three events, got %d", len(loaded))
	}
	for i, expected := range []string{"A1", "B1", "A2"} {
		var event ItemRecorded
		if err = json.Unmarshal(loaded[i].Content, &event); err != nil {
			return err
		}
		if event.Value != expected {
			return fmt.Errorf("event %d: got %q, want %q", i, event.Value, expected)
		}
	}
	fmt.Printf("Committed A1, B1, A2 at %v\n", result.Positions)
	return nil
}

func recordNested(ctx context.Context) error {
	unit, ok := transactions.FromContext(ctx)
	if !ok {
		return fmt.Errorf("no unit of work in context")
	}
	return unit.Stage(ctx, []eventsequences.Entry{{Source: "B", Event: ItemRecorded{Value: "B1"}}})
}
