// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command batches protects loaded history while appending one ordered atomic batch.
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
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/google/uuid"
)

// ItemRecorded records one value; its event source is carried as metadata.
type ItemRecorded struct {
	Value string `json:"value"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint),
		chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	store, err := client.EventStore(ctx, chronicle.StoreName("batch-example-"+uuid.NewString()))
	if err != nil {
		return err
	}
	sequence := store.EventLog()
	history, err := sequence.ReadHistory(ctx, "A", eventsequences.SourceFilter{})
	if err != nil {
		return err
	}
	result, err := sequence.AppendBatch(ctx, []eventsequences.Entry{
		{Source: "A", Event: ItemRecorded{Value: "A1"}},
		{Source: "B", Event: ItemRecorded{Value: "B1"}},
		{Source: "A", Event: ItemRecorded{Value: "A2"}},
	}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: history.Scope()}))
	if err != nil {
		return err // The write may have committed. Do not blindly retry.
	}
	if err = result.Err(); err != nil {
		return err
	}
	loaded, err := sequence.ReadSource(ctx, "A", eventsequences.SourceFilter{})
	if err != nil {
		return err
	}
	if len(loaded) != 2 || len(result.Positions) != 3 {
		return fmt.Errorf("unexpected batch/history sizes: %d/%d", len(result.Positions), len(loaded))
	}
	fmt.Printf("Batch positions: %v; A history positions: %d, %d\n",
		result.Positions, loaded[0].Context.SequenceNumber, loaded[1].Context.SequenceNumber)
	return nil
}
