//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
)

type ReactorOrderPlaced struct {
	Number int `json:"number"`
}
type ReactorOrderAccepted struct {
	Number int `json:"number"`
}
type KernelReactor struct{ handled chan int }

func (r *KernelReactor) Translate(ctx context.Context, event ReactorOrderPlaced, delivery reactors.Delivery) (ReactorOrderAccepted, error) {
	if event.Number == 99 {
		return ReactorOrderAccepted{}, errors.New("reactor integration deliberate failure")
	}
	select {
	case r.handled <- event.Number:
	case <-ctx.Done():
		return ReactorOrderAccepted{}, ctx.Err()
	}
	return ReactorOrderAccepted{event.Number}, nil
}
func TestKernelReactorOrderingReturnedEventsAndFailedPartition(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ReactorOrderPlaced](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ReactorOrderAccepted](registry); err != nil {
		t.Fatal(err)
	}
	handled := make(chan int, 8)
	if err := chronicle.RegisterReactor[*KernelReactor](registry, func() *KernelReactor { return &KernelReactor{handled} }, reactors.WithID("go-orders")); err != nil {
		t.Fatal(err)
	}
	client := f.client(registry)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appended, err := store.EventLog().AppendMany(f.ctx, "order", []any{ReactorOrderPlaced{0}, ReactorOrderPlaced{1}, ReactorOrderPlaced{2}})
	if err != nil {
		t.Fatal(err)
	}
	if err = appended.Err(); err != nil {
		t.Fatal(err)
	}
	var order []int
	for range 3 {
		select {
		case number := <-handled:
			order = append(order, number)
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
	}
	if !slices.Equal(order, []int{0, 1, 2}) {
		t.Fatal(order)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		history, err := store.EventLog().ReadSource(ctx, "order", eventsequences.SourceFilter{})
		if err != nil {
			t.Fatal(err)
		}
		outputs := 0
		for _, event := range history {
			if event.Context.EventType.ID == "ReactorOrderAccepted" {
				outputs++
			}
		}
		if outputs == 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("returned events not appended: %d: %v", outputs, ctx.Err())
		case <-ticker.C:
		}
	}
	appendSuccessfully(t, f.ctx, store, events.SourceID("failed-order"), ReactorOrderPlaced{99})
	failures := contracts.NewFailedPartitionsClient(f.conn)
	for {
		response, err := failures.GetFailedPartitions(ctx, &contracts.GetFailedPartitionsRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "go-orders"})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, partition := range response.GetItems() {
			if partition.Partition != "failed-order" {
				continue
			}
			for _, attempt := range partition.Attempts {
				for _, message := range attempt.Messages {
					if strings.Contains(message, "reactor integration deliberate failure") {
						found = true
					}
				}
			}
		}
		if found {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("kernel did not record failed partition: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	if err := store.UnregisterReactor(f.ctx, "go-orders"); err != nil {
		t.Fatal(err)
	}
}
