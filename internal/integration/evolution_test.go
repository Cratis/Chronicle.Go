//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
)

type EvolvedCustomerV1 struct {
	Name   string
	Status int32
}
type EvolvedCustomer struct {
	FirstName string
	LastName  string
	Status    int32
	Kind      string
}
type evolutionDelivery struct {
	Source     events.SourceID
	Generation events.Generation
	Name       string
	Status     int32
}
type HistoricalCustomerReactor struct{ deliveries chan evolutionDelivery }

func (r *HistoricalCustomerReactor) Observe(ctx context.Context, e EvolvedCustomerV1, ec events.Context) error {
	select {
	case r.deliveries <- evolutionDelivery{ec.SourceID, ec.EventType.Generation, e.Name, e.Status}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type CurrentCustomerReactor struct{ deliveries chan evolutionDelivery }

func (r *CurrentCustomerReactor) Observe(ctx context.Context, e EvolvedCustomer, ec events.Context) error {
	select {
	case r.deliveries <- evolutionDelivery{ec.SourceID, ec.EventType.Generation, e.FirstName + " " + e.LastName, e.Status}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestKernelHistoricalEvolutionAndSubscribedGenerations(t *testing.T) {
	f := newKernelFixture(t)
	first := f.client(integrationRegistry[EvolvedCustomerV1](t, events.WithID("customer-evolution")), chronicle.WithEventTypeGenerationValidation(true))
	firstStore, err := first.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, f.ctx, firstStore, "old", EvolvedCustomerV1{"Ada Lovelace", 1})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[EvolvedCustomer](registry, events.WithID("customer-evolution"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	old, err := chronicle.RegisterEventGeneration[EvolvedCustomerV1](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = chronicle.RegisterEventMigration(registry, current, old, events.Migration[EvolvedCustomer, EvolvedCustomerV1]{
		MapValues: func(b *events.ValueMapBuilder[EvolvedCustomer, EvolvedCustomerV1]) {
			b.For("Status", "Status", events.ValueMapping{From: int32(1), To: int32(10)})
		},
		Upcast: func(b *events.MigrationBuilder[EvolvedCustomer, EvolvedCustomerV1]) {
			b.Split("FirstName", "Name", " ", 0).Split("LastName", "Name", " ", 1).DefaultValue("Kind", "customer")
		},
		Downcast: func(b *events.MigrationBuilder[EvolvedCustomerV1, EvolvedCustomer]) {
			b.Combine("Name", " ", "FirstName", "LastName")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	migrationClient := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
	store, err := migrationClient.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	catalog, _, err := migrationClient.Catalogs(f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	// Historical backfill is a kernel job, not part of registration readiness.
	// Wait before starting observers: while backfill is pending, C# and Go both
	// deliver the raw fallback when the subscribed generation is unavailable.
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		history, err := store.EventLog().ReadSource(ctx, "old", eventsequences.SourceFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(history) != 1 {
			t.Fatalf("historical count = %d", len(history))
		}
		decoded, err := events.Decode[EvolvedCustomer](catalog, history[0])
		if err != nil {
			t.Fatal(err)
		}
		if decoded.FirstName == "Ada" && decoded.LastName == "Lovelace" && decoded.Kind == "customer" && decoded.Status == 10 {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("historical upcast missing: %+v; context=%+v; generations=%v", decoded, history[0].Context.EventType, history[0].GenerationalContent)
		}
	}
	if err := migrationClient.Close(); err != nil {
		t.Fatal(err)
	}
	historical := make(chan evolutionDelivery, 8)
	upgraded := make(chan evolutionDelivery, 8)
	if err := chronicle.RegisterReactor[*HistoricalCustomerReactor](registry, func() *HistoricalCustomerReactor { return &HistoricalCustomerReactor{historical} }, reactors.WithID("customer-v1")); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactor[*CurrentCustomerReactor](registry, func() *CurrentCustomerReactor { return &CurrentCustomerReactor{upgraded} }, reactors.WithID("customer-v2")); err != nil {
		t.Fatal(err)
	}
	client := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
	store, err = client.EventStore(ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, ctx, store, "new", EvolvedCustomer{"Grace", "Hopper", 10, "customer"})
	history, err := store.EventLog().ReadSource(ctx, "new", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("new count = %d", len(history))
	}
	downcast, err := events.Decode[EvolvedCustomerV1](catalog, history[0])
	if err != nil {
		t.Fatal(err)
	}
	if downcast.Name != "Grace Hopper" || downcast.Status != 1 {
		t.Fatalf("downcast = %+v", downcast)
	}
	for _, expected := range []struct {
		deliveries <-chan evolutionDelivery
		generation events.Generation
		status     int32
	}{{historical, 1, 1}, {upgraded, 2, 10}} {
		seen := map[events.SourceID]bool{}
		for len(seen) < 2 {
			select {
			case delivered := <-expected.deliveries:
				if delivered.Generation != expected.generation || delivered.Status != expected.status {
					t.Fatalf("wrong subscribed payload: %+v", delivered)
				}
				want := "Ada Lovelace"
				if delivered.Source == "new" {
					want = "Grace Hopper"
				}
				if delivered.Name != want {
					t.Fatalf("wrong event content: %+v", delivered)
				}
				seen[delivered.Source] = true
			case <-ctx.Done():
				t.Fatalf("generation %d observer missing events: %v", expected.generation, seen)
			}
		}
	}
	// Historical values remain appendable after the upgrade. Value maps leave
	// unlisted values alone rather than dropping or defaulting them.
	appendSuccessfully(t, f.ctx, store, "historical-after-upgrade", EvolvedCustomerV1{"Katherine Johnson", 777})
	history, err = store.EventLog().ReadSource(ctx, "historical-after-upgrade", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("historical append count = %d", len(history))
	}
	unlisted, err := events.Decode[EvolvedCustomer](catalog, history[0])
	if err != nil || unlisted.FirstName != "Katherine" || unlisted.Status != 777 || unlisted.Kind != "customer" {
		t.Fatalf("unlisted map value changed: %+v %v", unlisted, err)
	}
}
