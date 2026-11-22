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
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/reducers"
)

type PinnedCustomerSummary struct {
	ID   string
	Name string
}

type pinnedCustomerDelivery struct {
	Source    events.SourceID
	Name      string
	Delivered events.Generation
	Appended  events.Generation
}

func recordPinnedCustomer(ctx context.Context, target chan<- pinnedCustomerDelivery, name string, ec events.Context) error {
	select {
	case target <- pinnedCustomerDelivery{ec.SourceID, name, ec.EventType.Generation, ec.AppendedGeneration}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type PinnedHistoricalCustomerReactor struct{ deliveries chan<- pinnedCustomerDelivery }

func (r PinnedHistoricalCustomerReactor) Observe(ctx context.Context, e EvolvedCustomerV1, ec events.Context) error {
	return recordPinnedCustomer(ctx, r.deliveries, e.Name, ec)
}

type PinnedCurrentCustomerReactor struct{ deliveries chan<- pinnedCustomerDelivery }

func (r PinnedCurrentCustomerReactor) Observe(ctx context.Context, e EvolvedCustomer, ec events.Context) error {
	return recordPinnedCustomer(ctx, r.deliveries, e.FirstName+" "+e.LastName, ec)
}

func pinnedCustomerRegistry(t *testing.T) *chronicle.Registry {
	t.Helper()
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[EvolvedCustomer](registry, events.WithID("pinned-customer"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := chronicle.RegisterEventGeneration[EvolvedCustomerV1](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterEventMigration(registry, current, previous, events.Migration[EvolvedCustomer, EvolvedCustomerV1]{
		Upcast: func(b *events.MigrationBuilder[EvolvedCustomer, EvolvedCustomerV1]) {
			b.Split("FirstName", "Name", " ", 0).Split("LastName", "Name", " ", 1).DefaultValue("Kind", "customer")
		},
		Downcast: func(b *events.MigrationBuilder[EvolvedCustomerV1, EvolvedCustomer]) {
			b.Combine("Name", " ", "FirstName", "LastName")
		},
	}); err != nil {
		t.Fatal(err)
	}
	return registry
}

func registerPinnedCustomerObservers(t *testing.T, registry *chronicle.Registry, mode events.GenerationDelivery) []<-chan pinnedCustomerDelivery {
	t.Helper()
	historical := make(chan pinnedCustomerDelivery, 16)
	current := make(chan pinnedCustomerDelivery, 16)
	reduced := make(chan pinnedCustomerDelivery, 16)
	if err := chronicle.RegisterReactor[PinnedHistoricalCustomerReactor](registry, func() PinnedHistoricalCustomerReactor { return PinnedHistoricalCustomerReactor{historical} }, reactors.WithID("historical-customer"), reactors.WithGenerationDelivery(mode)); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactor[PinnedCurrentCustomerReactor](registry, func() PinnedCurrentCustomerReactor { return PinnedCurrentCustomerReactor{current} }, reactors.WithID("current-customer"), reactors.WithGenerationDelivery(mode)); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[PinnedCustomerSummary](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReducerHandlers(registry, model, "reduced-customer", []reducers.Handler{reducers.On(func(ctx context.Context, e EvolvedCustomer, _ *PinnedCustomerSummary, ec events.Context) (*PinnedCustomerSummary, error) {
		name := e.FirstName + " " + e.LastName
		if err := recordPinnedCustomer(ctx, reduced, name, ec); err != nil {
			return nil, err
		}
		return &PinnedCustomerSummary{string(ec.SourceID), name}, nil
	})}, reducers.WithGenerationDelivery(mode)); err != nil {
		t.Fatal(err)
	}
	return []<-chan pinnedCustomerDelivery{historical, current, reduced}
}

func assertPinnedCustomerDeliveries(t *testing.T, ctx context.Context, deliveries <-chan pinnedCustomerDelivery, generation events.Generation) {
	t.Helper()
	seen := make(map[events.SourceID]bool)
	for len(seen) < 2 {
		select {
		case delivered := <-deliveries:
			want := pinnedCustomerDelivery{Source: "old", Name: "Ada Lovelace", Delivered: generation, Appended: 1}
			if delivered.Source == "new" {
				want = pinnedCustomerDelivery{Source: "new", Name: "Grace Hopper", Delivered: generation, Appended: 2}
			}
			if delivered != want {
				t.Fatalf("delivered=%+v want=%+v", delivered, want)
			}
			seen[delivered.Source] = true
		case <-ctx.Done():
			t.Fatalf("generation %d missing deliveries: %v", generation, seen)
		}
	}
}

func TestKernelPinnedAndCompatibilityGenerationDelivery(t *testing.T) {
	for _, mode := range []events.GenerationDelivery{events.Compatibility, events.Pinned} {
		t.Run(map[events.GenerationDelivery]string{events.Compatibility: "compatibility", events.Pinned: "pinned"}[mode], func(t *testing.T) {
			f := newKernelFixture(t)
			registry := pinnedCustomerRegistry(t)
			deliveries := registerPinnedCustomerObservers(t, registry, mode)
			client := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
			ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
			defer cancel()
			store, err := client.EventStore(ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			appendSuccessfully(t, ctx, store, "old", EvolvedCustomerV1{"Ada Lovelace", 1})
			appendSuccessfully(t, ctx, store, "new", EvolvedCustomer{"Grace", "Hopper", 1, "customer"})
			for i, channel := range deliveries {
				generation := events.Generation(2)
				if i == 0 {
					generation = 1
				}
				assertPinnedCustomerDeliveries(t, ctx, channel, generation)
			}
		})
	}
}
