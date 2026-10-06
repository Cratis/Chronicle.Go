//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
)

func TestKernelGlobalSeedsReachFutureNamespaces(t *testing.T) {
	fixture := newKernelFixture(t)
	client := fixture.client(integrationRegistry[CatalogSeeded](t, events.WithID("go-catalog-seeded")))
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	// Hand-derived C# EventSeeding.Register request. Direct generated RPC: this
	// regression does not depend on Go's builder or registration/replay lifecycle.
	entry := &contracts.SeedingEntry{EventSourceId: "global", EventTypeId: "go-catalog-seeded", Content: `{"Name":"Global"}`}
	service := contracts.NewEventSeedingClient(fixture.conn)
	result, err := service.SeedEvents(fixture.ctx, &contracts.SeedEventsRequest{
		EventStore:          string(fixture.storeName),
		GlobalByEventType:   []*contracts.EventTypeSeedEntries{{EventTypeId: entry.EventTypeId, Entries: []*contracts.SeedingEntry{entry}}},
		GlobalByEventSource: []*contracts.EventSourceSeedEntries{{EventSourceId: entry.EventSourceId, Entries: []*contracts.SeedingEntry{entry}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(result); err != nil {
		t.Fatal(err)
	}
	initial, err := store.EventLog().ReadSource(fixture.ctx, "global", eventsequences.SourceFilter{})
	if err != nil || len(initial) != 1 {
		t.Fatal("existing namespace seed", initial, err)
	}
	stored, err := service.GetGlobalSeedData(fixture.ctx, &contracts.GetGlobalSeedDataRequest{EventStore: string(fixture.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data == nil || len(stored.Data.ByEventSource) != 1 || len(stored.Data.ByEventSource[0].Entries) != 1 {
		t.Fatal("global seed tracking missing", stored)
	}
	future, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace("future"))
	if err != nil {
		t.Fatal(err)
	}
	// NamespaceAdded is handled asynchronously by the kernel NamespacesReactor.
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, err := future.EventLog().ReadSource(fixture.ctx, "global", eventsequences.SourceFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 1 {
			return
		}
		if len(got) != 0 {
			t.Fatalf("future namespace has duplicate seeds: %d", len(got))
		}
		select {
		case <-fixture.ctx.Done():
			t.Fatal(fixture.ctx.Err())
		case <-deadline.C:
			t.Skip("19.32.3 kernel retains global seeds but does not apply them to later namespaces: https://github.com/Cratis/Chronicle/issues/4547")
		case <-ticker.C:
		}
	}
}
