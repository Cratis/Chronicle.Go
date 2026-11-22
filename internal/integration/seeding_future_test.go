//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
)

func TestKernelGlobalSeedsReachFutureNamespaces(t *testing.T) {
	fixture := newKernelFixture(t)
	client := fixture.client(integrationRegistry[CatalogSeeded](t, events.WithID("go-catalog-seeded")))
	initial, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	// Hand-derived C# EventSeeding.Register request. Direct generated RPC: this
	// regression does not depend on Go's builder or registration/replay lifecycle.
	entry := &contracts.SeedingEntry{EventSourceId: "global", EventTypeId: "go-catalog-seeded", Content: `{"Name":"Global"}`, Tags: []string{"reference-data"}}
	request := &contracts.SeedEventsRequest{
		EventStore:          string(fixture.storeName),
		GlobalByEventType:   []*contracts.EventTypeSeedEntries{{EventTypeId: entry.EventTypeId, Entries: []*contracts.SeedingEntry{entry}}},
		GlobalByEventSource: []*contracts.EventSourceSeedEntries{{EventSourceId: entry.EventSourceId, Entries: []*contracts.SeedingEntry{entry}}},
	}
	service := contracts.NewEventSeedingClient(fixture.conn)
	response, err := service.SeedEvents(fixture.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(response); err != nil {
		t.Fatal(err)
	}
	initialPosition := assertFutureSeed(t, fixture, initial)
	tracking, err := service.GetGlobalSeedData(fixture.ctx, &contracts.GetGlobalSeedDataRequest{EventStore: string(fixture.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(tracking); err != nil {
		t.Fatal(err)
	}
	if len(tracking.GetData().GetByEventSource()) != 1 {
		t.Fatalf("global tracking missing: %+v", tracking.GetData())
	}
	if len(tracking.GetData().GetByEventSource()[0].GetEntries()) != 1 {
		t.Fatalf("global tracking missing: %+v", tracking.GetData())
	}

	// Create the namespace through the authoritative RPC after global tracking
	// exists, rather than relying on a client handle to create it incidentally.
	ensureFutureNamespace(t, fixture)
	future, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace("future"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.WaitForRegistration(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	awaitFutureSeed(t, fixture, future)
	futurePosition := assertFutureSeed(t, fixture, future)
	for range 2 {
		ensureFutureNamespace(t, fixture)
		if got := assertFutureSeed(t, fixture, future); got != futurePosition {
			t.Fatalf("repeated ensure changed future seed position: got %d, want %d", got, futurePosition)
		}
	}
	if got := assertFutureSeed(t, fixture, initial); got != initialPosition {
		t.Fatalf("namespace creation changed default seed position: got %d, want %d", got, initialPosition)
	}
}

func ensureFutureNamespace(t *testing.T, fixture *kernelFixture) {
	t.Helper()
	response, err := namespaces.NewNamespacesClient(fixture.conn).EnsureNamespace(fixture.ctx, &namespaces.EnsureNamespaceRequest{EventStore: string(fixture.storeName), Namespace: "future"})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(response); err != nil {
		t.Fatal(err)
	}
}

func awaitFutureSeed(t *testing.T, fixture *kernelFixture, store *chronicle.EventStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(fixture.ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, err := store.EventLog().ReadSource(ctx, "global", eventsequences.SourceFilter{})
		if err != nil {
			t.Fatalf("future namespace seed read: %v", err)
		}
		if len(got) > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("future namespace has no global seed after bounded wait: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertFutureSeed(t *testing.T, fixture *kernelFixture, store *chronicle.EventStore) events.SequenceNumber {
	t.Helper()
	return assertSeedSource(t, fixture, store, "global", []string{"Global"})[0]
}
