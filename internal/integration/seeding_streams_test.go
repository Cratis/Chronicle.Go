//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/protobuf/proto"
)

// This is a hand-derived C# Register request, not an executed .NET fixture.
// Source: EventSeeding.cs at Chronicle 9157c14098688ac0c6946a335bfabf1f44220c7c.
func csharpStreamSeeds(store chronicle.StoreName) *contracts.SeedEventsRequest {
	entry := func(stream string) *contracts.SeedingEntry {
		return &contracts.SeedingEntry{EventSourceId: "item", EventTypeId: "go-catalog-seeded", Content: `{"Name":"same"}`, Tags: []string{"reference-data"}, EventStreamType: "Catalog", EventStreamId: stream}
	}
	one, two, repeated, red := entry("one"), entry("two"), entry("two"), entry("red-only")
	return &contracts.SeedEventsRequest{
		EventStore:          string(store),
		GlobalByEventType:   []*contracts.EventTypeSeedEntries{{EventTypeId: "go-catalog-seeded", Entries: []*contracts.SeedingEntry{one, two, repeated}}},
		GlobalByEventSource: []*contracts.EventSourceSeedEntries{{EventSourceId: "item", Entries: []*contracts.SeedingEntry{one, two, repeated}}},
		NamespacedEntries:   []*contracts.NamespacedSeedEntries{{Namespace: "red", ByEventType: []*contracts.EventTypeSeedEntries{{EventTypeId: "go-catalog-seeded", Entries: []*contracts.SeedingEntry{red}}}, ByEventSource: []*contracts.EventSourceSeedEntries{{EventSourceId: "item", Entries: []*contracts.SeedingEntry{red}}}}},
	}
}

func streamSeedCatalog(t *testing.T) *events.Catalog {
	t.Helper()
	event, err := events.Define[CatalogSeeded](events.WithID("go-catalog-seeded"), events.WithTags("reference-data"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func routedCatalogSeeds(b *seeding.Builder) error {
	seeding.For(b.WithStream("Catalog", "one"), "item", CatalogSeeded{Name: "same"})
	b.WithStream("Catalog", "two").ForEventSource("item", CatalogSeeded{Name: "same"}, CatalogSeeded{Name: "same"})
	seeding.For(b.ForNamespace("red").WithStream("Catalog", "red-only"), "item", CatalogSeeded{Name: "same"})
	return nil
}

func TestKernelStreamSeedsCSharpThenGoRestartAndNamespaceIsolation(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := integrationRegistry[CatalogSeeded](t, events.WithID("go-catalog-seeded"), events.WithTags("reference-data"))
	ensureSeedNamespaces(t, fixture)
	client, store := openStreamSeedStore(t, fixture, registry)
	sendCsharpStreamSeedFixture(t, fixture)
	before := readStreamSeedPositions(t, fixture, client)
	registerStreamSeedBatch(t, fixture, store)
	if after := readStreamSeedPositions(t, fixture, client); !reflect.DeepEqual(before, after) {
		t.Fatal("C# to Go seed registration changed positions", before, after)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	client, store = openStreamSeedStore(t, fixture, registry)
	registerStreamSeedBatch(t, fixture, store)
	if after := readStreamSeedPositions(t, fixture, client); !reflect.DeepEqual(before, after) {
		t.Fatal("restart duplicated or rerouted seed entries", before, after)
	}
}

func openStreamSeedStore(t *testing.T, fixture *kernelFixture, registry *chronicle.Registry) (*chronicle.Client, *chronicle.EventStore) {
	t.Helper()
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	return client, store
}

func registerStreamSeedBatch(t *testing.T, fixture *kernelFixture, store *chronicle.EventStore) {
	t.Helper()
	batch, err := store.PrepareSeeds(seeding.Func(routedCatalogSeeds))
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Register(fixture.ctx); err != nil {
		t.Fatal(err)
	}
}

func sendCsharpStreamSeedFixture(t *testing.T, fixture *kernelFixture) {
	t.Helper()
	// Independently compare both routed indexes before testing runtime identity.
	definition, err := seeding.Prepare(streamSeedCatalog(t), seeding.Func(routedCatalogSeeds))
	if err != nil {
		t.Fatal(err)
	}
	csharp := csharpStreamSeeds(fixture.storeName)
	if !proto.Equal(definition.Contract(fixture.storeName), csharp) {
		t.Fatal("Go request differs from hand-derived C# routing fixture")
	}
	response, err := contracts.NewEventSeedingClient(fixture.conn).SeedEvents(fixture.ctx, csharp)
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(response); err != nil {
		t.Fatal(err)
	}
}

func readStreamSeedPositions(t *testing.T, fixture *kernelFixture, client *chronicle.Client) map[chronicle.Namespace][]events.SequenceNumber {
	t.Helper()
	positions := make(map[chronicle.Namespace][]events.SequenceNumber)
	for _, ns := range []chronicle.Namespace{chronicle.DefaultNamespace, "red", "blue"} {
		scoped, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace(ns))
		if err != nil {
			t.Fatal(err)
		}
		got, err := scoped.EventLog().ReadSource(fixture.ctx, "item", eventsequences.SourceFilter{})
		if err != nil {
			t.Fatal(err)
		}
		want := map[events.StreamID]int{"one": 1, "two": 2}
		if ns == "red" {
			want["red-only"] = 1
		}
		counts := streamSeedCounts(t, scoped, got)
		if !reflect.DeepEqual(counts, want) {
			t.Fatalf("namespace %s: stream counts %v, want %v (no default fallback)", ns, counts, want)
		}
		for _, event := range got {
			positions[ns] = append(positions[ns], event.Context.SequenceNumber)
		}
	}
	return positions
}

func streamSeedCounts(t *testing.T, store *chronicle.EventStore, got []events.Appended) map[events.StreamID]int {
	t.Helper()
	counts := make(map[events.StreamID]int)
	for _, event := range got {
		assertSeedMetadata(t, store, event)
		var value CatalogSeeded
		if err := json.Unmarshal(event.Content, &value); err != nil || value.Name != "same" {
			t.Fatal("routed seed content", value, err)
		}
		if got := [3]string{string(event.Context.SourceID), string(event.Context.SourceType), string(event.Context.StreamType)}; got != [3]string{"item", string(events.DefaultSourceType), "Catalog"} {
			t.Fatal("routed seed metadata", event.Context)
		}
		counts[event.Context.StreamID]++
	}
	return counts
}
