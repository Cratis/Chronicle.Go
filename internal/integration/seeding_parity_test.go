//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"bytes"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/protobuf/proto"
)

type PersonSeeded struct {
	Name string
	Age  int
}

func TestKernelCSharpThenGoSeedsAreIdempotent(t *testing.T) {
	fixture := newKernelFixture(t)
	client := fixture.client(integrationRegistry[PersonSeeded](t, events.WithID("person-seeded")))
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	// Hand-derived C# EventSeeding.Register content, with declaration order and
	// JavaScriptEncoder.Default escaping independently checked against .NET 10.
	entry := &contracts.SeedingEntry{EventSourceId: "ada", EventTypeId: "person-seeded", Content: `{"Name":"Ada\u00E9\u003C\u003E\u0026\u0027\u002B","Age":37}`}
	csharp := &contracts.SeedEventsRequest{
		EventStore:          string(fixture.storeName),
		GlobalByEventType:   []*contracts.EventTypeSeedEntries{{EventTypeId: entry.EventTypeId, Entries: []*contracts.SeedingEntry{entry}}},
		GlobalByEventSource: []*contracts.EventSourceSeedEntries{{EventSourceId: entry.EventSourceId, Entries: []*contracts.SeedingEntry{entry}}},
	}
	event, err := events.Define[PersonSeeded](events.WithID("person-seeded"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	definition, err := seeding.Prepare(catalog, seeding.Func(func(b *seeding.Builder) error {
		seeding.For(b, "ada", PersonSeeded{Name: "Adaé<>&'+", Age: 37})
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	goRequest := definition.Contract(fixture.storeName)
	goContent := goRequest.GlobalByEventSource[0].Entries[0].Content
	if !bytes.Equal([]byte(entry.Content), []byte(goContent)) || !proto.Equal(csharp, goRequest) {
		t.Fatalf("ordinal seed mismatch: C# %s, Go %s", entry.Content, goContent)
	}
	service := contracts.NewEventSeedingClient(fixture.conn)
	for _, request := range []*contracts.SeedEventsRequest{csharp, goRequest} {
		response, err := service.SeedEvents(fixture.ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.CheckEnvelope(response); err != nil {
			t.Fatal(err)
		}
		got, err := store.EventLog().ReadSource(fixture.ctx, "ada", eventsequences.SourceFilter{})
		if err != nil || len(got) != 1 {
			t.Fatalf("C# → Go migration duplicated seed: count %d, error %v", len(got), err)
		}
		decoded, err := events.Decode[PersonSeeded](catalog, got[0])
		if err != nil || decoded != (PersonSeeded{Name: "Adaé<>&'+", Age: 37}) {
			t.Fatalf("seed round trip: %+v, %v", decoded, err)
		}
	}
}
