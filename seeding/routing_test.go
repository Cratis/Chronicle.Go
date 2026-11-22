// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package seeding_test

import (
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/protobuf/proto"
)

func TestStreamRoutingInBothSeedIndexes(t *testing.T) {
	for _, ns := range []metadata.Namespace{"", "default", "red"} {
		t.Run(string(ns), func(t *testing.T) {
			value := &ProductAdded{Name: "same"}
			definition, err := seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
				if ns != "" {
					b = b.ForNamespace(ns)
				}
				seeding.For(b.WithStream("Orders", "one"), "p", value, value)
				b.WithStream("Orders", "two").ForEventSource("p", value, PriceChanged{Price: 12})
				b.ForEventSource("p", value)
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			// Hand-derived, not executed C#: EventSeeding.cs Register at
			// Chronicle 9157c14098688ac0c6946a335bfabf1f44220c7c.
			product := func(stream string) *contracts.SeedingEntry {
				return &contracts.SeedingEntry{EventSourceId: "p", EventTypeId: "product-added", Content: `{"Name":"same","Age":0}`, Tags: []string{"catalog", "static"}, EventStreamType: "Orders", EventStreamId: stream}
			}
			legacy := product("")
			legacy.EventStreamType = ""
			one, repeated, two := product("one"), product("one"), product("two")
			price := &contracts.SeedingEntry{EventSourceId: "p", EventTypeId: "price-changed", Content: `{"Price":12}`, EventStreamType: "Orders", EventStreamId: "two"}
			byType := []*contracts.EventTypeSeedEntries{
				{EventTypeId: "product-added", Entries: []*contracts.SeedingEntry{one, repeated, two, legacy}},
				{EventTypeId: "price-changed", Entries: []*contracts.SeedingEntry{price}},
			}
			bySource := []*contracts.EventSourceSeedEntries{{EventSourceId: "p", Entries: []*contracts.SeedingEntry{one, repeated, two, price, legacy}}}
			want := &contracts.SeedEventsRequest{EventStore: "shop", GlobalByEventType: byType, GlobalByEventSource: bySource}
			if ns != "" {
				want.GlobalByEventType, want.GlobalByEventSource = nil, nil
				want.NamespacedEntries = []*contracts.NamespacedSeedEntries{{Namespace: string(ns), ByEventType: byType, ByEventSource: bySource}}
			}
			got := definition.Contract("shop")
			if !proto.Equal(got, want) {
				t.Fatalf("got %s, want %s", got, want)
			}
			value.Name = "mutated"
			got.Reset()
			if !proto.Equal(definition.Contract("shop"), want) {
				t.Fatal("seed snapshot changed")
			}
		})
	}
}

func TestStreamViewsComposeWithoutMutatingParentsOrSiblings(t *testing.T) {
	definition, err := seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
		routed := b.WithStream("Orders", "one")
		red := b.ForNamespace("red")
		routed.ForNamespace("red").ForEventSource("p", ProductAdded{})
		red.WithStream("Orders", "two").ForEventSource("p", ProductAdded{})
		routed.ForNamespace("blue").ForNamespace("red").WithStream("Other", "three").ForEventSource("p", ProductAdded{})
		red.ForEventSource("p", ProductAdded{})
		routed.ForEventSource("p", ProductAdded{})
		b.ForEventSource("p", ProductAdded{})
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	request := definition.Contract("shop")
	red := request.NamespacedEntries[0]
	if len(request.NamespacedEntries) != 1 || red.Namespace != "red" {
		t.Fatal(request)
	}
	assertSeedRoutes(t, red.ByEventSource[0].Entries, [][2]string{{"Orders", "one"}, {"Orders", "two"}, {"Other", "three"}, {"", ""}})
	assertSeedRoutes(t, request.GlobalByEventSource[0].Entries, [][2]string{{"Orders", "one"}, {"", ""}})
}

func assertSeedRoutes(t *testing.T, entries []*contracts.SeedingEntry, want [][2]string) {
	t.Helper()
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d", len(entries), len(want))
	}
	for i, entry := range entries {
		if entry.EventStreamType != want[i][0] || entry.EventStreamId != want[i][1] {
			t.Fatalf("entry %d: %s, want route %v", i, entry, want[i])
		}
	}
}

func TestStreamDefaultsOnlyEmptyStrings(t *testing.T) {
	cases := []struct {
		typ  events.StreamType
		id   events.StreamID
		want [2]string
	}{
		{"", "", [2]string{"All", "Default"}},
		{"", "two", [2]string{"All", "two"}},
		{"Orders", "", [2]string{"Orders", "Default"}},
		{" ", " ", [2]string{" ", " "}},
		{" Orders ", " Two ", [2]string{" Orders ", " Two "}},
	}
	for _, tc := range cases {
		t.Run(string(tc.typ)+"/"+string(tc.id), func(t *testing.T) {
			definition, err := seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
				b.WithStream(tc.typ, tc.id).ForEventSource("p", ProductAdded{})
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			assertSeedRoutes(t, definition.Contract("shop").GlobalByEventSource[0].Entries, [][2]string{tc.want})
		})
	}
}

func TestStreamViewsRespectInvalidNilAndSealedBuilders(t *testing.T) {
	var nilBuilder *seeding.Builder
	if nilBuilder.WithStream("Orders", "one") != nil {
		t.Fatal("nil builder became usable")
	}
	var zero seeding.Builder
	zero.WithStream("Orders", "one").ForEventSource("p", ProductAdded{})
	var retained *seeding.Builder
	definition, err := seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
		retained = b.WithStream("Orders", "one")
		retained.ForEventSource("p", ProductAdded{})
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	before := definition.Contract("shop")
	retained.WithStream("Other", "two").ForNamespace("red").ForEventSource("p", ProductAdded{})
	if !proto.Equal(before, definition.Contract("shop")) {
		t.Fatal("sealed builder changed snapshot")
	}
	_, err = seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
		b.ForNamespace(" ").WithStream("Orders", "one").ForEventSource("p", ProductAdded{})
		return nil
	}))
	if !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal("routing cleared sticky error", err)
	}
}
