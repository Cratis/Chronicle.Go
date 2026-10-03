// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package seeding_test

import (
	"errors"
	"math"
	"os"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type ProductAdded struct{ Name string }
type PriceChanged struct{ Price float64 }

func catalog(t *testing.T) *events.Catalog {
	t.Helper()
	product, err := events.Define[ProductAdded](events.WithID("product-added"), events.WithTags("catalog", "static"))
	if err != nil {
		t.Fatal(err)
	}
	price, err := events.Define[PriceChanged](events.WithID("price-changed"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := events.NewCatalog(product.Descriptor(), price.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSeedRequestMatchesHandDerivedCSharpGolden(t *testing.T) {
	// Hand-derived from EventSeeding.cs Register at Chronicle 2e31b0d.
	// This is a C#-derived payload fixture, not output captured from executing .NET.
	definition, err := seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
		seeding.For(b, "p1", ProductAdded{Name: "Book"})
		b.ForEventSource("p1", PriceChanged{Price: 12})
		seeding.For(b.ForNamespace("red"), "p2", ProductAdded{Name: "Red"})
		seeding.For(b, "p2", ProductAdded{Name: "Global"})
		b.ForNamespace("default").ForEventSource("p1", PriceChanged{Price: 9})
		b.ForNamespace("red").ForEventSource("p2", PriceChanged{Price: 8})
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/csharp-seed-request.json")
	if err != nil {
		t.Fatal(err)
	}
	want := &contracts.SeedEventsRequest{}
	if err := protojson.Unmarshal(data, want); err != nil {
		t.Fatal(err)
	}
	got := definition.Contract("shop")
	if !proto.Equal(got, want) {
		t.Fatalf("seed request\ngot  %s\nwant %s", got, want)
	}
	got.GlobalByEventType[0].Entries[0].Content = "mutated"
	got.NamespacedEntries[0].ByEventSource[0].Entries[0].Tags[0] = "mutated"
	if !proto.Equal(definition.Contract("shop"), want) {
		t.Fatal("contract leaked mutable data")
	}
}

func TestSeedSnapshotAndRepeatedFacts(t *testing.T) {
	value := &ProductAdded{Name: "original"}
	definition, err := seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
		seeding.For(b, " padded ", value, value)
		value.Name = "mutated"
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	entries := definition.Contract("store").GlobalByEventSource[0].Entries
	if len(entries) != 2 || entries[0].Content != `{"Name":"original"}` || entries[0].EventSourceId != " padded " {
		t.Fatal(entries)
	}
}

func TestSeedPreparationFailsAtomically(t *testing.T) {
	failure := errors.New("callback failure")
	cases := []struct {
		name string
		seed seeding.Func
	}{
		{"unknown", func(b *seeding.Builder) error { b.ForEventSource("p", struct{ Name string }{}); return nil }},
		{"empty unknown generic", func(b *seeding.Builder) error { seeding.For[int](b, "p"); return nil }},
		{"nil event", func(b *seeding.Builder) error { b.ForEventSource("p", (*ProductAdded)(nil)); return nil }},
		{"blank source", func(b *seeding.Builder) error { seeding.For(b, " ", ProductAdded{}); return nil }},
		{"blank namespace", func(b *seeding.Builder) error { b.ForNamespace(" "); return nil }},
		{"serialization", func(b *seeding.Builder) error { seeding.For(b, "p", PriceChanged{math.NaN()}); return nil }},
		{"callback", func(*seeding.Builder) error { return failure }},
		{"panic", func(*seeding.Builder) error { panic("sensitive seed") }},
		{"nil function", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			definition, err := seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
				seeding.For(b, "p", ProductAdded{})
				return nil
			}), tc.seed)
			if err == nil || !definition.IsEmpty() {
				t.Fatal("partial preparation accepted", err)
			}
			if tc.name == "callback" && !errors.Is(err, failure) {
				t.Fatal(err)
			}
		})
	}
	if _, err := seeding.Prepare(nil); err == nil {
		t.Fatal("nil catalog accepted")
	}
	if _, err := seeding.Prepare(catalog(t), nil); err == nil {
		t.Fatal("nil seeder accepted")
	}
}

func TestEmptySeedsDoNotCreateNamespaceGroups(t *testing.T) {
	definition, err := seeding.Prepare(catalog(t), seeding.Func(func(b *seeding.Builder) error {
		seeding.For[ProductAdded](b.ForNamespace("red"), "p")
		return nil
	}))
	if err != nil || !definition.IsEmpty() {
		t.Fatal(definition, err)
	}
}
