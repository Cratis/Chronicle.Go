//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type SparseEnumTick struct{ Marker int32 }
type SparseEnumKnown struct {
	Marker int32
	Status *boundaryStatus
}
type SparseEnumUnused struct{ TRUE *boundaryStatus }
type sparseEnumDetail struct{ Value bool }
type sparseEnumView struct {
	ID     string `json:"id"`
	Status *boundaryStatus
	Marker int32
	Detail *sparseEnumDetail
}

// Unsafe boolean/dotted expressions stay in native zero-RPC tests. This single
// namespace witnesses the admitted missing-property behavior, not a server-side
// trial of a compiler refusal or a broader inheritance/cross-join qualification.
func TestKernelEnumSparseEveryMissingThenKnown(t *testing.T) {
	f := newKernelFixture(t)
	f.storeName = chronicle.StoreName("es-" + uuid.NewString()[:8])
	codecs, err := serialization.NewCodecs(serialization.Enum(
		serialization.EnumMember[boundaryStatus]{Name: "Zero", Value: 0},
		serialization.EnumMember[boundaryStatus]{Name: "One", Value: 1},
	))
	if err != nil {
		t.Fatal(err)
	}
	r := chronicle.NewRegistry()
	tick, err := chronicle.RegisterEvent[SparseEnumTick](r, events.WithID("est"))
	if err != nil {
		t.Fatal(err)
	}
	known, err := chronicle.RegisterEvent[SparseEnumKnown](r, events.WithID("esk"), events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[SparseEnumUnused](r, events.WithID("esu"), events.WithCodecs(codecs)); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[sparseEnumView](r, readmodels.WithIdentifier("esv"), readmodels.WithContainerName("esv"), readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("esv", model, projections.NoAutoMap())
	addSparseEnumHandler(b, tick)
	addSparseEnumHandler(b, known)
	projections.Every(b, func(e *projections.EveryBuilder[sparseEnumView]) {
		projections.EveryMap(e, projections.Path[sparseEnumView, *boundaryStatus]("Status"), "Status")
	})
	addBoundaryProjection(t, r, b)
	store, err := f.client(r, chronicle.WithNamingPolicy(serialization.LegacyGoCamelCase)).EventStore(f.ctx, f.storeName, chronicle.WithNamespace("es"))
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	observe := func(marker int32, expected *boundaryStatus) {
		t.Helper()
		state := awaitProjection(t, f.ctx, reader, "subject", func(v sparseEnumView) bool {
			status := v.Status == nil && expected == nil || v.Status != nil && expected != nil && *v.Status == *expected
			return v.ID == "subject" && v.Marker == marker && status && v.Detail != nil && v.Detail.Value
		})
		raw, err := store.ReadModels().Get(f.ctx, "esv", "subject")
		if err != nil || !raw.Exists || !json.Valid(raw.Value) {
			t.Fatalf("raw sparse state: %v", err)
		}
		var typedStatus any
		if state.Value.Status != nil {
			typedStatus = *state.Value.Status
		}
		t.Logf("sparse enum store=%s namespace=es marker=%d typed-status=%v raw=%s", f.storeName, marker, typedStatus, raw.Value)
	}
	appendSuccessfully(t, f.ctx, store, "subject", SparseEnumTick{Marker: 1})
	observe(1, nil)
	one, zero := boundaryStatus(1), boundaryStatus(0)
	appendSuccessfully(t, f.ctx, store, "subject", SparseEnumKnown{Marker: 2, Status: &one})
	observe(2, &one)
	appendSuccessfully(t, f.ctx, store, "subject", SparseEnumKnown{Marker: 3, Status: &zero})
	observe(3, &zero)
}

func addSparseEnumHandler[E any](b *projections.Builder[sparseEnumView], event events.Type[E]) {
	projections.From(b, event, func(f *projections.FromBuilder[sparseEnumView, E]) {
		projections.EventSourceID(f, projections.Path[sparseEnumView, string]("id"))
		projections.Map(f, projections.Path[sparseEnumView, int32]("Marker"), projections.Path[E, int32]("Marker"))
		projections.Value(f, projections.Path[sparseEnumView, bool]("Detail.Value"), true)
	})
}
