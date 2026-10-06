//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type HistoryAllA struct {
	Name   string
	Marker string
}
type HistoryAllB struct{ Marker string }
type HistoryMixedAll struct {
	ID     string
	Name   string `chronicle:"set(HistoryAllA)"`
	Marker string `chronicle:"all(from=Marker)"`
}
type HistoryOnlyAll struct {
	ID     string
	Marker string `chronicle:"all(from=Marker)"`
}

func TestKernelModelHistoryMixedAndPureAllReplay(t *testing.T) {
	f := newKernelFixture(t)
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[HistoryAllA](r); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[HistoryAllB](r); err != nil {
		t.Fatal(err)
	}
	mixed, err := chronicle.RegisterReadModel[HistoryMixedAll](r)
	if err != nil {
		t.Fatal(err)
	}
	pure, err := chronicle.RegisterReadModel[HistoryOnlyAll](r)
	if err != nil {
		t.Fatal(err)
	}
	store, err := f.client(r).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), mixed)
	a, _ := metadata.ParseCorrelationID(uuid.NewString())
	b, _ := metadata.ParseCorrelationID(uuid.NewString())
	appendSuccessfully(t, metadata.WithCorrelation(f.ctx, a), store, "source", HistoryAllA{Name: "first", Marker: "A1"})
	awaitHistoryCollection(t, f.ctx, reader, func(c readmodels.Collection[HistoryMixedAll]) bool {
		return len(c.Instances) == 1 && c.Instances[0].Value.Marker == "A1"
	})
	appendSuccessfully(t, metadata.WithCorrelation(f.ctx, b), store, "source", HistoryAllB{Marker: "B"})
	// B matters live even though GetEventTypes' explicit filter will exclude it.
	awaitHistoryCollection(t, f.ctx, reader, func(c readmodels.Collection[HistoryMixedAll]) bool {
		return len(c.Instances) == 1 && c.Instances[0].Value.Marker == "B" && c.Instances[0].Value.Name == "first"
	})
	appendSuccessfully(t, metadata.WithCorrelation(f.ctx, a), store, "source", HistoryAllA{Name: "third", Marker: "A2"})
	awaitHistoryCollection(t, f.ctx, reader, func(c readmodels.Collection[HistoryMixedAll]) bool {
		return len(c.Instances) == 1 && c.Instances[0].Value.Marker == "A2"
	})
	// Chronicle#4562 (fixed in 19.32.1): replay and history no longer filter
	// to the explicit IDs and fold every event the projection handles live.
	// Two events must include B, which only the ALL subscription handles.
	bounded, err := reader.GetAll(f.ctx, new(events.Count(2)))
	if err != nil || len(bounded.Instances) != 1 || bounded.ProcessedEventsCount != 2 || bounded.Instances[0].Value.Name != "first" || bounded.Instances[0].Value.Marker != "B" {
		t.Fatalf("mixed ALL bounded replay: %+v %v", bounded, err)
	}
	complete, err := reader.GetAll(f.ctx, new(events.Count(3)))
	if err != nil || len(complete.Instances) != 1 || complete.ProcessedEventsCount != 3 || complete.Instances[0].Value.Name != "third" || complete.Instances[0].Value.Marker != "A2" {
		t.Fatalf("mixed ALL complete replay: %+v %v", complete, err)
	}
	legacy, err := store.ReadModels().ReplayProjection(f.ctx, mixed.Identifier(), 2)
	var replayed HistoryMixedAll
	if err != nil || len(legacy) != 1 || json.Unmarshal(legacy[0], &replayed) != nil || replayed.Name != "first" || replayed.Marker != "B" {
		t.Fatalf("mixed ALL legacy replay: %s %v", legacy, err)
	}
	// Groups fold cumulatively in returned group order (A then B), as for pure
	// ALL below: the B group keeps A's final Name and applies B's Marker.
	mixedHistory, err := reader.GetSnapshots(f.ctx, "source")
	if err != nil || len(mixedHistory) != 2 || mixedHistory[0].CorrelationID != a || mixedHistory[1].CorrelationID != b || len(mixedHistory[0].Events) != 2 || len(mixedHistory[1].Events) != 1 || mixedHistory[0].Instance.Marker != "A2" || mixedHistory[0].Instance.Name != "third" || mixedHistory[1].Instance.Marker != "B" || mixedHistory[1].Instance.Name != "third" {
		t.Fatalf("mixed ALL A/B/A history: %+v %v", mixedHistory, err)
	}
	all, err := readmodels.For(store.ReadModels(), pure).GetAll(f.ctx, new(events.Count(3)))
	if err != nil || len(all.Instances) != 1 || all.ProcessedEventsCount != 3 || all.Instances[0].Value.Marker != "A2" {
		t.Fatal("empty event-type filter did not read all events", err)
	}
	history, err := readmodels.For(store.ReadModels(), pure).GetSnapshots(f.ctx, "source")
	if err != nil || len(history) != 2 || history[0].CorrelationID != a || history[1].CorrelationID != b || len(history[0].Events) != 2 || len(history[1].Events) != 1 || history[0].Instance.Marker != "A2" || history[1].Instance.Marker != "B" {
		t.Fatal("pure ALL A/B/A history", err)
	}
}
