//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
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

func TestKernelModelHistoryMixedAllReplayRefusalAndPureAll(t *testing.T) {
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
	if v, err := reader.GetAll(f.ctx, new(events.Count(3))); v.Instances != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("incomplete mixed ALL replay admitted", err)
	}
	if v, err := reader.GetSnapshots(f.ctx, "source"); v != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("incomplete mixed ALL snapshots admitted", err)
	}
	raw, err := contracts.NewReadModelsClient(f.conn).GetAllInstances(f.ctx, &contracts.GetAllInstancesRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ReadModelIdentifier: string(mixed.Identifier()), EventSequenceId: "event-log", EventCount: 3})
	// Chronicle#4562, fixed in 19.32.3: replay no longer filters to the explicit
	// IDs and folds every event handled live. The SDK refusal above stays until
	// it is lifted deliberately.
	if err != nil || raw.ProcessedEventsCount != 3 || len(raw.Instances) != 1 {
		t.Fatal("mixed ALL kernel replay witness changed", err)
	}
	var replayed HistoryMixedAll
	if err := json.Unmarshal([]byte(raw.Instances[0]), &replayed); err != nil || replayed.Name != "third" || replayed.Marker != "A2" {
		t.Fatal("mixed ALL kernel replay folded the wrong state", raw.Instances[0], err)
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
