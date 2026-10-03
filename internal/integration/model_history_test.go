//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/google/uuid"
)

type HistoryNameChanged struct {
	Name string `json:"name"`
}
type HistoryModelRemoved struct{}
type HistoryModel struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(HistoryNameChanged)"`
}
type PassiveHistoryModel HistoryModel

func TestKernelModelHistoryProjectionCollectionsAndCorrelationGroups(t *testing.T) {
	f := newKernelFixture(t)
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[HistoryNameChanged](r); err != nil {
		t.Fatal(err)
	}
	removed, err := chronicle.RegisterEvent[HistoryModelRemoved](r)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[HistoryModel](r)
	if err != nil {
		t.Fatal(err)
	}
	passive, err := chronicle.RegisterReadModel[PassiveHistoryModel](r)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.AddProjection(projections.ModelBound(model, projections.RemovedWith(removed))); err != nil {
		t.Fatal(err)
	}
	if err = r.AddProjection(projections.ModelBound(passive, projections.Passive(), projections.RemovedWith(removed))); err != nil {
		t.Fatal(err)
	}
	client := f.client(r)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	empty, err := reader.GetAll(f.ctx, nil)
	if err != nil || empty.Instances == nil || len(empty.Instances) != 0 {
		t.Fatal("materialized absence", err)
	}
	absent, err := reader.GetSnapshots(f.ctx, "absent")
	if err != nil || absent == nil || len(absent) != 0 {
		t.Fatal("snapshot absence", err)
	}
	a, _ := metadata.ParseCorrelationID(uuid.NewString())
	b, _ := metadata.ParseCorrelationID(uuid.NewString())
	firstTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("offset", 2*60*60))
	for i, correlation := range []metadata.CorrelationID{a, b, a} {
		ctx := metadata.WithCorrelation(f.ctx, correlation)
		ctx = metadata.WithCausation(ctx, metadata.Causation{Type: "history-test", Occurred: firstTime, Properties: map[string]string{"case": "A-B-A"}})
		result, err := store.EventLog().Append(ctx, "source", HistoryNameChanged{Name: []string{"first", "second", "third"}[i]}, eventsequences.WithOccurred(firstTime.Add(time.Duration(i)*time.Second)))
		if err != nil || result.Err() != nil {
			t.Fatal("append", err, result.Err())
		}
	}
	awaitHistoryCollection(t, f.ctx, reader, func(v readmodels.Collection[HistoryModel]) bool {
		return len(v.Instances) == 1 && v.Instances[0].Value.Name == "third"
	})
	bounded, err := reader.GetAll(f.ctx, new(events.Count(2)))
	if err != nil || len(bounded.Instances) != 1 || bounded.Instances[0].Value.Name != "second" || bounded.ProcessedEventsCount != 2 {
		t.Fatalf("bounded=%+v err=%v", bounded, err)
	}
	all, err := readmodels.For(store.ReadModels(), passive).GetAll(f.ctx, nil)
	if err != nil || len(all.Instances) != 1 || all.Instances[0].Value.Name != "third" || all.ProcessedEventsCount != 3 {
		t.Fatalf("passive=%+v err=%v", all, err)
	}
	history, err := reader.GetSnapshots(f.ctx, "source")
	if err != nil || len(history) != 2 {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	if history[0].CorrelationID != a || history[1].CorrelationID != b || history[0].Instance.Name != "third" || history[1].Instance.Name != "second" || len(history[0].Events) != 2 || len(history[1].Events) != 1 || !history[0].Occurred.Equal(firstTime) {
		t.Fatalf("global correlation grouping=%+v", history)
	}
	for _, snapshot := range history {
		for _, e := range snapshot.Events {
			if e.Context.Store != f.storeName || e.Context.Namespace != store.Namespace() || e.Context.Sequence != events.EventLog || e.Context.SourceID != "source" || e.Context.CorrelationID != snapshot.CorrelationID || e.ID != "" || e.OriginalContent != nil || e.GenerationalContent != nil || len(e.Context.Causation) == 0 {
				t.Fatalf("contribution=%+v", e)
			}
		}
	}
	other, err := client.EventStore(f.ctx, f.storeName, chronicle.WithNamespace("isolated"))
	if err != nil {
		t.Fatal(err)
	}
	isolated, err := readmodels.For(other.ReadModels(), model).GetAll(f.ctx, new(events.Count(3)))
	if err != nil || len(isolated.Instances) != 0 {
		t.Fatal("namespace leak", err)
	}
	appendSuccessfully(t, f.ctx, store, "source", HistoryModelRemoved{})
	deleted, err := reader.GetAll(f.ctx, new(events.Count(4)))
	if err != nil || len(deleted.Instances) != 0 || deleted.ProcessedEventsCount != 4 {
		t.Fatalf("removed replay=%+v err=%v", deleted, err)
	}
	awaitHistoryCollection(t, f.ctx, reader, func(v readmodels.Collection[HistoryModel]) bool { return len(v.Instances) == 0 })
	raw, err := store.ReadModels().GetSnapshots(f.ctx, model.Identifier(), "source")
	if err != nil || len(raw) != 3 || string(raw[2].Instance) != "{}" {
		t.Fatalf("ambiguous deletion snapshot=%+v err=%v", raw, err)
	}
}

func awaitHistoryCollection[T any](t *testing.T, ctx context.Context, reader *readmodels.Reader[T], ready func(readmodels.Collection[T]) bool) readmodels.Collection[T] {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := reader.GetAll(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if ready(result) {
			return result
		}
		select {
		case <-ctx.Done():
			t.Fatal("collection did not materialize", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestKernelModelHistoryReducerCollectionRoutingAndDeletion(t *testing.T) {
	for _, passive := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "passive"}[passive], func(t *testing.T) {
			f := newKernelFixture(t)
			r, model, _ := reducerIntegrationRegistry(t, passive)
			client := f.client(r)
			store, err := client.EventStore(f.ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			reader := readmodels.For(store.ReadModels(), model)
			appendSuccessfully(t, f.ctx, store, "a", ReducedAmountChanged{-10})
			appendSuccessfully(t, f.ctx, store, "b", ReducedAmountChanged{0})
			appendSuccessfully(t, f.ctx, store, "a", ReducedAmountChanged{8})
			if !passive {
				materialized := awaitHistoryCollection(t, f.ctx, reader, func(c readmodels.Collection[ReducedBalance]) bool {
					for _, v := range c.Instances {
						if v.Value.ID == "a" && v.Value.Amount == -6 {
							return true
						}
					}
					return false
				})
				if materialized.ProcessedEventsCount != 0 {
					t.Fatal("materialized count invented")
				}
			}
			bounded, err := reader.GetAll(f.ctx, new(events.Count(2)))
			if err != nil || len(bounded.Instances) != 2 || bounded.ProcessedEventsCount != 2 || bounded.Instances[0].Value.Amount != -10 || bounded.Instances[1].Value.Amount != 0 {
				t.Fatalf("bounded=%+v err=%v", bounded, err)
			}
			all, err := reader.GetAll(f.ctx, new(events.UnlimitedCount))
			if err != nil || len(all.Instances) != 2 || all.Instances[0].Value.Amount != -6 || all.Instances[0].LastHandled == nil || *all.Instances[0].LastHandled != 2 || all.ProcessedEventsCount != 3 {
				t.Fatalf("all=%+v err=%v", all, err)
			}
			if _, err := reader.GetSnapshots(f.ctx, "a"); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("reducer snapshots not refused", err)
			}
			appendSuccessfully(t, f.ctx, store, "a", ReducedAccountDeleted{})
			all, err = reader.GetAll(f.ctx, new(events.UnlimitedCount))
			if err != nil || len(all.Instances) != 1 || all.Instances[0].Value.ID != "b" || all.ProcessedEventsCount != 4 {
				t.Fatalf("deleted=%+v err=%v", all, err)
			}
		})
	}
}

type HistorySecretChanged struct {
	Name string `json:"name" chronicle:"pii"`
}
type HistorySecret struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"pii;set(HistorySecretChanged)"`
}

type HistoryReducedSecret struct {
	ID   string `json:"id"`
	Name string `json:"name" chronicle:"pii"`
}
type HistoryPassiveSecret HistoryReducedSecret

func TestKernelModelHistoryReleaseOwnershipAndErasure(t *testing.T) {
	f := newKernelFixture(t)
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[HistorySecretChanged](r); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[HistorySecret](r)
	if err != nil {
		t.Fatal(err)
	}
	active, err := chronicle.RegisterReadModel[HistoryReducedSecret](r)
	if err != nil {
		t.Fatal(err)
	}
	passive, err := chronicle.RegisterReadModel[HistoryPassiveSecret](r, readmodels.Passive())
	if err != nil {
		t.Fatal(err)
	}
	if err = chronicle.RegisterReducerHandlers(r, active, "history-secret-active", []reducers.Handler{reducers.On(func(_ context.Context, e HistorySecretChanged, _ *HistoryReducedSecret, ec events.Context) (*HistoryReducedSecret, error) {
		return &HistoryReducedSecret{ID: string(ec.SourceID), Name: e.Name}, nil
	})}); err != nil {
		t.Fatal(err)
	}
	if err = chronicle.RegisterReducerHandlers(r, passive, "history-secret-passive", []reducers.Handler{reducers.On(func(_ context.Context, e HistorySecretChanged, _ *HistoryPassiveSecret, ec events.Context) (*HistoryPassiveSecret, error) {
		return &HistoryPassiveSecret{ID: string(ec.SourceID), Name: e.Name}, nil
	})}); err != nil {
		t.Fatal(err)
	}
	client := f.client(r)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	appendSuccessfully(t, f.ctx, store, "subject", HistorySecretChanged{Name: "private fixture"})
	awaitHistoryCollection(t, f.ctx, reader, func(c readmodels.Collection[HistorySecret]) bool {
		return len(c.Instances) == 1 && c.Instances[0].Value.Name == "private fixture"
	})
	activeReader := readmodels.For(store.ReadModels(), active)
	passiveReader := readmodels.For(store.ReadModels(), passive)
	awaitHistoryCollection(t, f.ctx, activeReader, func(c readmodels.Collection[HistoryReducedSecret]) bool {
		return len(c.Instances) == 1 && c.Instances[0].Value.Name == "private fixture"
	})
	for _, want := range []string{"private fixture", ""} {
		if want == "" {
			if err := store.Compliance().ErasePII(f.ctx, "subject"); err != nil {
				t.Fatal(err)
			}
		}
		for _, count := range []*events.Count{nil, new(events.Count(1)), new(events.UnlimitedCount)} {
			collection, err := reader.GetAll(f.ctx, count)
			if err != nil || len(collection.Instances) != 1 || collection.Instances[0].Value.Name != want {
				t.Fatal("model collection release boundary", err)
			}
			activeCollection, err := activeReader.GetAll(f.ctx, count)
			if err != nil || len(activeCollection.Instances) != 1 || activeCollection.Instances[0].Value.Name != want {
				t.Fatal("active reducer release boundary", err)
			}
			passiveCollection, err := passiveReader.GetAll(f.ctx, count)
			if err != nil || len(passiveCollection.Instances) != 1 || passiveCollection.Instances[0].Value.Name != want {
				t.Fatal("passive reducer release boundary", err)
			}
		}
		snapshots, err := reader.GetSnapshots(f.ctx, "subject")
		if err != nil || len(snapshots) != 1 || snapshots[0].Instance.Name != want || len(snapshots[0].Events) != 1 {
			t.Fatal("snapshot model release boundary", err)
		}
		keyed, err := passiveReader.Get(f.ctx, "subject")
		if err != nil || !keyed.Exists || keyed.Value.Name != want {
			t.Fatal("keyed passive release boundary", err)
		}
		event, err := events.Decode[HistorySecretChanged](store.EventTypes(), snapshots[0].Events[0])
		if err != nil || event.Name != want {
			t.Fatal("snapshot contribution release boundary", err)
		}
	}
}
