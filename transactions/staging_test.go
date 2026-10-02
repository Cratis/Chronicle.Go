// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

func TestStageSnapshotsEverythingAndPreservesNestedOrder(t *testing.T) {
	var request *sequences.AppendManyForEventSourcesWithNamedTagsRequest
	ctx, sequence, calls := fixture(t, func(_ context.Context, input any) (any, error) {
		request = input.(*sequences.AppendManyForEventSourcesWithNamedTagsRequest)
		return success(3), nil
	})
	actor := identities.Identity{Subject: "actor", OnBehalfOf: &identities.Identity{Subject: "user"}}
	ctx = metadata.WithIdentity(ctx, actor)
	occurred := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ctx = metadata.WithCausation(ctx, metadata.Causation{Type: "outer", Occurred: occurred})
	unit, owner := begin(t, ctx, sequence)
	if unit.CorrelationID() == (metadata.CorrelationID{}) {
		t.Fatal("missing correlation")
	}
	joinedCtx := transactions.WithUnitOfWork(ctx, unit)
	joined, found := transactions.FromContext(joinedCtx)
	if !found || joined != unit {
		t.Fatal("nested participant must join the same unit")
	}
	if _, ok := any(joined).(interface {
		Commit(context.Context) (eventsequences.BatchResult, error)
	}); ok {
		t.Fatal("participant exposes commit")
	}
	if _, ok := any(joined).(interface{ Rollback() error }); ok {
		t.Fatal("participant exposes rollback")
	}

	value := &changed{Value: "A1", Items: []string{"original"}}
	subject := events.Subject("person")
	entries := []eventsequences.Entry{{Source: "A", Event: value, Route: eventsequences.Route{StreamID: "audit"}, Tags: []events.Tag{"entry"}, NamedTags: []events.NamedTag{{Name: "name", Value: "value"}}, Occurred: &occurred, Subject: &subject, Causation: []metadata.Causation{{Type: "entry", Occurred: occurred, Properties: map[string]string{"key": "original"}}}}}
	source := events.SourceID("A")
	filterTypes := []events.TypeRef{{ID: "changed", Generation: 1}}
	check := eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.Exact(7), Filter: eventsequences.ScopeFilter{SourceID: &source, EventTypes: filterTypes}}}
	if err := joined.Stage(joinedCtx, entries, check); err != nil {
		t.Fatal(err)
	}
	value.Value, value.Items[0], subject, source = "mutated", "mutated", "mutated", "mutated"
	occurred = occurred.Add(time.Hour)
	entries[0].Tags[0], entries[0].NamedTags[0].Value = "mutated", "mutated"
	entries[0].Causation[0].Properties["key"] = "mutated"
	filterTypes[0].ID = "mutated"
	actor.OnBehalfOf.Subject = "mutated"
	nestedCtx := metadata.WithCausation(joinedCtx, metadata.Causation{Type: "nested", Occurred: occurred})
	stage(t, nestedCtx, joined, "B", "B1")
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: changed{Value: "A2"}}}); err != nil {
		t.Fatal(err)
	}
	pending := unit.GetEvents()
	pending[0][0] = '!'
	if calls.Load() != 0 {
		t.Fatal("Stage performed I/O")
	}
	// Commit's actor and causation must not replace the staged metadata.
	commitCtx := metadata.WithIdentity(ctx, identities.Identity{Subject: "different"})
	result, err := owner.Commit(commitCtx)
	if err != nil || result.Err() != nil {
		t.Fatalf("%+v %v", result, err)
	}
	if calls.Load() != 1 || !unit.IsSuccess() || !unit.IsCompleted() {
		t.Fatal("completion", unit.State(), calls.Load())
	}
	if request.EventStore != "store" || request.Namespace != "tenant" || request.EventSequenceId != "event-log" || wire.Correlation(request.CorrelationId) != unit.CorrelationID() {
		t.Fatal(request)
	}
	for i, want := range []string{"A1", "B1", "A2"} {
		var got changed
		if err := json.Unmarshal([]byte(request.Events[i].Content), &got); err != nil {
			t.Fatal(err)
		}
		if got.Value != want {
			t.Fatalf("event %d: %+v", i, got)
		}
	}
	first := request.Events[0]
	if first.Subject != "person" || first.EventStreamId != "audit" || first.Occurred.Value != "2026-01-02T03:04:05.0000000+00:00" || first.NamedTags[0].Value != "value" || !reflect.DeepEqual(first.Tags, []string{"static", "entry"}) || first.Causation[1].Properties["key"] != "original" {
		t.Fatal(first)
	}
	if len(request.Events[1].Causation) != 2 || request.Events[1].Causation[1].Type != "nested" || len(request.Events[2].Causation) != 1 || len(request.Causation) != 0 {
		t.Fatal("causation chains", request)
	}
	if request.CausedBy.Subject != "actor" || request.CausedBy.OnBehalfOf.Subject != "user" {
		t.Fatal(request.CausedBy)
	}
	if len(request.ConcurrencyScopes) != 2 || request.ConcurrencyScopes[0].Scope.SequenceNumber != 7 || request.ConcurrencyScopes[0].Scope.EventTypes[0].Id != "changed" {
		t.Fatal(request.ConcurrencyScopes)
	}
	if last, exists := unit.TryGetLastCommittedEventSequenceNumber(); !exists || last != 2 {
		t.Fatal(last, exists)
	}
	if err := joined.Stage(ctx, nil); !errors.Is(err, transactions.ErrCompleted) {
		t.Fatal(err)
	}
	if retained, ok := transactions.FromContext(joinedCtx); !ok || retained != unit {
		t.Fatal("hidden successor")
	}
}

func TestStageIsAtomicAndRejectsMetadataAndScopeChanges(t *testing.T) {
	ctx, sequence, calls := fixture(t, func(_ context.Context, input any) (any, error) {
		request := input.(*sequences.AppendManyForEventSourcesRequest)
		if len(request.Events) != 2 {
			t.Error(request)
		}
		return success(len(request.Events)), nil
	})
	unit, owner := begin(t, ctx, sequence)
	stage(t, ctx, unit, "A", "first")
	badEntries := []eventsequences.Entry{{Source: "B", Event: changed{}}, {Source: "C", Event: struct{}{}}}
	if err := unit.Stage(ctx, badEntries, scope("B")); !errors.Is(err, chronicle.ErrNotRegistered) {
		t.Fatal(err)
	}
	other, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	for _, changedCtx := range []context.Context{metadata.WithCorrelation(ctx, other), metadata.WithIdentity(ctx, identities.Identity{Subject: "other"})} {
		if err := unit.Stage(changedCtx, []eventsequences.Entry{{Source: "B", Event: changed{}}}, scope("B")); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatal(err)
		}
	}
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "B", Event: changed{}}}, eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.Exact(4)}}); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if err := unit.Stage(ctx, nil, scope("B"), scope("B")); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if len(unit.GetEvents()) != 1 || calls.Load() != 0 {
		t.Fatal("failed Stage changed pending work")
	}
	stage(t, ctx, unit, "A", "second") // identical explicit scope joins
	if _, err := owner.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestScopeEventTypesJoinAsSets(t *testing.T) {
	ctx, sequence, _ := fixture(t, func(_ context.Context, input any) (any, error) {
		request := input.(*sequences.AppendManyForEventSourcesRequest)
		if len(request.ConcurrencyScopes) != 1 {
			t.Error(request)
		}
		return success(2), nil
	})
	unit, owner := begin(t, ctx, sequence)
	first := eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent(), Filter: eventsequences.ScopeFilter{EventTypes: []events.TypeRef{{ID: "one", Generation: 1}, {ID: "two", Generation: 1}}}}}
	second := first
	second.Scope.Filter.EventTypes = []events.TypeRef{{ID: "two", Generation: 1}, {ID: "one", Generation: 1}, {ID: "one", Generation: 1}}
	for _, check := range []eventsequences.LabeledScope{first, second} {
		if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: changed{}}}, check); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := owner.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
