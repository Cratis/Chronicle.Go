//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
)

func TestKernelOrderedAtomicBatchAndHistory(t *testing.T) {
	ctx, sequence := batchFixture(t)
	if tail, exists, err := sequence.Tail(ctx, eventsequences.TailFilter{}); err != nil || exists || tail != 0 {
		t.Fatalf("empty tail: %d %v %v", tail, exists, err)
	}
	if has, err := sequence.HasEvents(ctx, "A"); err != nil || has {
		t.Fatalf("empty source: %v %v", has, err)
	}
	id, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	ctx = metadata.WithCorrelation(ctx, id)
	ctx = metadata.WithIdentity(ctx, identities.Identity{Subject: "go-client", OnBehalfOf: &identities.Identity{Subject: "test-user"}})
	occurred := time.Date(2026, 1, 2, 3, 4, 5, 123456700, time.UTC)
	ctx = metadata.WithCausation(ctx, metadata.Causation{Type: "test", Occurred: occurred, Properties: map[string]string{"operation": "batch"}})
	subject := events.Subject("person")
	result, err := sequence.AppendBatch(ctx, []eventsequences.Entry{
		{Source: "A", Event: BatchOpened{"A1"}, Tags: []events.Tag{"entry"}, NamedTags: []events.NamedTag{{Name: "opaque", Value: ""}}, Occurred: &occurred, Subject: &subject, Causation: []metadata.Causation{{Type: "first", Occurred: occurred}}},
		{Source: "B", Event: BatchChanged{"B1"}, Route: eventsequences.Route{SourceType: "Customer", StreamType: "Audit", StreamID: "audit"}},
		{Source: "A", Event: BatchOpened{"A2"}},
	}, eventsequences.WithScopes(sourceScope("A", eventsequences.NoMatchingEvent()), sourceScope("B", eventsequences.NoMatchingEvent())), eventsequences.WithBatchTags("batch"))
	if err != nil || result.Err() != nil || !reflect.DeepEqual(result.Positions, []events.SequenceNumber{0, 1, 2}) || !result.ConcurrencyCheckPerformed || result.CorrelationID != id {
		t.Fatalf("batch: %+v %v", result, err)
	}
	loaded, err := sequence.ReadFrom(ctx, 0, eventsequences.FromFilter{})
	if err != nil || len(loaded) != 3 {
		t.Fatalf("read: %+v %v", loaded, err)
	}
	for i, expected := range []string{"A1", "B1", "A2"} {
		var content BatchOpened
		if err := json.Unmarshal(loaded[i].Content, &content); err != nil {
			t.Fatal(err)
		}
		if content.Value != expected || loaded[i].Context.SequenceNumber != events.SequenceNumber(i) || loaded[i].Context.CorrelationID != id {
			t.Fatal(loaded[i])
		}
	}
	first := loaded[0].Context
	if first.Subject != subject || !first.Occurred.Equal(occurred.Truncate(time.Millisecond)) || len(first.NamedTags) != 1 || first.NamedTags[0].Value != "" || len(first.Causation) != 2 || first.Causation[1].Type != "first" || first.CausedBy.OnBehalfOf.Subject != "test-user" {
		t.Fatal(first)
	}
	if !slices.Contains(first.Tags, "opened") || slices.Contains(first.Tags, "changed") || !slices.Contains(loaded[1].Context.Tags, "changed") {
		t.Fatal("heterogeneous tags were unioned incorrectly")
	}
	if loaded[1].Context.SourceType != "Customer" || loaded[1].Context.StreamID != "audit" || loaded[1].Context.Subject != "B" || len(loaded[1].Context.Causation) != 1 {
		t.Fatal(loaded[1].Context)
	}
	history, err := sequence.ReadHistory(ctx, "A", eventsequences.SourceFilter{SourceType: "Default", StreamType: "All", StreamID: "Default"})
	if err != nil || len(history.Events) != 2 || history.Expectation != eventsequences.Exact(2) || history.Filter.SourceType != nil {
		t.Fatalf("history: %+v %v", history, err)
	}
	advanced, err := sequence.Append(ctx, "A", BatchOpened{"A3"}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil || advanced.Err() != nil {
		t.Fatalf("advance: %+v %v", advanced, err)
	}
	rejected, err := sequence.AppendBatch(ctx, []eventsequences.Entry{{Source: "A", Event: BatchOpened{"stale"}}, {Source: "C", Event: BatchChanged{"must-not-exist"}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: history.Scope()}, sourceScope("C", eventsequences.NoMatchingEvent())))
	var conflict *eventsequences.ConcurrencyError
	if err != nil || rejected.Disposition != eventsequences.Rejected || !errors.As(rejected.Err(), &conflict) || len(conflict.Violations) != 1 || conflict.Violations[0].SourceID != "A" {
		t.Fatalf("rejection: %+v %v", rejected, err)
	}
	if has, err := sequence.HasEvents(ctx, "C"); err != nil || has {
		t.Fatalf("atomicity: C exists=%v err=%v", has, err)
	}
	remaining, err := sequence.ReadSource(ctx, "A", eventsequences.SourceFilter{})
	if err != nil || len(remaining) != 3 {
		t.Fatalf("atomicity: %+v %v", remaining, err)
	}
	// An independent A check must reject a batch whose only event targets B.
	rejected, err = sequence.AppendBatch(ctx, []eventsequences.Entry{{Source: "B", Event: BatchChanged{"independent-conflict"}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: history.Scope()}, sourceScope("B", eventsequences.NoCheck())))
	if err != nil || rejected.Disposition != eventsequences.Rejected {
		t.Fatalf("independent scope: %+v %v", rejected, err)
	}
	routed, err := sequence.ReadSource(ctx, "B", eventsequences.SourceFilter{SourceType: "Customer", StreamType: "Audit", StreamID: "audit", EventTypes: []events.TypeRef{{ID: "batch-changed", Generation: 1}}})
	if err != nil || len(routed) != 1 {
		t.Fatalf("route filter: %+v %v", routed, err)
	}
	from, err := sequence.ReadFrom(ctx, 2, eventsequences.FromFilter{})
	if err != nil || len(from) != 2 || from[0].Context.SequenceNumber != 2 {
		t.Fatalf("inclusive boundary: %+v %v", from, err)
	}
}

func TestKernelSameSourceBatchRoutesAndTagUnion(t *testing.T) {
	for _, route := range []eventsequences.Route{{}, {SourceType: "Customer", StreamType: "Audit", StreamID: "audit"}} {
		for _, named := range []bool{false, true} {
			t.Run(string(route.StreamID)+map[bool]string{true: "-named", false: "-ordinary"}[named], func(t *testing.T) {
				ctx, sequence := batchFixture(t)
				options := []eventsequences.AppendOption{eventsequences.WithRoute(route), eventsequences.WithScope(sourceScope("A", eventsequences.NoMatchingEvent()).Scope)}
				if named {
					options = append(options, eventsequences.WithNamedTags(events.NamedTag{Name: "opaque", Value: ""}))
				}
				result, err := sequence.AppendMany(ctx, "A", []any{BatchOpened{"first"}, BatchChanged{"second"}}, options...)
				if err != nil || result.Err() != nil || len(result.Positions) != 2 {
					t.Fatalf("%+v %v", result, err)
				}
				loaded, err := sequence.ReadSource(ctx, "A", eventsequences.SourceFilter{})
				if err != nil || len(loaded) != 2 {
					t.Fatalf("%+v %v", loaded, err)
				}
				for _, event := range loaded {
					if !slices.Contains(event.Context.Tags, "opened") || !slices.Contains(event.Context.Tags, "changed") {
						t.Fatal(event.Context.Tags)
					}
					if named && (len(event.Context.NamedTags) != 1 || event.Context.NamedTags[0].Value != "") {
						t.Fatal(event.Context.NamedTags)
					}
					if route.StreamID != "" && event.Context.StreamID != route.StreamID {
						t.Fatal(event.Context)
					}
				}
			})
		}
	}
}
