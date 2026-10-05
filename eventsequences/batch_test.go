// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

func TestAppendManyRoutesAndStaticTagUnion(t *testing.T) {
	for _, test := range []struct {
		name, rpc     string
		routed, named bool
	}{
		{"legacy", "AppendMany", false, false}, {"legacy named", "AppendManyWithNamedTags", false, true},
		{"routed", "AppendManyForEventSources", true, false}, {"routed named", "AppendManyForEventSourcesWithNamedTags", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			captured := make(chan any, 1)
			id, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			sequence, calls := sequenceFixture(t, map[string]rpcHandler{test.rpc: func(_ context.Context, request any) (any, error) {
				captured <- request
				return batchSuccess(id, true, 2), nil
			}})
			source := events.SourceID("source")
			occurred := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
			options := []eventsequences.AppendOption{eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent(), Filter: eventsequences.ScopeFilter{SourceID: &source}}), eventsequences.WithTags("dynamic", "shared"), eventsequences.WithSubject("person"), eventsequences.WithOccurred(occurred)}
			if test.routed {
				options = append(options, eventsequences.WithRoute(eventsequences.Route{SourceType: "Customer", StreamType: "Audit", StreamID: "audit"}))
			}
			if test.named {
				options = append(options, eventsequences.WithNamedTags(events.NamedTag{Name: "empty"}, events.NamedTag{Name: "empty"}))
			}
			result, err := sequence.AppendMany(metadata.WithCorrelation(testContext(t), id), source, []any{opened{"A"}, changed{"B"}}, options...)
			if err != nil || result.Err() != nil || !reflect.DeepEqual(result.Positions, []events.SequenceNumber{0, 1}) || result.CorrelationID != id || calls.Load() != 1 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls.Load())
			}
			if result.Target.First == nil || *result.Target.First != 0 || result.Target.EventTypeTails["changed"] != 1 {
				t.Fatal(result.Target)
			}
			checkTags := func(tags []string) {
				t.Helper()
				sorted := slices.Clone(tags)
				slices.Sort(sorted)
				if !reflect.DeepEqual(sorted, []string{"dynamic", "first", "second", "shared"}) {
					t.Fatal(tags)
				}
			}
			checkScope := func(scope *sequences.ConcurrencyScope) {
				t.Helper()
				if !scope.EventSourceId || !scope.ExpectsNoMatchingEvent || scope.SequenceNumber != ^uint64(0) {
					t.Fatal(scope)
				}
			}
			checkEvent := func(content, subject string, date *sequences.SerializableDateTimeOffset) {
				t.Helper()
				if content != `{"value":"A"}` && content != `{"value":"B"}` {
					t.Fatal(content)
				}
				if subject != "person" || date.Value != "2026-01-02T03:04:05.1234567+00:00" {
					t.Fatal(subject, date)
				}
			}
			switch request := (<-captured).(type) {
			case *sequences.AppendManyRequest:
				checkTags(request.Tags)
				checkScope(request.ConcurrencyScope)
				if request.EventStore != "store" || request.Namespace != "tenant" || request.EventSequenceId != "event-log" || request.EventSourceId != "source" {
					t.Fatal(request)
				}
				for _, event := range request.Events {
					checkEvent(event.Content, event.Subject, request.Occurred)
				}
			case *sequences.AppendManyWithNamedTagsRequest:
				checkTags(request.Tags)
				checkScope(request.ConcurrencyScope)
				for _, event := range request.Events {
					checkEvent(event.Content, event.Subject, request.Occurred)
					if len(event.NamedTags) != 1 || event.NamedTags[0].Value != "" {
						t.Fatal(event)
					}
				}
			case *sequences.AppendManyForEventSourcesRequest:
				checkScope(request.ConcurrencyScopes[0].Scope)
				for _, event := range request.Events {
					checkTags(event.Tags)
					checkEvent(event.Content, event.Subject, event.Occurred)
					if event.EventSourceType != "Customer" || event.EventStreamType != "Audit" || event.EventStreamId != "audit" {
						t.Fatal(event)
					}
				}
			case *sequences.AppendManyForEventSourcesWithNamedTagsRequest:
				checkScope(request.ConcurrencyScopes[0].Scope)
				for _, event := range request.Events {
					checkTags(event.Tags)
					checkEvent(event.Content, event.Subject, event.Occurred)
					if event.EventStreamId != "audit" || len(event.NamedTags) != 1 {
						t.Fatal(event)
					}
				}
			default:
				t.Fatalf("unexpected request %T", request)
			}
		})
	}
}

func TestBatchPreservesOrderMetadataAndIndependentScopes(t *testing.T) {
	captured := make(chan *sequences.AppendManyForEventSourcesWithNamedTagsRequest, 1)
	tails := make(chan *sequences.TailSequenceNumberRequest, 3)
	id, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{
		"AppendManyForEventSourcesWithNamedTags": func(_ context.Context, raw any) (any, error) {
			captured <- raw.(*sequences.AppendManyForEventSourcesWithNamedTagsRequest)
			return batchSuccess(id, false, 3), nil
		},
		"TailSequenceNumber": func(_ context.Context, raw any) (any, error) {
			tails <- raw.(*sequences.TailSequenceNumberRequest)
			return tailResponse(7), nil
		},
	})
	ctx := metadata.WithIdentity(metadata.WithCorrelation(testContext(t), id), identities.Identity{Subject: "service", OnBehalfOf: &identities.Identity{Subject: "user"}})
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ctx = metadata.WithCausation(ctx, metadata.Causation{Type: "root", Occurred: when})
	independent := events.SourceID("decision")
	scopes := []eventsequences.LabeledScope{{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}, {Label: "decision", Scope: eventsequences.Scope{Expectation: eventsequences.Exact(3), Filter: eventsequences.ScopeFilter{SourceID: &independent}}}}
	option := eventsequences.WithScopes(scopes...)
	independent = "mutated"
	scopes[1].Scope.Expectation = eventsequences.NoCheck()
	result, err := sequence.AppendBatch(ctx, []eventsequences.Entry{
		{Source: "A", Event: opened{"A1"}, Tags: []events.Tag{"entry"}, NamedTags: []events.NamedTag{{Name: "name", Value: "first"}}, Causation: []metadata.Causation{{Type: "entry", Occurred: when, Properties: map[string]string{"key": "value"}}}},
		{Source: "B", Event: changed{"B1"}, Route: eventsequences.Route{StreamID: "route"}},
		{Source: "A", Event: opened{"A2"}},
	}, option, eventsequences.WithBatchTags("batch"), eventsequences.WithBatchNamedTags(events.NamedTag{Name: "name", Value: "second"}))
	if err != nil || result.Err() != nil {
		t.Fatalf("%+v %v", result, err)
	}
	request := <-captured
	if len(request.Events) != 3 || request.Events[0].Content != `{"value":"A1"}` || request.Events[1].Content != `{"value":"B1"}` || request.Events[2].Content != `{"value":"A2"}` {
		t.Fatal(request)
	}
	first := request.Events[0]
	if !reflect.DeepEqual(first.Tags, []string{"first", "shared", "entry", "batch"}) || len(first.NamedTags) != 2 || len(first.Causation) != 2 || first.Causation[1].Properties["key"] != "value" {
		t.Fatal(first)
	}
	if request.Events[1].Causation != nil || request.Events[2].Subject != "A" || len(request.Causation) != 1 || request.CausedBy.OnBehalfOf.Subject != "user" || wire.Correlation(request.CorrelationId) != id {
		t.Fatal(request)
	}
	if len(request.ConcurrencyScopes) != 3 || request.ConcurrencyScopes[1].EventSourceId != "decision" || request.ConcurrencyScopes[1].Scope.SequenceNumber != 3 || !request.ConcurrencyScopes[1].Scope.EventSourceId {
		t.Fatal(request.ConcurrencyScopes)
	}
	tail := <-tails
	if len(tails) != 0 || tail.EventSourceId != "B" || tail.EventStreamId != "route" || request.ConcurrencyScopes[2].Scope.SequenceNumber != 7 {
		t.Fatal(tail)
	}
	if result.ConcurrencyCheckPerformed {
		t.Fatal("mixed checked/unchecked scopes must preserve false")
	}
}
