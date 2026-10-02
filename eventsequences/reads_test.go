// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

func TestHistoryUsesOnlyLoadedEventsAndNormalizedFilter(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "loaded", true: "empty"}[empty], func(t *testing.T) {
			reads := make(chan *sequences.ForEventSourceIdAndEventTypesRequest, 1)
			appends := make(chan *sequences.AppendManyForEventSourcesRequest, 1)
			sequence, calls := sequenceFixture(t, map[string]rpcHandler{
				"ForEventSourceIdAndEventTypes": func(_ context.Context, raw any) (any, error) {
					reads <- raw.(*sequences.ForEventSourceIdAndEventTypesRequest)
					if empty {
						return readResponse(), nil
					}
					return readResponse(readEvent("source", 9), readEvent("source", 0)), nil
				},
				"AppendManyForEventSources": func(_ context.Context, raw any) (any, error) {
					appends <- raw.(*sequences.AppendManyForEventSourcesRequest)
					return batchSuccess(metadata.CorrelationID{}, true, 1), nil
				},
				"TailSequenceNumber": func(context.Context, any) (any, error) {
					t.Error("history must never substitute a newer tail")
					return tailResponse(100), nil
				},
			})
			filter := eventsequences.SourceFilter{SourceType: "Default", StreamType: "All", StreamID: "Default", EventTypes: []events.TypeRef{{ID: "opened", Generation: 7}, {ID: "opened", Generation: 1}}}
			history, err := sequence.ReadHistory(testContext(t), "source", filter)
			if err != nil {
				t.Fatal(err)
			}
			request := <-reads
			if request.EventTypeIds != "opened" || request.EventSourceType != "" || request.EventStreamType != "" || request.EventStreamId != "" || request.EventStore != "store" || request.Namespace != "tenant" {
				t.Fatal(request)
			}
			filter.EventTypes[0].ID = "mutated"
			if !reflect.DeepEqual(history.Filter.EventTypes, []events.TypeRef{{ID: "opened", Generation: 1}}) || history.Filter.SourceType != nil {
				t.Fatal(history.Filter)
			}
			if empty {
				if history.Expectation != eventsequences.NoMatchingEvent() || len(history.Events) != 0 {
					t.Fatal(history)
				}
			} else if history.Expectation != eventsequences.Exact(9) || len(history.Events) != 2 || history.Events[0].Context.SequenceNumber != 0 {
				t.Fatal(history)
			}
			scope := history.Scope()
			*history.Filter.SourceID = "mutated"
			result, err := sequence.AppendBatch(testContext(t), []eventsequences.Entry{{Source: "source", Event: opened{}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "source", Scope: scope}))
			if err != nil || result.Err() != nil {
				t.Fatalf("%+v %v", result, err)
			}
			appendRequest := <-appends
			actual := appendRequest.ConcurrencyScopes[0].Scope
			if actual.ExpectsNoMatchingEvent != empty || (!empty && actual.SequenceNumber != 9) || !actual.EventSourceId || calls.Load() != 2 {
				t.Fatal(actual, calls.Load())
			}
		})
	}
}

func TestReadPreservesEnvelopeMetadataAndGenerations(t *testing.T) {
	id, err := metadata.ParseCorrelationID("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	event := readEvent("source", 0)
	event.Context.CorrelationId = wire.Guid(id)
	event.Context.CausedBy = &sequences.Identity{Subject: "actor", OnBehalfOf: &sequences.Identity{Subject: "user"}}
	event.Context.Causation = []*sequences.Causation{{Type: "command", Occurred: event.Context.Occurred, Properties: map[string]string{"name": "open"}}}
	event.Context.NamedTags = []*sequences.NamedTag{{Name: "opaque", Value: ""}, {Name: "opaque", Value: "value"}}
	event.Context.Tags = []string{"ordinary"}
	event.Context.Hash = "hash"
	event.Context.ObservationState = sequences.EventObservationState_Replay
	event.Context.EventType.Tombstone = true
	event.Revisions = []*sequences.EventRevision{{Generation: 1, CorrelationId: id.String(), Occurred: event.Context.Occurred, CausedBy: event.Context.CausedBy, Content: `{"value":"revision"}`}}
	event.GenerationalContent = []*sequences.KeyValuePair_Int32_String{{Key: 1, Value: `{"value":"generation"}`}}
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{"FromSequenceNumber": func(_ context.Context, raw any) (any, error) {
		request := raw.(*sequences.FromSequenceNumberRequest)
		if request.FromEventSequenceNumber != 0 || request.EventSourceId != "source" || request.EventTypeIds != "opened" {
			t.Error(request)
		}
		return readResponse(event), nil
	}})
	source := events.SourceID("source")
	loaded, err := sequence.ReadFrom(testContext(t), 0, eventsequences.FromFilter{SourceID: &source, EventTypes: []events.TypeRef{{ID: "opened", Generation: 1}}})
	if err != nil || len(loaded) != 1 {
		t.Fatalf("%+v %v", loaded, err)
	}
	got := loaded[0]
	if got.Context.Store != "store" || got.Context.Namespace != "tenant" || got.Context.Sequence != "event-log" || got.Context.CorrelationID != id || got.Context.Subject != "source" || got.Context.CausedBy.OnBehalfOf.Subject != "user" || !got.Context.Tombstone || got.Context.ObservationState != events.ObservationReplay || got.Context.Hash != "hash" {
		t.Fatal(got.Context)
	}
	if len(got.Context.NamedTags) != 2 || got.Context.Causation[0].Properties["name"] != "open" || string(got.OriginalContent) != `{"value":"original"}` || string(got.Revisions[0].Content) != `{"value":"revision"}` || got.Revisions[0].CorrelationID != id || string(got.GenerationalContent[1]) != `{"value":"generation"}` {
		t.Fatal(got)
	}
	_, offset := got.Context.Occurred.Zone()
	if got.Context.Occurred.Nanosecond() != 123456700 || offset != 2*60*60 {
		t.Fatal(got.Context.Occurred)
	}
	got.Context.Causation[0].Properties["name"] = "changed"
	if event.Context.Causation[0].Properties["name"] != "open" {
		t.Fatal("shared mutable response")
	}
}

func TestTailPresenceNextAndHasEvents(t *testing.T) {
	for _, position := range []uint64{^uint64(0), 0, 9, ^uint64(0) - 1} {
		t.Run(string(rune('a'+position%10)), func(t *testing.T) {
			sequence, _ := sequenceFixture(t, map[string]rpcHandler{
				"TailSequenceNumber": func(context.Context, any) (any, error) { return tailResponse(position), nil },
				"HasEventsForEventSourceId": func(_ context.Context, raw any) (any, error) {
					request := raw.(*sequences.HasEventsForEventSourceIdRequest)
					if request.EventSourceId != "source" || request.Namespace != "tenant" {
						t.Error(request)
					}
					return &sequences.QueryResult_EventSourceEventsResponse{IsAuthorized: true, Data: &sequences.EventSourceEventsResponse{HasEvents: position != ^uint64(0)}}, nil
				},
			})
			tail, exists, err := sequence.Tail(testContext(t), eventsequences.TailFilter{})
			if position == ^uint64(0)-1 {
				if !errors.Is(err, chronicle.ErrProtocol) {
					t.Fatal(err)
				}
				return
			}
			if err != nil || exists != (position != ^uint64(0)) || (exists && uint64(tail) != position) {
				t.Fatalf("%d %v %v", tail, exists, err)
			}
			next, err := sequence.Next(testContext(t))
			if err != nil || (exists && uint64(next) != position+1) || (!exists && next != 0) {
				t.Fatalf("%d %v", next, err)
			}
			has, err := sequence.HasEvents(testContext(t), "source")
			if err != nil || has != exists {
				t.Fatalf("%v %v", has, err)
			}
		})
	}
}
