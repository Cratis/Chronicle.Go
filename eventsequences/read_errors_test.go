// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMalformedReadsNeverProduceHistory(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*sequences.AppendedEventResponse)
	}{
		{"missing context", func(e *sequences.AppendedEventResponse) { e.Context = nil }},
		{"missing type", func(e *sequences.AppendedEventResponse) { e.Context.EventType = nil }},
		{"reserved position", func(e *sequences.AppendedEventResponse) { e.Context.SequenceNumber = ^uint64(0) }},
		{"invalid JSON", func(e *sequences.AppendedEventResponse) { e.Content = "{" }},
		{"wrong source", func(e *sequences.AppendedEventResponse) { e.Context.EventSourceId = "other" }},
		{"invalid timestamp", func(e *sequences.AppendedEventResponse) { e.Context.Occurred.Value = "invalid" }},
		{"missing timestamp", func(e *sequences.AppendedEventResponse) { e.Context.Occurred = nil }},
		{"invalid tag", func(e *sequences.AppendedEventResponse) { e.Context.NamedTags = []*sequences.NamedTag{{}} }},
		{"duplicate generations", func(e *sequences.AppendedEventResponse) {
			e.GenerationalContent = []*sequences.KeyValuePair_Int32_String{{Key: 1, Value: "{}"}, {Key: 1, Value: "{}"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sequence, _ := sequenceFixture(t, map[string]rpcHandler{"ForEventSourceIdAndEventTypes": func(context.Context, any) (any, error) {
				event := readEvent("source", 0)
				test.mutate(event)
				return readResponse(event), nil
			}})
			history, err := sequence.ReadHistory(testContext(t), "source", eventsequences.SourceFilter{})
			if !errors.Is(err, chronicle.ErrProtocol) || len(history.Events) != 0 {
				t.Fatalf("%+v %v", history, err)
			}
		})
	}
}

func TestReadEnvelopeFailureAndDuplicatePositions(t *testing.T) {
	for _, response := range []*sequences.QueryResult_IEnumerable_AppendedEventResponse{
		{}, {IsAuthorized: true, ExceptionMessages: []string{"failed"}}, readResponse(readEvent("source", 1), readEvent("source", 1)),
	} {
		sequence, _ := sequenceFixture(t, map[string]rpcHandler{"ForEventSourceIdAndEventTypes": func(context.Context, any) (any, error) { return response, nil }})
		loaded, err := sequence.ReadSource(testContext(t), "source", eventsequences.SourceFilter{})
		if err == nil || loaded != nil {
			t.Fatalf("%v %v", loaded, err)
		}
	}
}

func TestReadCancellationAndValidation(t *testing.T) {
	sequence, calls := sequenceFixture(t, nil)
	ctx, cancel := context.WithCancel(testContext(t))
	cancel()
	if _, err := sequence.ReadSource(ctx, "source", eventsequences.SourceFilter{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := sequence.ReadFrom(testContext(t), events.Unavailable, eventsequences.FromFilter{}); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if _, err := sequence.ReadHistory(testContext(t), "", eventsequences.SourceFilter{}); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if _, _, err := sequence.Tail(testContext(t), eventsequences.TailFilter{EventTypes: []events.TypeRef{{ID: "bad,id", Generation: 1}}}); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal(calls.Load())
	}

	entered := make(chan struct{})
	sequence, _ = sequenceFixture(t, map[string]rpcHandler{"ForEventSourceIdAndEventTypes": func(ctx context.Context, _ any) (any, error) {
		close(entered)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}})
	ctx, cancel = context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := sequence.ReadSource(ctx, "source", eventsequences.SourceFilter{}); done <- err }()
	<-entered
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
}

func TestReadInclusiveBoundaryAndMissingTailPayload(t *testing.T) {
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{
		"FromSequenceNumber": func(_ context.Context, raw any) (any, error) {
			request := raw.(*sequences.FromSequenceNumberRequest)
			if request.FromEventSequenceNumber != 7 {
				t.Error(request)
			}
			return readResponse(readEvent("source", 6)), nil
		},
		"TailSequenceNumber": func(context.Context, any) (any, error) {
			return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true}, nil
		},
		"HasEventsForEventSourceId": func(context.Context, any) (any, error) {
			return &sequences.QueryResult_EventSourceEventsResponse{IsAuthorized: true}, nil
		},
	})
	if _, err := sequence.ReadFrom(testContext(t), 7, eventsequences.FromFilter{}); !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatal(err)
	}
	if _, _, err := sequence.Tail(testContext(t), eventsequences.TailFilter{}); !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatal(err)
	}
	if _, err := sequence.HasEvents(testContext(t), "source"); !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatal(err)
	}
}
