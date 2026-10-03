// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestHistoryMutationRequestsMatchCSharp(t *testing.T) {
	ctx := metadata.WithIdentity(testContext(t), identities.Identity{Subject: "operator", Name: "Operator", OnBehalfOf: &identities.Identity{Subject: "reviewer"}})
	ctx = metadata.WithCausation(ctx, metadata.Causation{Occurred: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Type: "approved-correction", Properties: map[string]string{"ticket": "case-40"}})
	causedBy := &sequences.Identity{Subject: "operator", Name: "Operator", OnBehalfOf: &sequences.Identity{Subject: "reviewer"}}
	causation := []*sequences.Causation{{Occurred: &sequences.SerializableDateTimeOffset{Value: "2026-01-02T03:04:05.0000000+00:00"}, Type: "approved-correction", Properties: map[string]string{"ticket": "case-40"}}}
	want := map[string]proto.Message{
		"Redact":               &sequences.RedactRequest{EventStore: "store", Namespace: "tenant", EventSequenceId: "event-log", SequenceNumber: 0, Reason: "approved removal", Causation: causation, CausedBy: causedBy},
		"RedactForEventSource": &sequences.RedactForEventSourceRequest{EventStore: "store", Namespace: "tenant", EventSequenceId: "event-log", EventSourceId: "source", Reason: "approved removal", EventTypes: []string{"opened", "changed"}, Causation: causation, CausedBy: causedBy},
		"Revise":               &sequences.ReviseRequest{EventStore: "store", Namespace: "tenant", EventSequenceId: "event-log", SequenceNumber: 3, EventType: &sequences.EventType{Id: "opened", Generation: 1}, Content: `{"value":"replacement"}`, Causation: causation, CausedBy: causedBy},
		"CompleteStream":       &sequences.CompleteStreamRequest{EventStore: "store", Namespace: "tenant", EventSequenceId: "event-log", EventStreamType: "Orders", EventStreamId: "closed"},
	}
	handlers := make(map[string]rpcHandler)
	for method, expected := range want {
		handlers[method] = func(_ context.Context, request any) (any, error) {
			if !proto.Equal(request.(proto.Message), expected) {
				t.Errorf("%s request\ngot  %v\nwant %v", method, request, expected)
			}
			if method == "CompleteStream" {
				return &sequences.CommandResult_CompleteStreamResponse{IsAuthorized: true, Response: &sequences.CompleteStreamResponse{IsSuccess: true, SequenceNumber: 8}}, nil
			}
			return &sequences.CommandResult{IsAuthorized: true}, nil
		}
	}
	sequence, calls := sequenceFixture(t, handlers)
	defer sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("mutation emitted append notification") })()
	if err := sequence.Redact(ctx, 0, "approved removal"); err != nil {
		t.Fatal(err)
	}
	if err := sequence.RedactForEventSource(ctx, "source", "approved removal", "opened", "changed"); err != nil {
		t.Fatal(err)
	}
	if err := sequence.Revise(ctx, 3, opened{"replacement"}); err != nil {
		t.Fatal(err)
	}
	if tail, err := sequence.CompleteStream(ctx, "Orders", "closed"); err != nil || tail != 8 {
		t.Fatalf("tail=%v error=%v", tail, err)
	}
	if calls.Load() != 4 {
		t.Fatal(calls.Load())
	}
}

func TestSourceRedactionWithoutTypesIsExplicitlyUnfiltered(t *testing.T) {
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{"RedactForEventSource": func(_ context.Context, value any) (any, error) {
		r := value.(*sequences.RedactForEventSourceRequest)
		if len(r.EventTypes) != 0 || r.CausedBy.Subject != identities.NotSet().Subject {
			t.Fatal(r)
		}
		return &sequences.CommandResult{IsAuthorized: true}, nil
	}})
	if err := sequence.RedactForEventSource(testContext(t), "source", "all history"); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryMutationsRejectInvalidInputBeforeDispatch(t *testing.T) {
	sequence, calls := sequenceFixture(t, nil)
	ctx := testContext(t)
	for _, err := range []error{
		sequence.Redact(ctx, events.Unavailable, "reason"), sequence.Redact(ctx, 0, " \t"),
		sequence.RedactForEventSource(ctx, " ", "reason"), sequence.RedactForEventSource(ctx, "source", "reason", ""),
		sequence.Revise(ctx, events.Unavailable, opened{}),
	} {
		if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatal(err)
		}
	}
	for _, err := range []error{sequence.Revise(ctx, 0, struct{}{}), sequence.RedactForEventSource(ctx, "source", "reason", "unknown")} {
		if !errors.Is(err, chronicle.ErrNotRegistered) {
			t.Fatal(err)
		}
	}
	if _, err := sequence.CompleteStream(ctx, "", "Default"); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, op := range historyOperations(sequence) {
		if err := op(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal(calls.Load())
	}
}

func TestPointHistoryMutationsRejectReservedPositionsBeforeDispatch(t *testing.T) {
	for name, position := range map[string]events.SequenceNumber{
		"unavailable":  events.Unavailable,
		"max":          events.Unavailable - 1,
		"before first": events.Unavailable - 2,
	} {
		t.Run(name, func(t *testing.T) {
			sequence, calls := sequenceFixture(t, nil)
			ctx := testContext(t)
			for _, err := range []error{sequence.Redact(ctx, position, "reason"), sequence.Revise(ctx, position, opened{})} {
				var unknown *eventsequences.MutationOutcomeUnknownError
				if !errors.Is(err, chronicle.ErrInvalidConfiguration) || errors.As(err, &unknown) {
					t.Fatalf("reserved target error: %v", err)
				}
			}
			if calls.Load() != 0 {
				t.Fatalf("reserved targets dispatched %d RPCs", calls.Load())
			}
		})
	}
}

func TestPointHistoryMutationsAllowActualBoundaryPositions(t *testing.T) {
	for _, position := range []events.SequenceNumber{0, events.Unavailable - 3} {
		sequence, calls := sequenceFixture(t, map[string]rpcHandler{
			"Redact": func(context.Context, any) (any, error) { return &sequences.CommandResult{IsAuthorized: true}, nil },
			"Revise": func(context.Context, any) (any, error) { return &sequences.CommandResult{IsAuthorized: true}, nil },
		})
		ctx := testContext(t)
		if err := sequence.Redact(ctx, position, "reason"); err != nil {
			t.Fatal(err)
		}
		if err := sequence.Revise(ctx, position, opened{}); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatal(calls.Load())
		}
	}
}

func TestStreamCompletionAllowsActualBoundaryTails(t *testing.T) {
	for _, tail := range []events.SequenceNumber{0, events.Unavailable - 3} {
		sequence, calls := sequenceFixture(t, map[string]rpcHandler{"CompleteStream": func(context.Context, any) (any, error) {
			return &sequences.CommandResult_CompleteStreamResponse{IsAuthorized: true, Response: &sequences.CompleteStreamResponse{IsSuccess: true, SequenceNumber: uint64(tail)}}, nil
		}})
		got, err := sequence.CompleteStream(testContext(t), "Orders", "closed")
		if err != nil || got != tail || calls.Load() != 1 {
			t.Fatalf("tail=%d error=%v calls=%d", got, err, calls.Load())
		}
	}
}

func historyOperations(s *eventsequences.Sequence) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"Redact":               func(ctx context.Context) error { return s.Redact(ctx, 0, "reason") },
		"RedactForEventSource": func(ctx context.Context) error { return s.RedactForEventSource(ctx, "source", "reason") },
		"Revise":               func(ctx context.Context) error { return s.Revise(ctx, 0, opened{"new"}) },
		"CompleteStream":       func(ctx context.Context) error { _, err := s.CompleteStream(ctx, "Orders", "closed"); return err },
	}
}

func TestMutationFailuresAreNeverRetriedOrNotified(t *testing.T) {
	for _, method := range []string{"Redact", "RedactForEventSource", "Revise", "CompleteStream"} {
		for _, kind := range []string{"transport", "canceled", "unauthorized", "exception", "malformed"} {
			t.Run(method+"/"+kind, func(t *testing.T) {
				sequence, calls := sequenceFixture(t, map[string]rpcHandler{method: func(context.Context, any) (any, error) {
					if kind == "transport" {
						return nil, status.Error(codes.Unavailable, "acknowledgment lost")
					}
					if kind == "canceled" {
						return nil, status.Error(codes.Canceled, "after dispatch")
					}
					authorized := kind != "unauthorized"
					var exceptions []string
					if kind == "exception" {
						exceptions = []string{"after effect"}
					}
					if method == "CompleteStream" {
						return &sequences.CommandResult_CompleteStreamResponse{IsAuthorized: authorized, ExceptionMessages: exceptions}, nil
					}
					if kind == "malformed" {
						return nil, status.Error(codes.Internal, "bad response")
					}
					return &sequences.CommandResult{IsAuthorized: authorized, ExceptionMessages: exceptions}, nil
				}})
				defer sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("mutation notification") })()
				err := historyOperations(sequence)[method](testContext(t))
				var unknown *eventsequences.MutationOutcomeUnknownError
				if err == nil || errors.As(err, &unknown) != (kind != "unauthorized") || calls.Load() != 1 {
					t.Fatalf("error=%v calls=%d", err, calls.Load())
				}
				if kind == "transport" && status.Code(err) != codes.Unavailable {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestHistoryMutationBeforeDispatchKeepsLocalError(t *testing.T) {
	catalog, err := events.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := eventsequences.New("store", "tenant", "event-log", catalog, &beforeDispatchConnection{cause: faults.ErrClosed})
	if err != nil {
		t.Fatal(err)
	}
	err = sequence.Redact(testContext(t), 0, "reason")
	var unknown *eventsequences.MutationOutcomeUnknownError
	if !errors.Is(err, faults.ErrClosed) || errors.As(err, &unknown) {
		t.Fatal(err)
	}
}

func TestStreamCompletionTypedRefusalsAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *sequences.CompleteStreamResponse
		want     error
		unknown  bool
	}{
		{"already closed", &sequences.CompleteStreamResponse{Error: sequences.CompleteStreamError_AlreadyCompleted}, eventsequences.StreamAlreadyCompleted, false},
		{"default", &sequences.CompleteStreamResponse{Error: sequences.CompleteStreamError_DefaultStreamCannotBeCompleted}, eventsequences.DefaultStreamCannotBeCompleted, false},
		{"empty", &sequences.CompleteStreamResponse{IsSuccess: true, SequenceNumber: uint64(events.Unavailable)}, nil, false},
		{"reserved max tail", &sequences.CompleteStreamResponse{IsSuccess: true, SequenceNumber: uint64(events.Unavailable - 1)}, chronicle.ErrProtocol, true},
		{"reserved before first tail", &sequences.CompleteStreamResponse{IsSuccess: true, SequenceNumber: uint64(events.Unavailable - 2)}, chronicle.ErrProtocol, true},
		{"future code", &sequences.CompleteStreamResponse{Error: 99}, chronicle.ErrProtocol, true},
		{"no disposition", &sequences.CompleteStreamResponse{}, chronicle.ErrProtocol, true},
		{"contradiction", &sequences.CompleteStreamResponse{IsSuccess: true, Error: sequences.CompleteStreamError_AlreadyCompleted}, chronicle.ErrProtocol, true},
		{"absent", nil, chronicle.ErrProtocol, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sequence, calls := sequenceFixture(t, map[string]rpcHandler{"CompleteStream": func(context.Context, any) (any, error) {
				return &sequences.CommandResult_CompleteStreamResponse{IsAuthorized: true, Response: tc.response}, nil
			}})
			tail, err := sequence.CompleteStream(testContext(t), events.AllStreamTypes, events.DefaultStreamID)
			var unknown *eventsequences.MutationOutcomeUnknownError
			if tail != events.Unavailable || !errors.Is(err, tc.want) || errors.As(err, &unknown) != tc.unknown || calls.Load() != 1 {
				t.Fatalf("tail=%d error=%v calls=%d", tail, err, calls.Load())
			}
		})
	}
}
