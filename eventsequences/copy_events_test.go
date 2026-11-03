// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var copyJobID = uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")

func copyStarted(id uuid.UUID) *sequences.CommandResult_Guid {
	return &sequences.CommandResult_Guid{IsAuthorized: true, Response: wire.Guid(metadata.CorrelationID(id))}
}

func TestCopyRequestsMatchCSharpAndReturnTheJobID(t *testing.T) {
	want := map[int]*sequences.CopyEventsForEventSourceRequest{
		0: {EventStore: "store", Namespace: "tenant", EventSequenceId: "event-log", SourceId: "source", TargetId: "target", EventTypes: []string{"opened", "changed"}},
		1: {EventStore: "store", Namespace: "tenant", EventSequenceId: "event-log", SourceId: "source", TargetId: "target"},
	}
	call := 0
	sequence, calls := sequenceFixture(t, map[string]rpcHandler{"CopyEventsForEventSource": func(_ context.Context, request any) (any, error) {
		if !proto.Equal(request.(proto.Message), want[call]) {
			t.Errorf("request %d\ngot  %v\nwant %v", call, request, want[call])
		}
		call++
		return copyStarted(copyJobID), nil
	}})
	defer sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("copy emitted append notification") })()
	ctx := testContext(t)
	for _, types := range [][]events.TypeID{{"opened", "changed"}, nil} {
		id, err := sequence.Copy(ctx, "source", "target", types...)
		if err != nil || id != copyJobID {
			t.Fatalf("id=%v error=%v", id, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
}

func TestCopyRejectsInvalidInputBeforeDispatch(t *testing.T) {
	sequence, calls := sequenceFixture(t, nil)
	ctx := testContext(t)
	for _, err := range []error{
		copyError(sequence.Copy(ctx, " ", "target")), copyError(sequence.Copy(ctx, "source", "")),
		copyError(sequence.Copy(ctx, "source", "target", "")),
	} {
		var unknown *eventsequences.MutationOutcomeUnknownError
		if !errors.Is(err, chronicle.ErrInvalidConfiguration) || errors.As(err, &unknown) {
			t.Fatal(err)
		}
	}
	if err := copyError(sequence.Copy(ctx, "source", "target", "unknown")); !errors.Is(err, chronicle.ErrNotRegistered) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := copyError(sequence.Copy(canceled, "source", "target")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal(calls.Load())
	}
}

func TestCopyFailuresAreNeverRetriedOrNotified(t *testing.T) {
	for _, kind := range []string{"transport", "canceled", "unauthorized", "exception", "missing job", "empty job"} {
		t.Run(kind, func(t *testing.T) {
			sequence, calls := sequenceFixture(t, map[string]rpcHandler{"CopyEventsForEventSource": func(context.Context, any) (any, error) {
				switch kind {
				case "transport":
					return nil, status.Error(codes.Unavailable, "acknowledgment lost")
				case "canceled":
					return nil, status.Error(codes.Canceled, "after dispatch")
				case "unauthorized":
					return &sequences.CommandResult_Guid{}, nil
				case "exception":
					return &sequences.CommandResult_Guid{IsAuthorized: true, ExceptionMessages: []string{"after effect"}}, nil
				case "missing job":
					return &sequences.CommandResult_Guid{IsAuthorized: true}, nil
				}
				return copyStarted(uuid.Nil), nil
			}})
			defer sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("copy notification") })()
			id, err := sequence.Copy(testContext(t), "source", "target")
			var unknown *eventsequences.MutationOutcomeUnknownError
			switch {
			case kind == "unauthorized":
				// A clean refusal is definitive, like source redaction.
				if err == nil || errors.As(err, &unknown) {
					t.Fatal(err)
				}
			case err == nil || !errors.As(err, &unknown):
				t.Fatalf("outcome must be unknown: %v", err)
			}
			if id != (uuid.UUID{}) || calls.Load() != 1 {
				t.Fatalf("id=%v calls=%d", id, calls.Load())
			}
		})
	}
}

func copyError(_ uuid.UUID, err error) error { return err }
