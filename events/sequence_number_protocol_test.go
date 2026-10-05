// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc"
)

// sequenceNumberConnection inspects the public RPC requests without network I/O.
// No stream operations are used by these regression tests.
type sequenceNumberConnection struct {
	grpc.ClientConnInterface
	invoke func(string, any, any) error
}

func (c sequenceNumberConnection) Invoke(_ context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	return c.invoke(method, request, response)
}

func TestSequenceNumberHelpersPreserveServerNextContract(t *testing.T) {
	for _, tail := range []events.SequenceNumber{events.Unavailable, events.First, 9, events.BeforeFirst - 2, events.BeforeFirst - 1} {
		t.Run(fmt.Sprint(tail), func(t *testing.T) {
			calls := 0
			connection := sequenceNumberConnection{invoke: func(method string, _, response any) error {
				calls++
				if method != sequences.EventSequences_TailSequenceNumber_FullMethodName {
					t.Fatalf("unexpected RPC %s", method)
				}
				*response.(*sequences.QueryResult_EventSequenceTailResponse) = sequences.QueryResult_EventSequenceTailResponse{
					IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: uint64(tail)},
				}
				return nil
			}}
			catalog, err := events.NewCatalog()
			if err != nil {
				t.Fatal(err)
			}
			sequence, err := eventsequences.New("store", "tenant", events.EventLog, catalog, connection)
			if err != nil {
				t.Fatal(err)
			}
			got, err := sequence.Next(t.Context())
			switch tail {
			case events.Unavailable:
				if err != nil || got != events.First {
					t.Fatalf("empty server Next = %d, %v; want First, nil", got, err)
				}
			case events.BeforeFirst - 1:
				if !errors.Is(err, chronicle.ErrProtocol) {
					t.Fatalf("exhausted server Next error = %v; want ErrProtocol", err)
				}
			default:
				if err != nil || got != tail+1 {
					t.Fatalf("server Next = %d, %v; want %d, nil", got, err, tail+1)
				}
			}
			if calls != 1 {
				t.Fatalf("tail RPC calls = %d; want 1", calls)
			}
		})
	}
}

func TestSequenceNumberSentinelsPreserveConcurrencyProtocol(t *testing.T) {
	definition, err := events.Define[declared](events.WithID("sequence-helper-regression"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	connection := sequenceNumberConnection{invoke: func(method string, request, response any) error {
		calls++
		if method != sequences.EventSequences_Append_FullMethodName {
			t.Fatalf("unexpected RPC %s", method)
		}
		appendRequest := request.(*sequences.AppendRequest)
		scope := appendRequest.ConcurrencyScope
		if scope.SequenceNumber != uint64(events.Unavailable) || !scope.ExpectsNoMatchingEvent {
			t.Fatalf("protected absence = %+v; want Unavailable plus explicit flag", scope)
		}
		*response.(*sequences.CommandResult_AppendResponse) = sequences.CommandResult_AppendResponse{
			IsAuthorized: true, Response: &sequences.AppendResponse{
				IsSuccess: true, SequenceNumber: uint64(events.First), ConcurrencyCheckPerformed: true, CorrelationId: appendRequest.CorrelationId,
			},
		}
		return nil
	}}
	sequence, err := eventsequences.New("store", "tenant", events.EventLog, catalog, connection)
	if err != nil {
		t.Fatal(err)
	}
	for _, sentinel := range []events.SequenceNumber{events.Unavailable, events.Max, events.BeforeFirst} {
		t.Run(fmt.Sprint(sentinel), func(t *testing.T) {
			_, err := sequence.Append(t.Context(), "source", declared{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.Exact(sentinel)}))
			if !errors.Is(err, chronicle.ErrInvalidConfiguration) || calls != 0 {
				t.Fatalf("Exact(%d): err=%v, RPC calls=%d; want local rejection", sentinel, err, calls)
			}
		})
	}
	result, err := sequence.Append(t.Context(), "source", declared{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent()}))
	if err != nil || result.Disposition != eventsequences.Committed || result.Position == nil || *result.Position != events.First || calls != 1 {
		t.Fatalf("protected append = %+v, %v, RPC calls=%d", result, err, calls)
	}
}
