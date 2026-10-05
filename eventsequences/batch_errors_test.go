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
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBatchFailuresAndCompleteDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *sequences.CommandResult_AppendManyResponse
		rpcError error
		want     eventsequences.Disposition
		category error
	}{
		{"transport", nil, status.Error(codes.Unavailable, "lost reply"), eventsequences.Unknown, nil},
		{"missing response", &sequences.CommandResult_AppendManyResponse{IsAuthorized: true}, nil, eventsequences.Unknown, chronicle.ErrProtocol},
		{"short positions", batchSuccess(metadata.CorrelationID{}, true, 0), nil, eventsequences.Unknown, chronicle.ErrProtocol},
		{"reserved position", &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: []uint64{^uint64(0)}}}, nil, eventsequences.Unknown, chronicle.ErrProtocol},
		{"contradictory flags", &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, HasErrors: true}}, nil, eventsequences.Unknown, chronicle.ErrProtocol},
		{"authorization", &sequences.CommandResult_AppendManyResponse{}, nil, eventsequences.Rejected, nil},
		{"unchecked protected commit", batchSuccess(metadata.CorrelationID{}, false, 1), nil, eventsequences.Committed, chronicle.ErrUnsupported},
		{"execution", &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, ExceptionMessages: []string{"execution failed"}}, nil, eventsequences.Unknown, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			sequence, calls := sequenceFixture(t, map[string]rpcHandler{"AppendManyForEventSources": func(context.Context, any) (any, error) { return test.response, test.rpcError }})
			result, err := sequence.AppendBatch(testContext(t), []eventsequences.Entry{{Source: "A", Event: opened{}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent()}}))
			if err == nil || result.Disposition != test.want || calls.Load() != 1 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls.Load())
			}
			if test.category != nil && !errors.Is(err, test.category) {
				t.Fatal(err)
			}
			var unknown *eventsequences.OutcomeUnknownError
			if errors.As(err, &unknown) != (test.want == eventsequences.Unknown) {
				t.Fatal(err)
			}
			if test.rpcError != nil && status.Code(err) != codes.Unavailable {
				t.Fatal(err)
			}
		})
	}
	t.Run("all domain diagnostics", func(t *testing.T) {
		sequence, _ := sequenceFixture(t, map[string]rpcHandler{"AppendMany": func(context.Context, any) (any, error) {
			return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{HasConstraintViolations: true, HasConcurrencyViolations: true, HasErrors: true, ConcurrencyCheckPerformed: true,
				ConstraintViolations:  []*sequences.ConstraintViolation{{ConstraintName: "unique", ConstraintType: sequences.ConstraintType_Unique, Message: "taken", Details: map[string]string{"key": "value"}}},
				ConcurrencyViolations: []*sequences.ConcurrencyViolation{{EventSourceId: "scope1", ExpectedSequenceNumber: 1, ActualSequenceNumber: 2}, {EventSourceId: "scope2", ExpectedSequenceNumber: ^uint64(0), ActualSequenceNumber: 3}}, Errors: []string{"FutureCode"}}}, nil
		}})
		result, err := sequence.AppendMany(testContext(t), "A", []any{opened{}}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		var constraints *eventsequences.ConstraintError
		var concurrency *eventsequences.ConcurrencyError
		if err != nil || result.Disposition != eventsequences.Rejected || len(result.Positions) != 0 || !errors.As(result.Err(), &constraints) || !errors.As(result.Err(), &concurrency) || len(concurrency.Violations) != 2 || constraints.Violations[0].Details["key"] != "value" || result.Errors[0] != "FutureCode" {
			t.Fatalf("%+v %v", result, err)
		}
	})
}

func TestInvalidBatchesNeverDispatch(t *testing.T) {
	wrong := events.SourceID("wrong")
	for _, test := range []struct {
		name    string
		entries []eventsequences.Entry
		options []eventsequences.BatchOption
	}{
		{"unknown event", []eventsequences.Entry{{Source: "A", Event: opened{}}, {Source: "B", Event: struct{}{}}}, nil},
		{"blank source", []eventsequences.Entry{{Event: opened{}}}, nil},
		{"blank named tag", []eventsequences.Entry{{Source: "A", Event: opened{}, NamedTags: []events.NamedTag{{Name: " "}}}}, nil},
		{"empty unchecked", nil, nil},
		{"nil option", nil, []eventsequences.BatchOption{nil}},
		{"duplicate label", nil, []eventsequences.BatchOption{eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A"}, eventsequences.LabeledScope{Label: "A"})}},
		{"blank label", nil, []eventsequences.BatchOption{eventsequences.WithScopes(eventsequences.LabeledScope{})}},
		{"mismatched label", nil, []eventsequences.BatchOption{eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.Exact(0), Filter: eventsequences.ScopeFilter{SourceID: &wrong}}})}},
		{"reserved expectation", nil, []eventsequences.BatchOption{eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.Exact(events.Unavailable)}})}},
		{"unchecked narrowed", nil, []eventsequences.BatchOption{eventsequences.WithScopes(eventsequences.LabeledScope{Label: "wrong", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck(), Filter: eventsequences.ScopeFilter{SourceID: &wrong}}})}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sequence, calls := sequenceFixture(t, nil)
			result, err := sequence.AppendBatch(testContext(t), test.entries, test.options...)
			if err == nil || result.Disposition != eventsequences.Unknown || calls.Load() != 0 {
				t.Fatalf("%+v %v calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestBatchCancellationAfterDispatchIsUnknown(t *testing.T) {
	entered := make(chan struct{})
	sequence, calls := sequenceFixture(t, map[string]rpcHandler{"AppendManyForEventSources": func(ctx context.Context, _ any) (any, error) {
		close(entered)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}})
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := sequence.AppendBatch(ctx, []eventsequences.Entry{{Source: "A", Event: opened{}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}))
		done <- err
	}()
	<-entered
	cancel()
	err := <-done
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(err, &unknown) || status.Code(err) != codes.Canceled || calls.Load() != 1 {
		t.Fatalf("%v calls=%d", err, calls.Load())
	}
}

func TestEventlessProtectedBatchAndOlderKernelRefusal(t *testing.T) {
	for _, older := range []bool{false, true} {
		t.Run(map[bool]string{false: "supported", true: "older kernel"}[older], func(t *testing.T) {
			sequence, calls := sequenceFixture(t, map[string]rpcHandler{"AppendManyForEventSources": func(_ context.Context, raw any) (any, error) {
				request := raw.(*sequences.AppendManyForEventSourcesRequest)
				if len(request.Events) != 0 || len(request.ConcurrencyScopes) != 1 || !request.ConcurrencyScopes[0].Scope.ExpectsNoMatchingEvent {
					t.Error(request)
				}
				if older {
					return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, ValidationResults: []*sequences.ValidationResult{{Message: "At least one event is required."}}}, nil
				}
				return batchSuccess(metadata.CorrelationID{}, true, 0), nil
			}})
			result, err := sequence.AppendBatch(testContext(t), nil, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "decision", Scope: eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent()}}))
			if older {
				if !errors.Is(err, chronicle.ErrUnsupported) || result.Disposition != eventsequences.Rejected {
					t.Fatalf("%+v %v", result, err)
				}
			} else if err != nil || result.Err() != nil || result.Target.First != nil {
				t.Fatalf("%+v %v", result, err)
			}
			if calls.Load() != 1 {
				t.Fatal(calls.Load())
			}
		})
	}
}
