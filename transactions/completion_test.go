// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommitDispositionsAreTerminalAndObservable(t *testing.T) {
	cases := []struct {
		name        string
		response    *sequences.CommandResult_AppendManyResponse
		err         error
		state       transactions.State
		disposition eventsequences.Disposition
	}{
		{name: "committed", response: success(1), state: transactions.Committed, disposition: eventsequences.Committed},
		{name: "constraint and concurrency rejection", response: &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{HasConstraintViolations: true, HasConcurrencyViolations: true, HasErrors: true, Errors: []string{"future-code"}, ConstraintViolations: []*sequences.ConstraintViolation{{ConstraintName: "unique", Details: map[string]string{"key": "original"}}}, ConcurrencyViolations: []*sequences.ConcurrencyViolation{{EventSourceId: "A", ExpectedSequenceNumber: 2, ActualSequenceNumber: 3}}}}, state: transactions.Rejected, disposition: eventsequences.Rejected},
		{name: "errors alone can follow persistence", response: &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{HasErrors: true, Errors: []string{"post-persist-failure"}}}, state: transactions.OutcomeUnknown, disposition: eventsequences.Unknown},
		{name: "lost acknowledgement", err: status.Error(codes.Unavailable, "lost"), state: transactions.OutcomeUnknown, disposition: eventsequences.Unknown},
		{name: "malformed response", response: &sequences.CommandResult_AppendManyResponse{IsAuthorized: true}, state: transactions.OutcomeUnknown, disposition: eventsequences.Unknown},
		{name: "unauthorized envelope", response: &sequences.CommandResult_AppendManyResponse{}, state: transactions.Rejected, disposition: eventsequences.Rejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, sequence, calls := fixture(t, func(context.Context, any) (any, error) { return tc.response, tc.err })
			unit, owner := begin(t, ctx, sequence)
			stage(t, ctx, unit, "A", "first")
			var callbacks []int
			for i := range 2 {
				if err := unit.OnCompleted(func(completed *transactions.UnitOfWork) {
					callbacks = append(callbacks, i)
					if completed != unit || !completed.IsCompleted() || completed.State() != tc.state {
						t.Error("callback before final state")
					}
					if err := completed.Stage(ctx, nil); !errors.Is(err, transactions.ErrCompleted) {
						t.Error(err)
					}
					if err := owner.Rollback(); err != nil {
						t.Error(err)
					} // reentrancy, no lock held
				}); err != nil {
					t.Fatal(err)
				}
			}
			result, err := owner.Commit(ctx)
			if result.Disposition != tc.disposition || unit.State() != tc.state || !unit.IsCompleted() {
				t.Fatalf("%+v %v state=%v", result, err, unit.State())
			}
			var unknown *eventsequences.OutcomeUnknownError
			if tc.state == transactions.OutcomeUnknown && !errors.As(err, &unknown) {
				t.Fatalf("missing ambiguity: %v", err)
			}
			if tc.name == "constraint and concurrency rejection" {
				var constraint *eventsequences.ConstraintError
				var concurrency *eventsequences.ConcurrencyError
				if err != nil || !errors.As(result.Err(), &constraint) || !errors.As(result.Err(), &concurrency) || len(result.Errors) != 1 {
					t.Fatalf("lost diagnostics: %+v %v", result, err)
				}
				if len(unit.GetConstraintViolations()) != 1 || len(unit.GetConcurrencyViolations()) != 1 || len(unit.GetAppendErrors()) != 1 {
					t.Fatal("missing C#-named diagnostics")
				}
				result.ConstraintViolations[0].Details["key"] = "mutated"
				copy, _ := unit.Result()
				if copy.ConstraintViolations[0].Details["key"] != "original" {
					t.Fatal("result alias")
				}
			}
			if tc.state == transactions.Committed {
				result.Positions[0] = 99
				result.Target.EventTypeTails["changed"] = 99
				if last, ok := unit.TryGetLastCommittedEventSequenceNumber(); !ok || last != 0 {
					t.Fatal(last, ok)
				}
			}
			if _, err := owner.Commit(ctx); !errors.Is(err, transactions.ErrCompleted) {
				t.Fatal(err)
			}
			if err := unit.OnCompleted(func(*transactions.UnitOfWork) { callbacks = append(callbacks, 2) }); !errors.Is(err, transactions.ErrCompleted) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(callbacks, []int{1}) || calls.Load() != 1 {
				t.Fatal(callbacks, calls.Load())
			}
		})
	}
}

func TestEmptyCompletionRollbackAndCancellation(t *testing.T) {
	ctx, sequence, calls := fixture(t, func(context.Context, any) (any, error) { t.Error("unexpected RPC"); return success(0), nil })
	unit, owner := begin(t, ctx, sequence)
	if result, err := owner.Commit(ctx); err != nil || result.Err() != nil || !unit.IsSuccess() {
		t.Fatal(result, err)
	}
	if _, ok := unit.TryGetLastCommittedEventSequenceNumber(); ok {
		t.Fatal("empty commit has a position")
	}

	unit, owner = begin(t, ctx, sequence)
	stage(t, ctx, unit, "A", "pending")
	callbackCount := 0
	if err := unit.OnCompleted(func(*transactions.UnitOfWork) { callbackCount++ }); err != nil {
		t.Fatal(err)
	}
	if err := owner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if unit.State() != transactions.RolledBack || len(unit.GetEvents()) != 0 || unit.IsSuccess() || callbackCount != 1 {
		t.Fatal(unit.State(), callbackCount)
	}
	if _, err := owner.Commit(ctx); !errors.Is(err, transactions.ErrCompleted) {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := transactions.Begin(canceled, sequence); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	unit, owner = begin(t, ctx, sequence)
	if err := unit.Stage(canceled, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stage(t, ctx, unit, "A", "pending")
	result, err := owner.Commit(canceled)
	if !errors.Is(err, context.Canceled) || result.Disposition != eventsequences.Rejected || unit.State() != transactions.Rejected {
		t.Fatal(result, err, unit.State())
	}
	if calls.Load() != 0 {
		t.Fatal("pre-dispatch cancellation performed I/O")
	}
	if _, _, err := transactions.Begin(ctx, nil); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	var zero transactions.UnitOfWork
	if err := zero.Stage(ctx, nil); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}

func TestEventlessScopeCommit(t *testing.T) {
	ctx, sequence, calls := fixture(t, func(_ context.Context, input any) (any, error) {
		request := input.(*sequences.AppendManyForEventSourcesRequest)
		if len(request.Events) != 0 || len(request.ConcurrencyScopes) != 1 || !request.ConcurrencyScopes[0].Scope.ExpectsNoMatchingEvent {
			t.Error(request)
		}
		return success(0), nil
	})
	unit, owner := begin(t, ctx, sequence)
	if err := unit.Stage(ctx, nil, eventsequences.LabeledScope{Label: "independent", Scope: eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent()}}); err != nil {
		t.Fatal(err)
	}
	if result, err := owner.Commit(ctx); err != nil || result.Err() != nil || calls.Load() != 1 {
		t.Fatal(result, err)
	}
}

func TestConfirmedUncheckedCommitIsNotRelabeledAsRejection(t *testing.T) {
	ctx, sequence, _ := fixture(t, func(context.Context, any) (any, error) {
		response := success(1)
		response.Response.ConcurrencyCheckPerformed = false
		return response, nil
	})
	unit, owner := begin(t, ctx, sequence)
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: changed{}}}, eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent()}}); err != nil {
		t.Fatal(err)
	}
	result, err := owner.Commit(ctx)
	if !errors.Is(err, chronicle.ErrUnsupported) || result.Disposition != eventsequences.Committed || unit.State() != transactions.Committed || unit.IsSuccess() {
		t.Fatal(result, err, unit.State())
	}
}
