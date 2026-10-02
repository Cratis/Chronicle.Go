// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
)

// Commit freezes staging and attempts at most one ordered atomic append. Every
// attempt is terminal, even cancellation before dispatch. Known constraint and
// concurrency rejections are results with nil operation error (use Result.Err).
// Transport loss and errors-only kernel responses are outcome-unknown. No owner
// retry is possible. A repeated call returns the retained result and ErrCompleted;
// a concurrent attempt returns ErrCompleting. Empty work succeeds without an RPC.
func (o *Owner) Commit(ctx context.Context) (eventsequences.BatchResult, error) {
	if o == nil || o.unit == nil {
		return eventsequences.BatchResult{}, faults.ErrInvalidConfiguration
	}
	u := o.unit
	u.mu.Lock()
	if err := u.openError(); err != nil {
		result, prior := cloneResult(u.result), u.err
		u.mu.Unlock()
		return result, errors.Join(err, prior)
	}
	u.state = Completing
	pending, hasWork := u.pending, u.hasWork
	u.mu.Unlock()

	result := eventsequences.BatchResult{Disposition: eventsequences.Committed, CorrelationID: u.correlation}
	err := ctx.Err()
	if err != nil {
		result.Disposition = eventsequences.Rejected
	} else if hasWork {
		result, err = u.sequence.AppendPreparedBatch(ctx, pending)
	}
	if err != nil && ctx.Err() != nil && !errors.Is(err, ctx.Err()) {
		err = errors.Join(err, ctx.Err())
	}
	state := Committed
	var unknown *eventsequences.OutcomeUnknownError
	switch {
	case errors.As(err, &unknown):
		state, result.Disposition = OutcomeUnknown, eventsequences.Unknown
	case len(result.Errors) > 0 && len(result.ConstraintViolations) == 0 && len(result.ConcurrencyViolations) == 0:
		// The kernel also catches exceptions AFTER persistence. Errors alone do
		// not establish rejection, regardless of legacy SDK disposition mapping.
		state, result.Disposition = OutcomeUnknown, eventsequences.Unknown
		if err == nil {
			err = &eventsequences.OutcomeUnknownError{Cause: result.Err()}
		}
	case result.Disposition == eventsequences.Committed:
		// A confirmed commit may still carry an unsupported-check error. Never
		// relabel it as rejected or authorize retry.
	case result.Disposition == eventsequences.Rejected || err != nil:
		state, result.Disposition = Rejected, eventsequences.Rejected
	default:
		state = OutcomeUnknown
	}
	if result.CorrelationID == (metadata.CorrelationID{}) {
		result.CorrelationID = u.correlation
	}
	u.finish(state, result, err)
	return result, err
}

// Rollback discards only open, pending work. It cannot undo a committed or
// outcome-unknown append. After any terminal completion it is a harmless no-op,
// suitable for defer; while Commit is running it returns ErrCompleting.
func (o *Owner) Rollback() error {
	if o == nil || o.unit == nil {
		return faults.ErrInvalidConfiguration
	}
	u := o.unit
	u.mu.Lock()
	if u.state >= Committed {
		u.mu.Unlock()
		return nil
	}
	if err := u.openError(); err != nil {
		u.mu.Unlock()
		return err
	}
	u.state = RolledBack
	u.pending = nil
	callback := u.onCompleted
	u.onCompleted = nil
	u.mu.Unlock()
	if callback != nil {
		callback(u)
	}
	return nil
}

func (u *UnitOfWork) finish(state State, result eventsequences.BatchResult, err error) {
	u.mu.Lock()
	u.state, u.result, u.err = state, cloneResult(result), err
	callback := u.onCompleted
	u.onCompleted = nil
	u.mu.Unlock()
	if callback != nil {
		callback(u)
	}
}

// Result returns an owned snapshot of the commit result and operation error.
// Before completion and after rollback it has Unknown disposition: consult State
// to distinguish those states from an ambiguous commit. Domain rejection details
// are preserved in BatchResult and may be promoted with BatchResult.Err.
func (u *UnitOfWork) Result() (eventsequences.BatchResult, error) {
	if u == nil {
		return eventsequences.BatchResult{}, faults.ErrInvalidConfiguration
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state == Invalid {
		return eventsequences.BatchResult{}, faults.ErrInvalidConfiguration
	}
	return cloneResult(u.result), u.err
}

// IsSuccess reports successful committed completion, not merely IsCompleted.
// A successful empty commit is true; rollback is not a committed completion.
func (u *UnitOfWork) IsSuccess() bool {
	result, err := u.Result()
	return err == nil && result.Disposition == eventsequences.Committed && result.Err() == nil
}

func cloneResult(result eventsequences.BatchResult) eventsequences.BatchResult {
	result.Positions = slices.Clone(result.Positions)
	result.Errors = slices.Clone(result.Errors)
	result.ConcurrencyViolations = slices.Clone(result.ConcurrencyViolations)
	result.ConstraintViolations = slices.Clone(result.ConstraintViolations)
	for i := range result.ConstraintViolations {
		result.ConstraintViolations[i].Details = maps.Clone(result.ConstraintViolations[i].Details)
	}
	result.Target.EventTypeTails = maps.Clone(result.Target.EventTypeTails)
	if result.Target.First != nil {
		first := *result.Target.First
		result.Target.First = &first
	}
	return result
}
