// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package transactions provides strict, ordered units of work. Participants
// stage events; only a separately retained Owner can commit or roll back.
package transactions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
)

// ErrCompleted means the unit has finished; no successor is created implicitly.
var ErrCompleted = errors.New("chronicle: unit of work is completed")

// ErrCompleting means an owner has already frozen staging for commit.
var ErrCompleting = errors.New("chronicle: unit of work is completing")

// State is a unit of work's lifecycle, independent of an individual RPC.
type State uint8

const (
	// Invalid identifies a zero-value or nil unit, which must not be used.
	Invalid State = iota
	// Open permits staging and owner completion.
	Open
	// Completing freezes staging while the owner attempts its single commit.
	Completing
	// Committed means confirmed successful completion. An empty unit sends no RPC.
	Committed
	// Rejected means the commit is known not to have persisted, including local failure.
	Rejected
	// OutcomeUnknown means persistence could not be determined. Never retry blindly.
	OutcomeUnknown
	// RolledBack means pending work was discarded without dispatch.
	RolledBack
)

// UnitOfWork is a concurrency-safe staging participant, constructed by Begin.
// It has no completion methods and must not be copied. Share this same pointer
// with nested participants. It borrows its sequence and never owns the client.
type UnitOfWork struct {
	mu                sync.Mutex
	sequence          *eventsequences.Sequence
	correlation       metadata.CorrelationID
	origin            eventsequences.Origin
	pending           *eventsequences.PreparedBatch
	hasWork           bool
	state             State
	result            eventsequences.BatchResult
	err               error
	onCompleted       func(*UnitOfWork)
	decisionConflicts map[string][]DecisionConflict
}

// Owner is the capability to complete a shared UnitOfWork exactly once. Keep it
// in the outer execution scope, never in a participant context. Copies refer to
// the same unit and cannot bypass once-only completion. Its zero value is invalid.
type Owner struct{ unit *UnitOfWork }

// Begin binds one sequence (and hence one store and namespace), correlation and
// actor without I/O, assigning a fresh append origin independent of ctx's origin.
// Missing correlation is generated once. It does not retain ctx
// or install ambient state. Use WithUnitOfWork to share the participant explicitly.
// Cancellation of Begin's context later does not cancel an independent Commit ctx.
func Begin(ctx context.Context, sequence *eventsequences.Sequence) (*UnitOfWork, *Owner, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	id := metadata.Correlation(ctx)
	if id == (metadata.CorrelationID{}) {
		var err error
		id, err = metadata.NewCorrelationID()
		if err != nil {
			return nil, nil, err
		}
	}
	pending, err := sequence.PrepareBatch(metadata.WithCorrelation(ctx, id), nil)
	if err != nil {
		return nil, nil, err
	}
	unit := &UnitOfWork{sequence: sequence, correlation: id, origin: eventsequences.NewOrigin(), pending: pending, state: Open}
	return unit, &Owner{unit: unit}, nil
}

// Stage serializes and snapshots an entire enrollment without I/O. Failed
// enrollment changes nothing. Concurrent calls are ordered by successful
// enrollment, with each call's entries contiguous; non-overlapping calls retain
// call order. Inputs must not be mutated until Stage returns. The actor must
// match Begin; absent correlation inherits Begin's, a different one fails.
// Causation is captured per entry from this ctx plus Entry.Causation. Resolve
// scopes are resolved at Commit; use ReadHistory's Scope for loaded-state checks.
func (u *UnitOfWork) Stage(ctx context.Context, entries []eventsequences.Entry, scopes ...eventsequences.LabeledScope) error {
	if u == nil {
		return faults.ErrInvalidConfiguration
	}
	u.mu.Lock()
	err := u.openError()
	u.mu.Unlock()
	if err != nil {
		return err
	}
	if id := metadata.Correlation(ctx); id != (metadata.CorrelationID{}) && id != u.correlation {
		return fmt.Errorf("%w: unit of work correlation must remain fixed", faults.ErrInvalidConfiguration)
	}
	// Serialization may call user methods. Never invoke them under the state lock.
	batch, err := u.sequence.PrepareBatch(metadata.WithCorrelation(ctx, u.correlation), entries, eventsequences.WithScopes(scopes...))
	if err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err = u.openError(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	joined, err := u.pending.Merge(batch)
	if err != nil {
		return err
	}
	u.pending = joined
	u.hasWork = u.hasWork || len(entries) != 0 || len(scopes) != 0
	return nil
}

func (u *UnitOfWork) openError() error {
	switch u.state {
	case Open:
		return nil
	case Invalid:
		return faults.ErrInvalidConfiguration
	case Completing:
		return ErrCompleting
	default:
		return ErrCompleted
	}
}

// State returns a synchronized lifecycle snapshot; nil and zero units are Invalid.
func (u *UnitOfWork) State() State {
	if u == nil {
		return Invalid
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.state
}

// IsCompleted reports terminal completion, including rejection, ambiguity and rollback.
// It never implies that events were persisted.
func (u *UnitOfWork) IsCompleted() bool { return u.State() >= Committed }

// CorrelationID returns the fixed correlation; nil and zero units return zero.
func (u *UnitOfWork) CorrelationID() metadata.CorrelationID {
	if u == nil {
		return metadata.CorrelationID{}
	}
	return u.correlation
}

// Origin returns the unit's immutable append attribution identity, including
// after completion. Nil and zero units return zero. It is independent of the
// correlation and any origins installed in Begin, Stage or Commit contexts.
func (u *UnitOfWork) Origin() eventsequences.Origin {
	if u == nil {
		return eventsequences.Origin{}
	}
	return u.origin
}

// GetEvents returns defensive JSON snapshots in global staging order, including
// after a commit attempt. Rollback clears them. Values are not persisted events.
func (u *UnitOfWork) GetEvents() []json.RawMessage {
	if u == nil {
		return nil
	}
	u.mu.Lock()
	pending := u.pending
	u.mu.Unlock()
	return pending.GetEvents()
}

// OnCompleted sets the synchronous completion callback, replacing any prior
// callback like C# IUnitOfWork.OnCompleted. It runs outside locks once the final
// result is visible, including failed commits and rollback. Register before
// completion; late registration returns ErrCompleted. The callback must return
// promptly, and its panic propagates to the completing caller. Nil is invalid.
func (u *UnitOfWork) OnCompleted(callback func(*UnitOfWork)) error {
	if u == nil || callback == nil {
		return faults.ErrInvalidConfiguration
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state == Invalid {
		return faults.ErrInvalidConfiguration
	}
	if u.state >= Committed {
		return ErrCompleted
	}
	u.onCompleted = callback
	return nil
}

// TryGetLastCommittedEventSequenceNumber returns the last confirmed position.
// Position zero is valid; empty, rejected and unknown commits return false.
func (u *UnitOfWork) TryGetLastCommittedEventSequenceNumber() (events.SequenceNumber, bool) {
	result, _ := u.Result()
	if result.Disposition != eventsequences.Committed || len(result.Positions) == 0 {
		return 0, false
	}
	return result.Positions[len(result.Positions)-1], true
}
