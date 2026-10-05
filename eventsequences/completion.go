// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"fmt"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/observation"
)

// Completion snapshots this append's exact coordinates and generation. Known
// rejection yields a trivial target; unknown outcome is an error, never success.
func (r AppendOperationResult) Completion() (observation.Completion, error) {
	if r.result.Disposition == Unknown {
		return observation.Completion{}, r.result.Err()
	}
	if r.result.Disposition == Rejected {
		return observation.Completion{}, nil
	}
	if r.result.Position == nil {
		return observation.Completion{}, faults.ErrProtocol
	}
	m := r.operation
	return observation.NewCompletion(m.store, m.namespace, m.sequence, m.appended, []events.SequenceNumber{*r.result.Position})
}

// Completion snapshots the batch's exact per-input generations and positions.
// It uses the greatest actual position per type, not the whole-batch tail.
func (r BatchOperationResult) Completion() (observation.Completion, error) {
	if r.result.Disposition == Unknown {
		return observation.Completion{}, r.result.Err()
	}
	if r.result.Disposition == Rejected {
		return observation.Completion{}, nil
	}
	m := r.operation
	return observation.NewCompletion(m.store, m.namespace, m.sequence, m.appended, r.result.Positions)
}

// WaitForCompletion waits on original single-append coordinates without
// retaining a client in AppendResult. A nil service cannot confirm committed work.
func (r AppendOperationResult) WaitForCompletion(ctx context.Context, observers *observation.Service, timeout time.Duration) (observation.CompletionResult, error) {
	c, err := r.Completion()
	if err != nil {
		return observation.CompletionResult{}, err
	}
	return observers.WaitForCompletion(ctx, c, timeout)
}

// WaitForCompletion waits on original batch coordinates and actual type tails.
func (r BatchOperationResult) WaitForCompletion(ctx context.Context, observers *observation.Service, timeout time.Duration) (observation.CompletionResult, error) {
	c, err := r.Completion()
	if err != nil {
		return observation.CompletionResult{}, err
	}
	return observers.WaitForCompletion(ctx, c, timeout)
}

// Completion snapshots a confirmed append notification, including prepared/UoW
// batches. The caller must synchronize notification capture across concurrent calls.
func (n AppendNotification) Completion() (observation.Completion, error) {
	if n.Result.Disposition == Unknown {
		return observation.Completion{}, n.Result.Err()
	}
	if n.Result.Disposition == Rejected {
		return observation.Completion{}, nil
	}
	refs := make([]events.TypeRef, len(n.Events))
	positions := make([]events.SequenceNumber, len(n.Events))
	for i, e := range n.Events {
		if e.Position == nil {
			return observation.Completion{}, faults.ErrProtocol
		}
		refs[i] = e.EventType
		positions[i] = *e.Position
	}
	return observation.NewCompletion(n.Operation.Store(), n.Operation.Namespace(), n.Operation.Sequence(), refs, positions)
}

// TailForObserver reads the source tail filtered by the observer's exact declared
// event types, like C# GetTailSequenceNumberForObserver. It is NOT the observer's
// handled checkpoint. Pass plan.EventTypes() or Information.EventTypes(); empty
// types fail rather than accidentally querying the unfiltered sequence tail.
func (s *Sequence) TailForObserver(ctx context.Context, types []events.TypeRef) (events.SequenceNumber, bool, error) {
	if len(types) == 0 {
		return 0, false, fmt.Errorf("%w: observer event types required", faults.ErrInvalidConfiguration)
	}
	return s.Tail(ctx, TailFilter{EventTypes: types})
}
