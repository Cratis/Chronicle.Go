// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"fmt"
	"maps"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/observation"
)

// BatchResult owns an atomic append's disposition and complete diagnostics.
// Known rejection has no positions; transport failure returns Unknown, not Rejected.
type BatchResult struct {
	// Disposition reports confirmed commitment, rejection or unknown outcome.
	Disposition Disposition
	// Positions corresponds one-to-one with input entries, in original order.
	Positions []events.SequenceNumber
	// CorrelationID is the kernel's correlation for this batch.
	CorrelationID metadata.CorrelationID
	// ConstraintViolations contains every rejected constraint and its details.
	ConstraintViolations []constraints.Violation
	// ConcurrencyViolations identifies rejected labels in SourceID, with their tails.
	ConcurrencyViolations []ConcurrencyViolation
	// Errors preserves every append error code, including future codes.
	Errors []AppendError
	// ConcurrencyCheckPerformed means EVERY supplied scope was checked. False is
	// expected when any scope is NoCheck or Resolve finds no matching history.
	ConcurrencyCheckPerformed bool
	// Target identifies committed events for observer completion; empty for eventless checks.
	Target observation.CompletionTarget
}

// Err promotes known rejection to the same inspectable errors as AppendResult.Err.
// It returns nil only for a committed result without diagnostics.
func (r BatchResult) Err() error {
	return (AppendResult{Disposition: r.Disposition, ConstraintViolations: r.ConstraintViolations, ConcurrencyViolations: r.ConcurrencyViolations, Errors: r.Errors}).Err()
}

func (s *Sequence) batchResult(response *sequences.AppendManyResponse, refs []events.TypeRef) (BatchResult, error) {
	if response == nil {
		return BatchResult{}, faults.ErrProtocol
	}
	constraintsPresent, concurrencyPresent, errorsPresent := len(response.ConstraintViolations) > 0, len(response.ConcurrencyViolations) > 0, len(response.Errors) > 0
	if response.HasConstraintViolations != constraintsPresent || response.HasConcurrencyViolations != concurrencyPresent || response.HasErrors != errorsPresent || response.IsSuccess == (constraintsPresent || concurrencyPresent || errorsPresent) {
		return BatchResult{}, fmt.Errorf("%w: inconsistent batch result flags", faults.ErrProtocol)
	}
	result := BatchResult{Disposition: Rejected, CorrelationID: wire.Correlation(response.CorrelationId), ConcurrencyCheckPerformed: response.ConcurrencyCheckPerformed}
	for _, violation := range response.ConstraintViolations {
		if violation == nil {
			return BatchResult{}, faults.ErrProtocol
		}
		result.ConstraintViolations = append(result.ConstraintViolations, constraints.Violation{EventTypeID: events.TypeID(violation.EventTypeId), SequenceNumber: events.SequenceNumber(violation.SequenceNumber), Type: constraints.Type(violation.ConstraintType), ConstraintName: violation.ConstraintName, Message: violation.Message, Details: maps.Clone(violation.Details)})
	}
	for _, violation := range response.ConcurrencyViolations {
		if violation == nil {
			return BatchResult{}, faults.ErrProtocol
		}
		result.ConcurrencyViolations = append(result.ConcurrencyViolations, ConcurrencyViolation{SourceID: events.SourceID(violation.EventSourceId), Expected: events.SequenceNumber(violation.ExpectedSequenceNumber), Actual: events.SequenceNumber(violation.ActualSequenceNumber)})
	}
	for _, err := range response.Errors {
		result.Errors = append(result.Errors, AppendError(err))
	}
	if !response.IsSuccess {
		if len(response.SequenceNumbers) != 0 {
			return BatchResult{}, fmt.Errorf("%w: rejected atomic batch returned positions", faults.ErrProtocol)
		}
		return result, nil
	}
	if len(response.SequenceNumbers) != len(refs) {
		return BatchResult{}, fmt.Errorf("%w: batch position count does not match input", faults.ErrProtocol)
	}
	result.Disposition = Committed
	for i, number := range response.SequenceNumbers {
		position := events.SequenceNumber(number)
		if position >= events.Unavailable-2 || (i > 0 && position <= result.Positions[i-1]) {
			return BatchResult{}, fmt.Errorf("%w: invalid batch position order", faults.ErrProtocol)
		}
		result.Positions = append(result.Positions, position)
		if i == 0 {
			result.Target = observation.CompletionTarget{Store: s.store, Namespace: s.namespace, Sequence: s.id, First: copyPointer(&position), EventTypeTails: make(map[events.TypeID]events.SequenceNumber)}
		}
		result.Target.EventTypeTails[refs[i].ID] = position
	}
	return result, nil
}
