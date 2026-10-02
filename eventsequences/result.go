// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package eventsequences appends registered events to Chronicle sequences.
// Event logs use the same Sequence implementation. Writes are never retried by the SDK.
package eventsequences

import (
	"errors"
	"fmt"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/observation"
)

// Disposition distinguishes unknown, known rejection and committed writes.
type Disposition uint8

const (
	// Unknown means no authoritative write disposition is available; never assume success.
	Unknown Disposition = iota
	// Rejected means the kernel rejected the append without committing it.
	Rejected
	// Committed means the kernel confirmed persistence.
	Committed
)

// AppendError preserves a kernel error code verbatim, including future codes.
type AppendError string

func (e AppendError) Error() string { return string(e) }

// ConcurrencyViolation preserves expected and actual tails, including sentinels.
type ConcurrencyViolation struct {
	// SourceID is the source named by the kernel.
	SourceID events.SourceID
	// Expected is the supplied sequence-wide expectation.
	Expected events.SequenceNumber
	// Actual is the kernel's matching tail.
	Actual events.SequenceNumber
}

// AppendResult owns all returned diagnostics and completion coordinates. Domain
// rejection returns this result with a nil operation error; call Err to promote it.
type AppendResult struct {
	// Disposition records whether the write committed, was rejected, or is unknown.
	Disposition Disposition
	// Position is present only for a confirmed commit; position zero is valid.
	Position *events.SequenceNumber
	// CorrelationID is the kernel's correlation for this append.
	CorrelationID metadata.CorrelationID
	// ConstraintViolations preserves all kernel constraint details.
	ConstraintViolations []constraints.Violation
	// ConcurrencyViolations preserves the single-append concurrency violation, if any.
	ConcurrencyViolations []ConcurrencyViolation
	// Errors preserves all kernel append error codes.
	Errors []AppendError
	// ConcurrencyCheckPerformed is the kernel's actual check flag, not an assumption.
	ConcurrencyCheckPerformed bool
	// Target identifies committed work for future observer completion APIs.
	Target observation.CompletionTarget
}

// ConstraintError promotes constraint rejections for errors.As callers.
type ConstraintError struct {
	// Violations contains the rejected constraints.
	Violations []constraints.Violation
}

func (e *ConstraintError) Error() string { return "chronicle: append rejected by constraints" }

// ConcurrencyError promotes concurrency rejections for errors.As callers.
type ConcurrencyError struct {
	// Violations contains the expected/actual conflicts.
	Violations []ConcurrencyViolation
}

func (e *ConcurrencyError) Error() string { return "chronicle: append rejected by concurrency" }

// OutcomeUnknownError means an append may have committed despite the operation
// error. Do not blindly retry. Unwrap preserves transport status and context identity.
type OutcomeUnknownError struct {
	// Cause is the original failure.
	Cause error
}

func (e *OutcomeUnknownError) Error() string {
	return fmt.Sprintf("chronicle: append outcome unknown: %v", e.Cause)
}
func (e *OutcomeUnknownError) Unwrap() error { return e.Cause }

// Err returns nil only for Committed results without diagnostics. It exposes both
// constraint and concurrency errors via errors.As when both occur.
func (r AppendResult) Err() error {
	var result []error
	if len(r.ConstraintViolations) > 0 {
		result = append(result, &ConstraintError{Violations: r.ConstraintViolations})
	}
	if len(r.ConcurrencyViolations) > 0 {
		result = append(result, &ConcurrencyError{Violations: r.ConcurrencyViolations})
	}
	for _, err := range r.Errors {
		result = append(result, err)
	}
	if len(result) == 0 && r.Disposition != Committed {
		return fmt.Errorf("chronicle: append has no committed disposition")
	}
	return errors.Join(result...)
}
