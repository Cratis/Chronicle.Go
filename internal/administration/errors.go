// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package administration shares failure semantics for explicit administration mutations.
package administration

import (
	"errors"
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
)

// OutcomeUnknownError means a dispatched mutation may have taken effect. Never
// retry automatically. Cause preserves protocol, envelope, context and RPC identities.
type OutcomeUnknownError struct {
	// Operation identifies the attempted administration action.
	Operation string
	// Cause is the original failure, not evidence that the server rolled back.
	Cause error
}

func (e *OutcomeUnknownError) Error() string {
	return fmt.Sprintf("chronicle: %s outcome unknown: %v", e.Operation, e.Cause)
}
func (e *OutcomeUnknownError) Unwrap() error { return e.Cause }

// MutationError distinguishes known local/authorization rejection from unknown effects.
func MutationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var local *faults.BeforeDispatch
	if errors.As(err, &local) {
		return local.Cause
	}
	var envelope *wire.EnvelopeError
	if errors.As(err, &envelope) && len(envelope.ExceptionMessages) == 0 {
		return err
	}
	return &OutcomeUnknownError{Operation: operation, Cause: wire.RPCError(err)}
}
