// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions

import (
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/eventsequences"
)

// GetConstraintViolations returns a defensive copy of the commit's constraint diagnostics.
func (u *UnitOfWork) GetConstraintViolations() []constraints.Violation {
	result, _ := u.Result() // Invalid/uncompleted units have no diagnostics.
	return result.ConstraintViolations
}

// GetConcurrencyViolations returns a defensive copy of the commit's labeled conflicts.
func (u *UnitOfWork) GetConcurrencyViolations() []eventsequences.ConcurrencyViolation {
	result, _ := u.Result() // Operation errors remain available through Result.
	return result.ConcurrencyViolations
}

// GetAppendErrors returns a defensive copy of the kernel's append error codes.
// Transport and pre-dispatch failures are operation errors available through Result.
func (u *UnitOfWork) GetAppendErrors() []eventsequences.AppendError {
	result, _ := u.Result()
	return result.Errors
}
