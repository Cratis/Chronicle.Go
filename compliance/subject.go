// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package compliance

import (
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
)

// InvalidSubjectError identifies an empty or reserved compliance subject. It
// never retains the identifier, which may itself contain personal data.
type InvalidSubjectError struct {
	// Reserved distinguishes a confidentiality key identifier from an empty subject.
	Reserved bool
}

// Error returns a payload-free description.
func (e *InvalidSubjectError) Error() string {
	if e.Reserved {
		return "chronicle: confidentiality key identifiers cannot be compliance subjects"
	}
	return "chronicle: a nonempty compliance subject is required"
}

// Unwrap exposes the invalid-configuration category.
func (e *InvalidSubjectError) Unwrap() error { return faults.ErrInvalidConfiguration }

// ValidateSubject rejects empty subjects and identifiers in the kernel's reserved
// confidentiality key space. Call before admitting writes or operating on PII keys.
// Event subjects are checked even for currently unclassified events, since their
// subject can propagate to protected read models.
func ValidateSubject(subject string) error {
	if subject == "" {
		return &InvalidSubjectError{}
	}
	if strings.HasPrefix(subject, "$chronicle-encrypted-value$") {
		return &InvalidSubjectError{Reserved: true}
	}
	return nil
}
