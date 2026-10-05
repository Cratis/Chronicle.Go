// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "github.com/cratis/chronicle.go/internal/faults"

// CallbackError reports an application codec or IsZero failure without formatting
// its cause. errors.Is/As expose ordinary causes for deliberate inspection; causes
// may contain personal data and must not be logged indiscriminately.
type CallbackError struct {
	cause    error
	panicked bool
}

// Error returns a payload-free diagnostic.
func (*CallbackError) Error() string { return "chronicle: serialization callback failed" }

// Unwrap preserves ordinary callback error identity.
func (e *CallbackError) Unwrap() error { return e.cause }

// Is identifies an unsupported serialization result.
func (*CallbackError) Is(target error) bool { return target == faults.ErrUnsupported }

// CallbackPanicError identifies an application panic. The panic value is neither
// retained nor formatted, including when that value implements error.
type CallbackPanicError struct{}

// Error returns a payload-free diagnostic.
func (*CallbackPanicError) Error() string { return "chronicle: serialization callback panicked" }

func invoke[T any](callback func() (T, error)) (result T, err error) {
	defer func() {
		if recover() != nil {
			var zero T
			result = zero
			err = &CallbackError{cause: &CallbackPanicError{}, panicked: true}
		}
	}()
	result, err = callback()
	if err != nil {
		err = &CallbackError{cause: err}
	}
	return
}
