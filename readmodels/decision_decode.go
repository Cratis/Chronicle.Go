// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"encoding/json"
	"errors"

	"github.com/cratis/chronicle.go/internal/faults"
)

// DecisionCodecPanicError means an application model decoder panicked during a
// decision read. It matches ErrProtocol and never retains or formats the panic
// value. The read returns no model or token; cleanup already completed.
type DecisionCodecPanicError struct{}

// Error returns a fixed, payload-free description.
func (*DecisionCodecPanicError) Error() string { return "chronicle: decision model codec panicked" }

// Unwrap preserves the protocol failure category without exposing a panic value.
func (*DecisionCodecPanicError) Unwrap() error { return faults.ErrProtocol }

// decodeDecision is the application-code boundary, after cleanup and lease
// release and before final cancellation/generation/epoch checks and issuance.
func decodeDecision[T any](raw Instance[json.RawMessage], descriptor Descriptor) (instance Instance[T], err error) {
	defer func() {
		if recover() != nil {
			instance = Instance[T]{}
			err = &DecisionCodecPanicError{}
		}
		var codecPanic *CodecPanicError
		if errors.As(err, &codecPanic) {
			instance, err = Instance[T]{}, &DecisionCodecPanicError{}
		}
	}()
	if raw.Exists {
		if err = validateReleasedDocument(descriptor, raw.Value); err != nil {
			return Instance[T]{}, err
		}
		raw.Value, err = normalizeID(raw.Value, descriptor)
		if err != nil {
			return Instance[T]{}, err
		}
	}
	return decode[T](raw, descriptor)
}
