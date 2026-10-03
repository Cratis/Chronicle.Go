// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import "errors"

// ErrSequenceNumberRange identifies actual-position overflow, underflow or a
// result crossing into the reserved values. Failed arithmetic returns Unavailable.
// Inspect this stable category with errors.Is.
var ErrSequenceNumberRange = errors.New("sequence number outside actual range")

// IsActualValue reports whether n is an actual position, including First (zero).
// All three reserved values (Unavailable, Max and BeforeFirst) are non-actual.
func (n SequenceNumber) IsActualValue() bool { return n < BeforeFirst }

// IsUnavailable reports whether n is the Unavailable sentinel.
func (n SequenceNumber) IsUnavailable() bool { return n == Unavailable }

// IsBeforeFirst reports whether n is the local BeforeFirst sentinel.
func (n SequenceNumber) IsBeforeFirst() bool { return n == BeforeFirst }

// Next returns the next actual position, or preserves a reserved receiver.
// BeforeFirst.Next returns BeforeFirst, not First. A range failure returns
// Unavailable and ErrSequenceNumberRange. This pure operation performs no I/O;
// it is distinct from eventsequences.Sequence.Next(ctx), which queries the tail.
func (n SequenceNumber) Next() (SequenceNumber, error) { return n.Add(1) }

// Add adds an unsigned delta to an actual position without wrapping or entering
// the reserved range. A reserved receiver is returned unchanged with nil error,
// regardless of delta. A range failure returns Unavailable and ErrSequenceNumberRange.
// Reject negative signed amounts before converting them to uint64.
func (n SequenceNumber) Add(delta uint64) (SequenceNumber, error) {
	if !n.IsActualValue() {
		return n, nil
	}
	if delta > uint64(BeforeFirst-1-n) {
		return Unavailable, ErrSequenceNumberRange
	}
	return n + SequenceNumber(delta), nil
}

// Subtract subtracts an unsigned delta from an actual position without wrapping.
// A reserved receiver is returned unchanged with nil error, regardless of delta.
// Underflow returns Unavailable and ErrSequenceNumberRange. Reject negative signed
// amounts before converting them to uint64.
func (n SequenceNumber) Subtract(delta uint64) (SequenceNumber, error) {
	if !n.IsActualValue() {
		return n, nil
	}
	if delta > uint64(n) {
		return Unavailable, ErrSequenceNumberRange
	}
	return n - SequenceNumber(delta), nil
}
