// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "github.com/cratis/chronicle.go/internal/faults"

// UnmarshalError describes invalid JSON or a failed declared decoder without
// disclosing document content. It matches ErrProtocol; errors.Is/As can inspect
// the original cause, whose diagnostics may contain sensitive data.
type UnmarshalError struct{ cause error }

// Error returns a payload-free description, never the decoder's message.
func (*UnmarshalError) Error() string { return "chronicle: invalid JSON content" }

// Is preserves the protocol failure category independently of the decoder cause.
func (*UnmarshalError) Is(target error) bool { return target == faults.ErrProtocol }

// Unwrap exposes the original decoding error for deliberate inspection.
func (e *UnmarshalError) Unwrap() error { return e.cause }
