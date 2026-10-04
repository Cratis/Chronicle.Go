// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package identities

import "errors"

// Name is an identity display name, not a subject or username. Rename preserves
// accepted names exactly; the zero value is not a valid requested name.
type Name string

// String returns the unmodified display name.
func (n Name) String() string { return string(n) }

// GoString returns the unmodified display name.
func (n Name) GoString() string { return string(n) }

// ErrNotFound means a successful identity listing contained no exact subject.
var ErrNotFound = errors.New("chronicle: identity not found")

// NotFoundError reports absence without retaining the requested identity.
type NotFoundError struct{}

func (*NotFoundError) Error() string { return "chronicle: identity not found" }

// Unwrap exposes ErrNotFound.
func (*NotFoundError) Unwrap() error { return ErrNotFound }
