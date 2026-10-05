// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"fmt"
	"io"

	"github.com/cratis/chronicle.go/internal/faults"
)

var (
	// ErrNotPrepared identifies access before successful preparation.
	ErrNotPrepared = errors.New("chronicle: client is not prepared")
	// ErrPreparationInProgress identifies a concurrent or reentrant Prepare call.
	ErrPreparationInProgress = errors.New("chronicle: preparation is in progress")
	// ErrClosed identifies an operation attempted after Close.
	ErrClosed = faults.ErrClosed
	// ErrUnsupported identifies a capability not implemented by this client.
	ErrUnsupported = faults.ErrUnsupported
	// ErrNotRegistered identifies an event missing from the client's frozen catalog.
	ErrNotRegistered = faults.ErrNotRegistered
	// ErrInvalidConfiguration identifies invalid client configuration.
	ErrInvalidConfiguration = faults.ErrInvalidConfiguration
	// ErrProtocol identifies a missing or inconsistent kernel response.
	ErrProtocol = faults.ErrProtocol
)

// ClientStateError reports a denied operation with payload-free state diagnostics.
// It never includes configuration, coordinates, application objects or errors.
type ClientStateError struct {
	// Operation is the fixed SDK operation name.
	Operation string
	cause     error
}

// Error returns the operation and state category only.
func (e *ClientStateError) Error() string { return e.cause.Error() + " during " + e.Operation }

// Unwrap exposes ErrNotPrepared, ErrPreparationInProgress or ErrClosed.
func (e *ClientStateError) Unwrap() error { return e.cause }

// Format preserves payload-free diagnostics for every formatting verb.
func (e *ClientStateError) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, e.Error()) }

// CompatibilityError reports a failed structural compatibility preflight.
// Details are the kernel's incompatibility descriptions; no operations are admitted.
type CompatibilityError struct {
	// ServerVersion identifies the kernel that rejected the descriptor.
	ServerVersion string
	// Details preserves every incompatibility returned by the kernel.
	Details []string
}

func (e *CompatibilityError) Error() string {
	return "chronicle: kernel rejected protocol compatibility"
}
