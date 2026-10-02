// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "github.com/cratis/chronicle.go/internal/faults"

var (
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
