// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package faults

import "errors"

var (
	ErrClosed               = errors.New("chronicle: client closed")
	ErrUnsupported          = errors.New("chronicle: unsupported capability")
	ErrNotRegistered        = errors.New("chronicle: event type not registered")
	ErrInvalidConfiguration = errors.New("chronicle: invalid configuration")
	ErrProtocol             = errors.New("chronicle: invalid kernel response")
)

// BeforeDispatch marks a locally rejected operation, known not to have reached Invoke.
type BeforeDispatch struct{ Cause error }

func (e *BeforeDispatch) Error() string { return e.Cause.Error() }
func (e *BeforeDispatch) Unwrap() error { return e.Cause }
