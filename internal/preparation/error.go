// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package preparation contains payload-free outgoing callback boundaries.
package preparation

import (
	"context"
	"fmt"
)

// Error reports a failed outgoing preparation without retaining application errors.
type Error struct {
	Phase         string
	ProviderIndex int
	EventIndex    int
	Panicked      bool
}

func (e *Error) Error() string {
	return fmt.Sprintf("chronicle: outgoing preparation failed (phase=%s provider=%d event=%d panic=%t)", e.Phase, e.ProviderIndex, e.EventIndex, e.Panicked)
}

// Call discards callback failures without invoking any diagnostic/error hooks.
func Call(ctx context.Context, phase string, provider, event int, callback func() error) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() {
		if recover() != nil {
			err = &Error{Phase: phase, ProviderIndex: provider, EventIndex: event, Panicked: true}
		}
		if canceled := ctx.Err(); canceled != nil {
			err = canceled
		}
	}()
	if callback() != nil {
		return &Error{Phase: phase, ProviderIndex: provider, EventIndex: event}
	}
	return nil
}
