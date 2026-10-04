// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactoreffects

import "context"

// Preparation carries classified built-ins across the internal SDK-only seam.
// Type parameters avoid a package cycle without exposing a new appender API.
type Preparation[Entry, Scope any] struct {
	Entries             []Entry
	Scopes              []Scope
	Bare, Single, Batch bool
}

// Preparer returns an execution closure owning only frozen outgoing values.
type Preparer[Entry, Scope any] interface {
	PrepareReturnedEvents(context.Context, Preparation[Entry, Scope]) (func(context.Context) error, error)
}
