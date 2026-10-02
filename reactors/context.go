// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
)

// Batch is immutable scope-opening metadata. It intentionally has no event or
// actor: batches can contain several events, each with its own invocation context.
type Batch struct {
	Reactor   ID
	Store     metadata.StoreName
	Namespace metadata.Namespace
	Sequence  events.SequenceID
}
type batchKey struct{}

// WithBatch installs coordinates before opening operation resources. Intended for
// observer runtime adapters, never for installing a resolver in context.
func WithBatch(ctx context.Context, batch Batch) context.Context {
	return context.WithValue(ctx, batchKey{}, batch)
}

// BatchFromContext returns scope-opening coordinates, when inside a delivery.
func BatchFromContext(ctx context.Context) (Batch, bool) {
	batch, ok := ctx.Value(batchKey{}).(Batch)
	return batch, ok
}
