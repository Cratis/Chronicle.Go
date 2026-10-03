// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package appendorigin carries SDK-owned unit completion attribution across
// package boundaries. External callers cannot construct this capability.
package appendorigin

import "context"

type unitKey struct{}
type unitOrigin[T, B comparable] struct {
	origin T
	batch  B
}

// WithUnit installs an explicit origin for only this unit's prepared batch.
func WithUnit[T, B comparable](ctx context.Context, origin T, batch B) context.Context {
	return context.WithValue(ctx, unitKey{}, unitOrigin[T, B]{origin: origin, batch: batch})
}

// Unit returns an SDK-installed origin only for the owner's exact batch. Even
// callbacks receiving this context cannot bypass resolution for other appends.
func Unit[T, B comparable](ctx context.Context, batch B) (T, bool) {
	value, ok := ctx.Value(unitKey{}).(unitOrigin[T, B])
	return value.origin, ok && value.batch == batch
}
