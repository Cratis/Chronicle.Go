// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions

import "context"

type unitOfWorkKey struct{}

// WithUnitOfWork exposes only the participant to nested execution. It neither
// starts work nor changes correlation/identity metadata. Pass a context derived
// from Begin's context. A nil unit masks any prior participant.
func WithUnitOfWork(ctx context.Context, unit *UnitOfWork) context.Context {
	return context.WithValue(ctx, unitOfWorkKey{}, unit)
}

// FromContext returns the same joined participant, even after completion. It
// never creates a successor unit or reveals the completion owner. Missing or
// explicitly masked participants return nil, false.
func FromContext(ctx context.Context) (*UnitOfWork, bool) {
	unit, _ := ctx.Value(unitOfWorkKey{}).(*UnitOfWork)
	return unit, unit != nil
}
