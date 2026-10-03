// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import "context"

// ReleaseMany releases unreleased sink values in input order. It is not
// idempotent: never pass server-released read results, since legitimate plaintext
// can resemble ciphertext. Input values are never modified. Any failure returns
// a nil slice, never partially released data. Nil input remains nil.
func (r *Reader[T]) ReleaseMany(ctx context.Context, values []T) ([]T, error) {
	if _, err := r.descriptor(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if values == nil {
		return nil, nil
	}
	result := make([]T, len(values))
	for i, value := range values {
		released, err := r.Release(ctx, value)
		if err != nil {
			return nil, err
		}
		result[i] = released
	}
	return result, nil
}
