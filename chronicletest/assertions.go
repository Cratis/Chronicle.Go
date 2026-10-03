// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import "testing"

// ShouldHaveProduced checks for a matching effect of T. A nil predicate matches
// any T. It returns the first match for further Go assertions. This is recording
// evidence only, not transport acceptance. Use the exact returned pointer/value type.
func ShouldHaveProduced[T any](t testing.TB, produced []any, predicate func(T) bool) T {
	t.Helper()
	for _, effect := range produced {
		if value, ok := effect.(T); ok && (predicate == nil || predicate(value)) {
			return value
		}
	}
	t.Errorf("expected a matching produced %T; recorded %d effects", *new(T), len(produced))
	var zero T
	return zero
}

// ShouldNotHaveProduced fails if a handler returned any effect of T.
func ShouldNotHaveProduced[T any](t testing.TB, produced []any) {
	t.Helper()
	for _, effect := range produced {
		if _, ok := effect.(T); ok {
			t.Errorf("unexpected produced %T", effect)
			return
		}
	}
}

// RequireFidelity fails the test before assertions reach substituted layers.
func RequireFidelity(t testing.TB, fidelity Fidelity, layers ...Layer) {
	t.Helper()
	if err := fidelity.Require(layers...); err != nil {
		t.Fatalf("scenario fidelity: %v", err)
	}
}
