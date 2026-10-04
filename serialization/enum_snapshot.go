// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "slices"

// Check the admitted root enum fields even when the snapshot omits them or has
// explicit nulls. A naming-only rebind must never reinterpret a member table.
func rebindEnumProfiles(before, after *node) error {
	check := func(source, target *node) error {
		for _, f := range source.fields {
			if !hasEnum(f.value) {
				continue
			}
			matched := false
			for _, other := range target.fields {
				if slices.Equal(f.index, other.index) && sameRepresentation(f.value, other.value, map[[2]*node]bool{}) {
					matched = true
					break
				}
			}
			if !matched {
				return unsupported(f.value.typ, "snapshot enum representation changed")
			}
		}
		return nil
	}
	if err := check(before, after); err != nil {
		return err
	}
	return check(after, before)
}
