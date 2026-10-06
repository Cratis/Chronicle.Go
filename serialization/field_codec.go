// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "reflect"

// Element returns the compiled element metadata of a slice, array or map, after
// unwrapping pointers. It retains the owning path but does not invent a writable
// element path. Unlike recompilation from Type, it preserves family registrations.
func (f Field) Element() (Field, bool) {
	n := f.plan
	if n == nil {
		return Field{}, false
	}
	for n.typ.Kind() == reflect.Pointer {
		n = n.item
	}
	if n.reference != nil {
		n = n.reference
	}
	if n.scalar || n.item == nil || n.typ.Kind() != reflect.Slice && n.typ.Kind() != reflect.Array && n.typ.Kind() != reflect.Map {
		return Field{}, false
	}
	f.plan, f.Type, f.Collection = n.item, n.item.typ, true
	f.Scalar, f.Format = classify(n.item)
	f.Nullable = f.Type.Kind() == reflect.Pointer || f.Type.Kind() == reflect.Interface
	f.Index = append([]int(nil), f.Index...)
	return f, true
}

// SameRepresentation compares the closed compiled codec graphs, including exact
// declared types, names and derivative identities. It is stricter than scalar
// conversion compatibility and never executes a callback. Projection compilers
// can use it when copying an entire open family without inferring child mappings.
func (f Field) SameRepresentation(other Field) bool {
	if f.plan == nil || other.plan == nil {
		return false
	}
	return sameRepresentation(f.plan, other.plan, map[[2]*node]bool{})
}
func sameRepresentation(a, b *node, seen map[[2]*node]bool) bool {
	if a.reference != nil {
		a = a.reference
	}
	if b.reference != nil {
		b = b.reference
	}
	if !sameEnum(a.enum, b.enum) {
		return false
	}
	if a.typ != b.typ || a.family != b.family || a.scalar != b.scalar || a.binary != b.binary || len(a.fields) != len(b.fields) || len(a.derivatives) != len(b.derivatives) {
		return false
	}
	pair := [2]*node{a, b}
	if seen[pair] {
		return true
	}
	seen[pair] = true
	if (a.item == nil) != (b.item == nil) {
		return false
	}
	if a.item != nil && !sameRepresentation(a.item, b.item, seen) {
		return false
	}
	for i, f := range a.fields {
		other := b.fields[i]
		if f.name != other.name || f.omitEmpty != other.omitEmpty || f.omitZero != other.omitZero || !sameRepresentation(f.value, other.value, seen) {
			return false
		}
	}
	for _, d := range a.derivatives {
		found := false
		for _, other := range b.derivatives {
			if d.registration.id == other.registration.id && d.registration.concrete == other.registration.concrete {
				if !sameRepresentation(d.node, other.node, seen) {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
