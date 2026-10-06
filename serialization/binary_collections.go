// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "reflect"

// Client options normalize declared nonnullable binary on event reads too.
// Preserve the existing event behavior for every unrelated collection shape.
func (n *node) normalizeBinaryCollections(value reflect.Value) error {
	if n.reference != nil {
		return n.reference.normalizeBinaryCollections(value)
	}
	if n.binary {
		if value.IsNil() {
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		}
		return nil
	}
	if n.scalar || n.concept != nil || n.family {
		return nil
	}
	switch value.Kind() {
	case reflect.Pointer:
		if !value.IsNil() {
			return n.item.normalizeBinaryCollections(value.Elem())
		}
	case reflect.Struct:
		for _, f := range n.fields {
			if !f.value.containsBinary {
				continue
			}
			child, err := fieldValue(value, f.index, false)
			if err != nil {
				return err
			}
			if child.IsValid() {
				if err := f.value.normalizeBinaryCollections(child); err != nil {
					return err
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if !n.item.containsBinary {
			return nil
		}
		if value.Kind() == reflect.Slice && value.IsNil() {
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		}
		for i := range value.Len() {
			if err := n.item.normalizeBinaryCollections(value.Index(i)); err != nil {
				return err
			}
		}
	}
	return nil
}
