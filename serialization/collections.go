// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "reflect"

// Normalize only compiled collection nodes; never inspect codec internals or
// reconstruct metadata from reflection. Interface values need owned settable
// storage even when their registered variant is a value rather than a pointer.
func (n *node) normalizeCollections(value reflect.Value) error {
	if n.reference != nil {
		return n.reference.normalizeCollections(value)
	}
	if n.binary {
		if value.IsNil() {
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		}
		return nil
	}
	if n.scalar || n.concept != nil {
		return nil
	}
	if n.family {
		if value.IsNil() {
			return nil
		}
		for _, derivative := range n.derivatives {
			if value.Elem().Type() != derivative.registration.concrete {
				continue
			}
			concrete := reflect.New(value.Elem().Type()).Elem()
			concrete.Set(value.Elem())
			if err := derivative.node.normalizeCollections(concrete); err != nil {
				return err
			}
			value.Set(concrete)
			return nil
		}
		return unsupported(n.typ, "unregistered derivative during normalization")
	}
	switch value.Kind() {
	case reflect.Pointer:
		if !value.IsNil() {
			return n.item.normalizeCollections(value.Elem())
		}
	case reflect.Struct:
		for _, f := range n.fields {
			child, err := fieldValue(value, f.index, false)
			if err != nil {
				return err
			}
			if child.IsValid() {
				if err := f.value.normalizeCollections(child); err != nil {
					return err
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		}
		for i := 0; i < value.Len(); i++ {
			if err := n.item.normalizeCollections(value.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			entry := reflect.New(value.Type().Elem()).Elem()
			entry.Set(iterator.Value())
			if err := n.item.normalizeCollections(entry); err != nil {
				return err
			}
			value.SetMapIndex(iterator.Key(), entry)
		}
	}
	return nil
}
