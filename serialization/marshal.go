// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// Marshal serializes a value or non-nil pointer matching this plan. Nil object
// properties are omitted, while false and zero are preserved unless explicitly
// tagged omitempty/omitzero. The caller must not mutate value during this call.
func (p *Plan) Marshal(value any) ([]byte, error) {
	v := reflect.ValueOf(value)
	if v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, fmt.Errorf("chronicle: nil event")
		}
		v = v.Elem()
	}
	if !v.IsValid() || v.Type() != p.typ {
		return nil, fmt.Errorf("chronicle: value does not match serializer plan")
	}
	encoded, err := p.root.encode(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(encoded)
}

func (n *node) encode(value reflect.Value) (any, error) {
	if n.scalar {
		return value.Interface(), nil
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return nil, nil
		}
		return n.item.encode(value.Elem())
	case reflect.Struct:
		result := make(map[string]any, len(n.fields))
		for _, field := range n.fields {
			v := value.Field(field.index)
			if (field.omitEmpty && empty(v)) || (field.omitZero && v.IsZero()) {
				continue
			}
			encoded, err := field.value.encode(v)
			if err != nil {
				return nil, fmt.Errorf("property %s: %w", field.name, err)
			}
			if encoded != nil {
				result[field.name] = encoded
			}
		}
		return result, nil
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return nil, nil
		}
		result := make([]any, value.Len())
		for i := range result {
			encoded, err := n.item.encode(value.Index(i))
			if err != nil {
				return nil, err
			}
			if encoded == nil {
				return nil, fmt.Errorf("chronicle: null collection element at %d", i)
			}
			result[i] = encoded
		}
		return result, nil
	case reflect.Map:
		if value.IsNil() {
			return nil, nil
		}
		result := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			encoded, err := n.item.encode(iterator.Value())
			if err != nil {
				return nil, err
			}
			if encoded == nil {
				return nil, fmt.Errorf("chronicle: null map value")
			}
			result[iterator.Key().String()] = encoded
		}
		return result, nil
	default:
		return value.Interface(), nil
	}
}

func empty(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map, reflect.String:
		return value.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return value.IsZero()
	default:
		return false
	}
}
