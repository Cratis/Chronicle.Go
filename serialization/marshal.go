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
// tagged omitempty/omitzero. Integers nested under maps outside -2^53 through
// 2^53, and unsigned values above MaxInt64, return ErrUnsupported before dispatch.
// Object fields use declaration order (promoted fields use encoding/json order),
// and strings use System.Text.Json's default escaping. Map keys are sorted, not
// insertion-ordered like C# dictionaries. The caller must not mutate value during
// this call.
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
	encoded, err := p.root.encode(v, false, &encodeState{active: map[valueVisit]bool{}}, 0)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(encoded)
	if err != nil {
		return nil, err
	}
	return escapeJSONStrings(data)
}

func (n *node) checkInteger(value reflect.Value, dictionary bool) error {
	// The pinned MongoDB append path parses payload JSON as BSON, whose bare
	// integer parser cannot represent unsigned values above MaxInt64.
	if (value.Kind() == reflect.Uint || value.Kind() == reflect.Uint64) && value.Uint() > 1<<63-1 {
		return unsupported(n.typ, "kernel MongoDB append requires unsigned integers at most MaxInt64")
	}
	// The kernel ignores dictionary value schemas and reads all nested numbers
	// as double. Reject outside its contiguous exact-integer range before RPC.
	if dictionary {
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if v := value.Int(); v < -(1<<53) || v > 1<<53 {
				return unsupported(n.typ, "dictionary integers must be between -2^53 and 2^53")
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if value.Uint() > 1<<53 {
				return unsupported(n.typ, "dictionary integers must be at most 2^53")
			}
		}
	}
	return nil
}

type valueVisit struct {
	typ     reflect.Type
	pointer uintptr
	length  int
}
type encodeState struct{ active map[valueVisit]bool }

func (n *node) encode(value reflect.Value, dictionary bool, state *encodeState, depth int) (any, error) {
	if depth > 256 {
		return nil, unsupported(n.typ, "JSON nesting exceeds 256 levels")
	}
	if n.reference != nil {
		return n.reference.encode(value, dictionary, state, depth)
	}
	if value.Kind() == reflect.Pointer || value.Kind() == reflect.Map || value.Kind() == reflect.Slice {
		visit := valueVisit{typ: value.Type(), pointer: value.Pointer()}
		if value.Kind() == reflect.Slice {
			visit.length = value.Len()
		}
		if visit.pointer != 0 {
			if state.active[visit] {
				return nil, unsupported(n.typ, "cyclic JSON value")
			}
			state.active[visit] = true
			defer delete(state.active, visit)
		}
	}
	if n.concept != nil {
		return n.encodeConcept(value, dictionary)
	}
	if err := n.checkInteger(value, dictionary); err != nil {
		return nil, err
	}
	if n.scalar {
		return value.Interface(), nil
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return nil, nil
		}
		return n.item.encode(value.Elem(), dictionary, state, depth+1)
	case reflect.Struct:
		result := make(orderedObject, 0, len(n.fields))
		for _, field := range n.fields {
			v, err := fieldValue(value, field.index, false)
			if err != nil {
				return nil, err
			}
			if !v.IsValid() || (field.omitEmpty && empty(v)) || (field.omitZero && field.isZero(v)) {
				continue
			}
			encoded, err := field.value.encode(v, dictionary, state, depth+1)
			if err != nil {
				return nil, fmt.Errorf("property %s: %w", field.name, err)
			}
			if encoded != nil {
				result = append(result, objectProperty{name: field.name, value: encoded})
			}
		}
		return result, nil
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return nil, nil
		}
		result := make([]any, value.Len())
		for i := range result {
			encoded, err := n.item.encode(value.Index(i), dictionary, state, depth+1)
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
			encoded, err := n.item.encode(iterator.Value(), true, state, depth+1)
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

type zeroer interface{ IsZero() bool }

// zeroFunc follows encoding/json's omitzero rules, including pointer-receiver
// methods on unaddressable struct values and nil pointers (without calling them).
func zeroFunc(typ reflect.Type) func(reflect.Value) bool {
	contract := reflect.TypeFor[zeroer]()
	switch {
	case typ.Kind() == reflect.Pointer && typ.Implements(contract):
		return func(v reflect.Value) bool { return v.IsNil() || v.Interface().(zeroer).IsZero() }
	case typ.Implements(contract):
		return func(v reflect.Value) bool { return v.Interface().(zeroer).IsZero() }
	case reflect.PointerTo(typ).Implements(contract):
		return func(v reflect.Value) bool {
			if !v.CanAddr() {
				copy := reflect.New(v.Type()).Elem()
				copy.Set(v)
				v = copy
			}
			return v.Addr().Interface().(zeroer).IsZero()
		}
	default:
		return reflect.Value.IsZero
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
