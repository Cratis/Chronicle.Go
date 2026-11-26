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
// tagged omitempty/omitzero. Ordinary typed fields preserve the full uint64 range.
// Integers nested under maps or open-derived values outside -2^53 through 2^53
// return ErrUnsupported before dispatch.
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
	// The kernel ignores dictionary value schemas and reads all nested numbers
	// as double. Reject outside its contiguous exact-integer range before RPC.
	if !dictionary {
		return nil
	}
	return n.checkDictionaryInteger(value)
}

func (n *node) checkDictionaryInteger(value reflect.Value) error {
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v := value.Int(); v < -(1<<53) || v > 1<<53 {
			return unsupported(n.typ, "open-schema integers must be between -2^53 and 2^53")
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value.Uint() > 1<<53 {
			return unsupported(n.typ, "open-schema integers must be at most 2^53")
		}
	}
	return nil
}

type valueVisit struct {
	typ     reflect.Type
	pointer uintptr
	length  int
}
type encodeState struct {
	active           map[valueVisit]bool
	panicked         bool
	privateCallbacks bool
}

func (n *node) encode(value reflect.Value, dictionary bool, state *encodeState, depth int) (any, error) {
	if depth > 256 {
		return nil, unsupported(n.typ, "JSON nesting exceeds 256 levels")
	}
	if n.reference != nil {
		return n.reference.encode(value, dictionary, state, depth)
	}
	encoder := nodeEncoder{node: n, dictionary: dictionary, state: state, depth: depth}
	return encoder.encode(value)
}

type nodeEncoder struct {
	node       *node
	dictionary bool
	state      *encodeState
	depth      int
}

func encodingVisit(value reflect.Value) valueVisit {
	switch value.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
		visit := valueVisit{typ: value.Type(), pointer: value.Pointer()}
		if value.Kind() == reflect.Slice {
			visit.length = value.Len()
		}
		return visit
	default:
		return valueVisit{}
	}
}

func (e nodeEncoder) encode(value reflect.Value) (any, error) {
	visit := encodingVisit(value)
	if visit.pointer != 0 {
		if e.state.active[visit] {
			return nil, unsupported(e.node.typ, "cyclic JSON value")
		}
		e.state.active[visit] = true
		defer delete(e.state.active, visit)
	}
	return e.value(value)
}

func (e nodeEncoder) value(value reflect.Value) (any, error) {
	n := e.node
	if n.family {
		return e.family(value)
	}
	if n.binary {
		return encodeBinary(value), nil
	}
	if n.enum != nil {
		return n.encodeEnum(value)
	}
	if n.concept != nil {
		return n.encodeConcept(value, e.dictionary, e.state)
	}
	if err := n.checkInteger(value, e.dictionary); err != nil {
		return nil, err
	}
	if n.scalar {
		return value.Interface(), nil
	}
	return e.composite(value)
}

func (e nodeEncoder) family(value reflect.Value) (any, error) {
	if value.IsNil() {
		return nil, nil
	}
	concrete := value.Elem()
	if concrete.Kind() == reflect.Pointer && concrete.IsNil() {
		return nil, unsupported(e.node.typ, "typed-nil derivative")
	}
	for _, derivative := range e.node.derivatives {
		if derivative.registration.concrete != concrete.Type() {
			continue
		}
		encoded, err := derivative.node.encode(concrete, true, e.state, e.depth+1)
		if err != nil {
			return nil, err
		}
		object, ok := encoded.(orderedObject)
		if !ok {
			return nil, unsupported(e.node.typ, "derivative must encode an object")
		}
		return append(object, objectProperty{name: derivedTypeID, value: derivative.registration.id}), nil
	}
	return nil, unsupported(e.node.typ, "unregistered dynamic derivative")
}

func (e nodeEncoder) composite(value reflect.Value) (any, error) {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return nil, nil
		}
		return e.node.item.encode(value.Elem(), e.dictionary, e.state, e.depth+1)
	case reflect.Struct:
		return e.object(value)
	case reflect.Slice, reflect.Array:
		return e.collection(value)
	case reflect.Map:
		return e.dictionaryValue(value)
	default:
		return value.Interface(), nil
	}
}

func (e nodeEncoder) object(value reflect.Value) (any, error) {
	result := make(orderedObject, 0, len(e.node.fields))
	for _, field := range e.node.fields {
		v, err := fieldValue(value, field.index, false)
		if err != nil {
			return nil, err
		}
		if !v.IsValid() {
			continue
		}
		encoded, err := e.field(field, v)
		if err != nil {
			return nil, err
		}
		if encoded != nil {
			result = append(result, objectProperty{name: field.name, value: encoded})
		}
	}
	return result, nil
}

func (e nodeEncoder) field(field field, value reflect.Value) (any, error) {
	if err := field.value.validateFamilyValue(value); err != nil {
		return nil, fmt.Errorf("property %s: %w", field.name, err)
	}
	if field.omitEmpty && empty(value) {
		return nil, nil
	}
	if field.omitZero {
		zero, err := e.omitZero(field, value)
		if err != nil {
			return nil, err
		}
		if zero {
			return nil, nil
		}
	}
	encoded, err := field.value.encode(value, e.dictionary, e.state, e.depth+1)
	if err != nil {
		return nil, fmt.Errorf("property %s: %w", field.name, err)
	}
	return encoded, nil
}

func (e nodeEncoder) omitZero(field field, value reflect.Value) (bool, error) {
	zero, err := invoke(func() (bool, error) { return field.isZero(value), nil })
	if err != nil {
		if failure, ok := err.(*CallbackError); ok {
			e.state.panicked = failure.panicked
		}
	}
	return zero, err
}

func (e nodeEncoder) collection(value reflect.Value) (any, error) {
	if value.Kind() == reflect.Slice && value.IsNil() {
		return nil, nil
	}
	result := make([]any, value.Len())
	for i := range result {
		encoded, err := e.node.item.encode(value.Index(i), e.dictionary, e.state, e.depth+1)
		if err != nil {
			return nil, err
		}
		if encoded == nil {
			return nil, fmt.Errorf("chronicle: null collection element at %d", i)
		}
		result[i] = encoded
	}
	return result, nil
}

func (e nodeEncoder) dictionaryValue(value reflect.Value) (any, error) {
	if value.IsNil() {
		return nil, nil
	}
	result := make(map[string]any, value.Len())
	iterator := value.MapRange()
	for iterator.Next() {
		encoded, err := e.node.item.encode(iterator.Value(), true, e.state, e.depth+1)
		if err != nil {
			return nil, err
		}
		if encoded == nil {
			return nil, fmt.Errorf("chronicle: null map value")
		}
		result[dictionaryKeyText(iterator.Key())] = encoded
	}
	return result, nil
}

type zeroer interface{ IsZero() bool }

// zeroFunc follows encoding/json's omitzero rules, including pointer-receiver
// methods on unaddressable struct values and nil pointers (without calling them).
func zeroFunc(typ reflect.Type) func(reflect.Value) bool {
	contract := reflect.TypeFor[zeroer]()
	switch {
	case typ.Kind() == reflect.Interface && typ.Implements(contract):
		return nilOrZero
	case typ.Kind() == reflect.Pointer && typ.Implements(contract):
		return nilOrZero
	case typ.Implements(contract):
		return func(v reflect.Value) bool { return v.Interface().(zeroer).IsZero() }
	case reflect.PointerTo(typ).Implements(contract):
		return addressableZero
	default:
		return reflect.Value.IsZero
	}
}

func nilOrZero(v reflect.Value) bool {
	return v.IsNil() || v.Interface().(zeroer).IsZero()
}

func addressableZero(v reflect.Value) bool {
	if !v.CanAddr() {
		copy := reflect.New(v.Type()).Elem()
		copy.Set(v)
		v = copy
	}
	return v.Addr().Interface().(zeroer).IsZero()
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
