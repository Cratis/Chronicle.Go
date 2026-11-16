// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"reflect"
	"slices"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/jsonstructure"
)

// Marshal serializes a value of the field's exact declared type through its codec.
// Unlike object serialization, a nil field value is retained as JSON null. It
// does not apply encryption. The caller must not mutate value during the call.
func (f Field) Marshal(value any) ([]byte, error) {
	v := f.snapshotValue(value)
	if !f.matchesSnapshotValue(v) {
		return nil, unsupported(f.Type, "value does not match field codec")
	}
	encoded, err := f.plan.encode(v, false, &encodeState{active: map[valueVisit]bool{}}, 0)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(encoded)
	if err != nil {
		return nil, err
	}
	return escapeJSONStrings(data)
}

func (f Field) snapshotValue(value any) reflect.Value {
	v := reflect.ValueOf(value)
	if f.Type == nil || f.Type.Kind() != reflect.Interface {
		return v
	}
	if value == nil {
		v = reflect.New(f.Type).Elem()
	}
	if v.IsValid() && v.Type().AssignableTo(f.Type) {
		wrapped := reflect.New(f.Type).Elem()
		wrapped.Set(v)
		v = wrapped
	}
	return v
}

func (f Field) matchesSnapshotValue(v reflect.Value) bool {
	return f.plan != nil && v.IsValid() && v.Type() == f.Type
}

// RebindJSON renames an already serialized snapshot by field identity into next.
// Both plans must describe the same Go type. Values, including explicit nulls and
// omitted properties, are retained without invoking user codecs or callbacks.
// Object keys are canonicalized in sorted order; dictionary keys are not renamed.
func (p *Plan) RebindJSON(data []byte, next *Plan) ([]byte, error) {
	if p == nil || next == nil || p.typ != next.typ {
		return nil, unsupported(nil, "matching snapshot plans required")
	}
	if err := rebindEnumProfiles(p.root, next.root); err != nil {
		return nil, err
	}
	if err := jsonstructure.Validate(data); err != nil {
		return nil, &UnmarshalError{cause: err}
	}
	bound, err := rebindJSON(data, p.root, next.root, 0)
	if err != nil {
		return nil, err
	}
	return escapeJSONStrings(bound)
}

func snapshotFieldError(typ reflect.Type, f field) error {
	return &declarations.DeclarationError{Artifact: typ.String(), GoField: f.goName, Path: f.name, Offset: -1, Message: "snapshot field has no unique matching plan field", Cause: faults.ErrInvalidConfiguration}
}

func rebindJSON(data json.RawMessage, before, after *node, depth int) (json.RawMessage, error) {
	if depth > 256 {
		return nil, unsupported(before.typ, "JSON nesting exceeds 256 levels")
	}
	before = snapshotNode(before)
	after = snapshotNode(after)
	if before.binary != after.binary {
		return nil, unsupported(before.typ, "snapshot binary representation changed")
	}
	if snapshotLeaf(data, before) {
		return data, nil
	}
	if before.family {
		return rebindFamilyJSON(data, before, after, depth)
	}
	if before.typ != after.typ || before.family != after.family {
		return nil, unsupported(before.typ, "snapshot representation changed")
	}
	return rebindCompositeJSON(data, before, after, depth)
}

func snapshotNode(n *node) *node {
	if n.reference != nil {
		return n.reference
	}
	return n
}

func snapshotLeaf(data json.RawMessage, n *node) bool {
	return string(data) == "null" || n.scalar || n.concept != nil
}

func rebindCompositeJSON(data json.RawMessage, before, after *node, depth int) (json.RawMessage, error) {
	switch before.typ.Kind() {
	case reflect.Pointer:
		return rebindJSON(data, before.item, after.item, depth+1)
	case reflect.Struct:
		return rebindObjectJSON(data, before, after, depth)
	case reflect.Slice, reflect.Array:
		return rebindArrayJSON(data, before, after, depth)
	case reflect.Map:
		return rebindMapJSON(data, before, after, depth)
	default:
		return data, nil
	}
}

func rebindFamilyJSON(data json.RawMessage, before, after *node, depth int) (json.RawMessage, error) {
	id, err := discriminator(data)
	if err != nil {
		return nil, &UnmarshalError{cause: err}
	}
	source := snapshotDerivative(before, id)
	target := snapshotDerivative(after, id)
	if source == nil || target == nil || source.typ != target.typ {
		return nil, unsupported(before.typ, "snapshot derivative representation changed")
	}
	return rebindDerivativeJSON(data, source, target, depth)
}

func snapshotDerivative(n *node, id string) *node {
	var selected *node
	for _, d := range n.derivatives {
		if d.registration.id == id {
			selected = d.node
		}
	}
	return selected
}

func rebindDerivativeJSON(data json.RawMessage, before, after *node, depth int) (json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	discriminatorJSON := object[derivedTypeID]
	delete(object, derivedTypeID)
	plain, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	rebound, err := rebindJSON(plain, before, after, depth+1)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rebound, &object); err != nil {
		return nil, err
	}
	object[derivedTypeID] = discriminatorJSON
	return json.Marshal(object)
}

func rebindObjectJSON(data json.RawMessage, before, after *node, depth int) (json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return nil, unsupported(before.typ, "snapshot requires an object")
	}
	result := make(map[string]json.RawMessage, len(object))
	for _, original := range before.fields {
		value, exists := object[original.name]
		if !exists {
			continue
		}
		delete(object, original.name)
		target, err := matchingSnapshotField(before.typ, original, after.fields)
		if err != nil {
			return nil, err
		}
		value, err = rebindJSON(value, original.value, target.value, depth+1)
		if err != nil {
			return nil, err
		}
		result[target.name] = value
	}
	if len(object) != 0 {
		return nil, unsupported(before.typ, "unknown snapshot property")
	}
	return json.Marshal(result)
}

func matchingSnapshotField(typ reflect.Type, original field, fields []field) (*field, error) {
	// Naming can change embedded-field promotion, including which fields
	// survive. Match the declared Go field, never position or JSON name.
	var target *field
	for i := range fields {
		candidate := &fields[i]
		if !slices.Equal(original.index, candidate.index) || original.value.typ != candidate.value.typ {
			continue
		}
		if target != nil {
			return nil, snapshotFieldError(typ, original)
		}
		target = candidate
	}
	if target == nil {
		return nil, snapshotFieldError(typ, original)
	}
	return target, nil
}

func rebindArrayJSON(data json.RawMessage, before, after *node, depth int) (json.RawMessage, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, err
	}
	for i, item := range items {
		value, err := rebindJSON(item, before.item, after.item, depth+1)
		if err != nil {
			return nil, err
		}
		items[i] = value
	}
	return json.Marshal(items)
}

func rebindMapJSON(data json.RawMessage, before, after *node, depth int) (json.RawMessage, error) {
	var items map[string]json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, err
	}
	for key, item := range items {
		value, err := rebindJSON(item, before.item, after.item, depth+1)
		if err != nil {
			return nil, err
		}
		items[key] = value
	}
	return json.Marshal(items)
}
