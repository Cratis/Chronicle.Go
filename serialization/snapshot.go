// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"reflect"
)

// Marshal serializes a value of the field's exact declared type through its codec.
// Unlike object serialization, a nil field value is retained as JSON null. It
// does not apply encryption. The caller must not mutate value during the call.
func (f Field) Marshal(value any) ([]byte, error) {
	v := reflect.ValueOf(value)
	if f.plan == nil || !v.IsValid() || v.Type() != f.Type {
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

// RebindJSON renames an already serialized snapshot by field identity into next.
// Both plans must describe the same Go type. Values, including explicit nulls and
// omitted properties, are retained without invoking user codecs or callbacks.
// Object keys are canonicalized in sorted order; dictionary keys are not renamed.
func (p *Plan) RebindJSON(data []byte, next *Plan) ([]byte, error) {
	if p == nil || next == nil || p.typ != next.typ {
		return nil, unsupported(nil, "matching snapshot plans required")
	}
	bound, err := rebindJSON(data, p.root, next.root, 0)
	if err != nil {
		return nil, err
	}
	return escapeJSONStrings(bound)
}

func rebindJSON(data json.RawMessage, before, after *node, depth int) (json.RawMessage, error) {
	if depth > 256 {
		return nil, unsupported(before.typ, "JSON nesting exceeds 256 levels")
	}
	if before.reference != nil {
		before = before.reference
	}
	if after.reference != nil {
		after = after.reference
	}
	if string(data) == "null" || before.scalar || before.concept != nil {
		return data, nil
	}
	switch before.typ.Kind() {
	case reflect.Pointer:
		return rebindJSON(data, before.item, after.item, depth+1)
	case reflect.Struct:
		var object map[string]json.RawMessage
		if err := json.Unmarshal(data, &object); err != nil || object == nil {
			return nil, unsupported(before.typ, "snapshot requires an object")
		}
		result := make(map[string]json.RawMessage, len(object))
		for i, field := range before.fields {
			value, exists := object[field.name]
			if !exists {
				continue
			}
			delete(object, field.name)
			value, err := rebindJSON(value, field.value, after.fields[i].value, depth+1)
			if err != nil {
				return nil, err
			}
			result[after.fields[i].name] = value
		}
		if len(object) != 0 {
			return nil, unsupported(before.typ, "unknown snapshot property")
		}
		return json.Marshal(result)
	case reflect.Slice, reflect.Array:
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
	case reflect.Map:
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
	default:
		return data, nil
	}
}
