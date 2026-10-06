// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/jsonstructure"
	"github.com/cratis/fundamentals.go/concepts"
)

// Unmarshal decodes a JSON object through the same field plan as Marshal. target
// must be a non-nil pointer to the plan's type. It is replaced only on success.
// Duplicate members anywhere in the input are rejected before invoking codecs.
// Unknown properties are ignored; exact JSON names win over case-insensitive
// matches. An exact name declared for another field is never reused as a
// case-insensitive fallback. Concepts use their declared JSON decoder. Decoding
// errors are UnmarshalError values with payload-free messages; errors.Is/As can
// inspect the original cause, which may contain sensitive data.
func (p *Plan) Unmarshal(data []byte, target any) error {
	value := reflect.ValueOf(target)
	if p == nil || !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Type() != p.typ {
		return fmt.Errorf("%w: target does not match serializer plan", faults.ErrInvalidConfiguration)
	}
	if err := jsonstructure.Validate(data); err != nil {
		return &UnmarshalError{cause: err}
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return fmt.Errorf("%w: event must be a JSON object", faults.ErrProtocol)
	}
	decoded := reflect.New(p.typ).Elem()
	if err := p.root.decode(data, decoded, 0); err != nil {
		return &UnmarshalError{cause: err}
	}
	if p.binary {
		if err := p.root.normalizeBinaryCollections(decoded); err != nil {
			return &UnmarshalError{cause: err}
		}
	}
	if p.root.readModelRoot {
		if err := p.root.normalizeCollections(decoded); err != nil {
			return &UnmarshalError{cause: err}
		}
	}
	value.Elem().Set(decoded)
	return nil
}

func decodeLeafJSON(data []byte, value reflect.Value) error {
	target := value.Addr()
	if target.Type().Implements(reflect.TypeFor[json.Unmarshaler]()) || target.Type().Implements(reflect.TypeFor[encoding.TextUnmarshaler]()) {
		_, err := invoke(func() (struct{}, error) { return struct{}{}, json.Unmarshal(data, target.Interface()) })
		return err
	}
	return json.Unmarshal(data, target.Interface())
}

func (n *node) decode(data []byte, value reflect.Value, depth int) error {
	return n.decodeContext(data, value, depth, false)
}

func (n *node) decodeContext(data []byte, value reflect.Value, depth int, open bool) error {
	if depth > 256 {
		return faults.ErrProtocol
	}
	if n.reference != nil {
		return n.reference.decodeContext(data, value, depth, open)
	}
	if n.binary {
		return decodeBinary(data, value)
	}
	if n.enum != nil {
		return n.decodeEnum(data, value)
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		if n.item != nil && n.item.enum != nil {
			value.SetZero()
			return n.decodeMissingEnum(value)
		}
		return decodeLeafJSON(data, value)
	}
	if n.family {
		return n.decodeFamily(data, value, depth)
	}
	if n.concept != nil {
		if err := concepts.CheckJSON(*n.concept, data); err != nil {
			return err
		}
		if open && n.schema["type"] == "integer" {
			underlying := reflect.New(n.concept.Type)
			if err := json.Unmarshal(data, underlying.Interface()); err != nil {
				return err
			}
			if err := n.checkInteger(underlying.Elem(), true); err != nil {
				return err
			}
		}
		return decodeLeafJSON(data, value)
	}
	if n.scalar {
		return decodeLeafJSON(data, value)
	}
	switch value.Kind() {
	case reflect.Pointer:
		value.Set(reflect.New(value.Type().Elem()))
		return n.item.decodeContext(data, value.Elem(), depth+1, open)
	case reflect.Struct:
		var properties map[string]json.RawMessage
		if err := json.Unmarshal(data, &properties); err != nil {
			return err
		}
		declared := make(map[string]bool, len(n.fields))
		for _, field := range n.fields {
			declared[field.name] = true
		}
		for _, field := range n.fields {
			raw, ok := properties[field.name]
			if !ok {
				// Walk sorted keys for deterministic behavior if a payload supplies several
				// non-exact spellings. Ordinary producer payloads always use exact names.
				keys := make([]string, 0, len(properties))
				for key := range properties {
					keys = append(keys, key)
				}
				slices.Sort(keys)
				for _, key := range keys {
					if !declared[key] && strings.EqualFold(key, field.name) {
						raw, ok = properties[key], true
						break
					}
				}
			}
			if !ok && (field.value.enum != nil || field.value.typ.Kind() == reflect.Slice && field.value.item != nil && field.value.item.enum != nil) {
				target, err := fieldValue(value, field.index, true)
				if err != nil {
					return err
				}
				if err := field.value.decodeMissingEnum(target); err != nil {
					return err
				}
			}
			if ok {
				target, err := fieldValue(value, field.index, true)
				if err != nil {
					return err
				}
				if err := field.value.decodeContext(raw, target, depth+1, open); err != nil {
					return err
				}
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			return err
		}
		if value.Kind() == reflect.Slice {
			value.Set(reflect.MakeSlice(value.Type(), len(items), len(items)))
		}
		for i := 0; i < len(items) && i < value.Len(); i++ {
			if dereference(n.item.typ).Kind() == reflect.Interface && bytes.Equal(bytes.TrimSpace(items[i]), []byte("null")) {
				return faults.ErrProtocol
			}
			if err := n.item.decodeContext(items[i], value.Index(i), depth+1, open); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		var items map[string]json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			return err
		}
		value.Set(reflect.MakeMapWithSize(value.Type(), len(items)))
		for key, raw := range items {
			if dereference(n.item.typ).Kind() == reflect.Interface && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return faults.ErrProtocol
			}
			item := reflect.New(value.Type().Elem()).Elem()
			if err := n.item.decodeContext(raw, item, depth+1, open); err != nil {
				return err
			}
			mapKey := reflect.New(value.Type().Key()).Elem()
			mapKey.SetString(key)
			value.SetMapIndex(mapKey, item)
		}
		return nil
	default:
		if err := decodeLeafJSON(data, value); err != nil {
			return err
		}
		if open {
			return n.checkInteger(value, true)
		}
		return nil
	}
}
