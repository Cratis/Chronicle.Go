// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/fundamentals.go/concepts"
)

// Unmarshal decodes a JSON object through the same field plan as Marshal. target
// must be a non-nil pointer to the plan's type. It is replaced only on success.
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
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return fmt.Errorf("%w: event must be a JSON object", faults.ErrProtocol)
	}
	decoded := reflect.New(p.typ).Elem()
	if err := p.root.decode(data, decoded, 0); err != nil {
		return &UnmarshalError{cause: err}
	}
	value.Elem().Set(decoded)
	return nil
}

func (n *node) decode(data []byte, value reflect.Value, depth int) error {
	if depth > 256 {
		return faults.ErrProtocol
	}
	if n.reference != nil {
		return n.reference.decode(data, value, depth)
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return json.Unmarshal(data, value.Addr().Interface())
	}
	if n.concept != nil {
		if err := concepts.CheckJSON(*n.concept, data); err != nil {
			return err
		}
		return json.Unmarshal(data, value.Addr().Interface())
	}
	if n.scalar {
		return json.Unmarshal(data, value.Addr().Interface())
	}
	switch value.Kind() {
	case reflect.Pointer:
		value.Set(reflect.New(value.Type().Elem()))
		return n.item.decode(data, value.Elem(), depth+1)
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
			if ok {
				target, err := fieldValue(value, field.index, true)
				if err != nil {
					return err
				}
				if err := field.value.decode(raw, target, depth+1); err != nil {
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
			if err := n.item.decode(items[i], value.Index(i), depth+1); err != nil {
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
			item := reflect.New(value.Type().Elem()).Elem()
			if err := n.item.decode(raw, item, depth+1); err != nil {
				return err
			}
			mapKey := reflect.New(value.Type().Key()).Elem()
			mapKey.SetString(key)
			value.SetMapIndex(mapKey, item)
		}
		return nil
	default:
		return json.Unmarshal(data, value.Addr().Interface())
	}
}
