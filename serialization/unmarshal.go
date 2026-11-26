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
	"time"

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
	if !p.acceptsDecodeTarget(value) {
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
	if err := p.normalizeDecoded(decoded); err != nil {
		return &UnmarshalError{cause: err}
	}
	value.Elem().Set(decoded)
	return nil
}

func (p *Plan) acceptsDecodeTarget(value reflect.Value) bool {
	if p == nil || !value.IsValid() {
		return false
	}
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return false
	}
	return value.Elem().Type() == p.typ
}

func (p *Plan) normalizeDecoded(value reflect.Value) error {
	if p.binary {
		if err := p.root.normalizeBinaryCollections(value); err != nil {
			return err
		}
	}
	if !p.root.readModelRoot {
		return nil
	}
	return p.root.normalizeCollections(value)
}

func decodeLeafJSON(data []byte, value reflect.Value) error {
	if value.Type() == reflect.TypeFor[time.Time]() {
		data = unescapedString(data)
	}
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
		return n.decodeNull(data, value)
	}
	return n.decodeNonNull(data, value, depth, open)
}

func (n *node) decodeNull(data []byte, value reflect.Value) error {
	if n.item != nil && n.item.enum != nil {
		value.SetZero()
		return n.decodeMissingEnum(value)
	}
	return decodeLeafJSON(data, value)
}

func (n *node) decodeNonNull(data []byte, value reflect.Value, depth int, open bool) error {
	if n.family {
		return n.decodeFamily(data, value, depth)
	}
	if n.concept != nil {
		return n.decodeConcept(data, value, open)
	}
	if n.scalar {
		return decodeLeafJSON(data, value)
	}
	return n.decodeKind(data, value, depth, open)
}

func (n *node) decodeConcept(data []byte, value reflect.Value, open bool) error {
	if err := concepts.CheckJSON(*n.concept, data); err != nil {
		return err
	}
	if open && n.schema["type"] == "integer" {
		if err := n.checkConceptInteger(data); err != nil {
			return err
		}
	}
	return decodeLeafJSON(data, value)
}

func (n *node) checkConceptInteger(data []byte) error {
	underlying := reflect.New(n.concept.Type)
	if err := json.Unmarshal(data, underlying.Interface()); err != nil {
		return err
	}
	return n.checkInteger(underlying.Elem(), true)
}

func (n *node) decodeKind(data []byte, value reflect.Value, depth int, open bool) error {
	switch value.Kind() {
	case reflect.Pointer:
		value.Set(reflect.New(value.Type().Elem()))
		return n.item.decodeContext(data, value.Elem(), depth+1, open)
	case reflect.Struct:
		return n.decodeStruct(data, value, depth, open)
	case reflect.Slice, reflect.Array:
		return n.decodeSequence(data, value, depth, open)
	case reflect.Map:
		return n.decodeMap(data, value, depth, open)
	default:
		return n.decodeScalar(data, value, open)
	}
}

func (n *node) decodeScalar(data []byte, value reflect.Value, open bool) error {
	if err := decodeLeafJSON(data, value); err != nil {
		return err
	}
	if open {
		return n.checkInteger(value, true)
	}
	return nil
}

type decodeProperties struct {
	values   map[string]json.RawMessage
	declared map[string]bool
}

func (n *node) decodeStruct(data []byte, value reflect.Value, depth int, open bool) error {
	properties := decodeProperties{declared: make(map[string]bool, len(n.fields))}
	if err := json.Unmarshal(data, &properties.values); err != nil {
		return err
	}
	for _, field := range n.fields {
		properties.declared[field.name] = true
	}
	for _, field := range n.fields {
		if err := field.decodeProperty(properties, value, depth, open); err != nil {
			return err
		}
	}
	return nil
}

func (p decodeProperties) lookup(name string) (json.RawMessage, bool) {
	if raw, ok := p.values[name]; ok {
		return raw, true
	}
	// Walk sorted keys for deterministic behavior if a payload supplies several
	// non-exact spellings. Ordinary producer payloads always use exact names.
	keys := make([]string, 0, len(p.values))
	for key := range p.values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if !p.declared[key] && strings.EqualFold(key, name) {
			return p.values[key], true
		}
	}
	return nil, false
}

func (f field) decodeProperty(properties decodeProperties, value reflect.Value, depth int, open bool) error {
	raw, ok := properties.lookup(f.name)
	if !ok {
		return f.decodeMissing(value)
	}
	target, err := fieldValue(value, f.index, true)
	if err != nil {
		return err
	}
	return f.value.decodeContext(raw, target, depth+1, open)
}

func (f field) decodeMissing(value reflect.Value) error {
	if !f.value.needsMissingEnum() {
		return nil
	}
	target, err := fieldValue(value, f.index, true)
	if err != nil {
		return err
	}
	return f.value.decodeMissingEnum(target)
}

func (n *node) needsMissingEnum() bool {
	if n.enum != nil {
		return true
	}
	if n.typ.Kind() != reflect.Slice || n.item == nil {
		return false
	}
	return n.item.enum != nil
}

func (n *node) decodeSequence(data []byte, value reflect.Value, depth int, open bool) error {
	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}
	if value.Kind() == reflect.Slice {
		value.Set(reflect.MakeSlice(value.Type(), len(items), len(items)))
	}
	for i := 0; i < len(items) && i < value.Len(); i++ {
		if err := n.decodeCollectionItem(items[i], value.Index(i), depth, open); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) decodeMap(data []byte, value reflect.Value, depth int, open bool) error {
	var items map[string]json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}
	guids, err := parseGuidDictionaryKeys(value.Type(), items)
	if err != nil {
		return err
	}
	value.Set(reflect.MakeMapWithSize(value.Type(), len(items)))
	for key, raw := range items {
		item, err := n.decodeMapItem(raw, value.Type().Elem(), depth, open)
		if err != nil {
			return err
		}
		value.SetMapIndex(dictionaryKey(value.Type().Key(), key, guids), item)
	}
	return nil
}

func (n *node) decodeMapItem(data []byte, typ reflect.Type, depth int, open bool) (reflect.Value, error) {
	if n.nullInterfaceItem(data) {
		return reflect.Value{}, faults.ErrProtocol
	}
	item := reflect.New(typ).Elem()
	err := n.item.decodeContext(data, item, depth+1, open)
	return item, err
}

func (n *node) nullInterfaceItem(data []byte) bool {
	return dereference(n.item.typ).Kind() == reflect.Interface && bytes.Equal(bytes.TrimSpace(data), []byte("null"))
}

func (n *node) decodeCollectionItem(data []byte, value reflect.Value, depth int, open bool) error {
	if n.nullInterfaceItem(data) {
		return faults.ErrProtocol
	}
	return n.item.decodeContext(data, value, depth+1, open)
}

// unescapedString rewrites a JSON string that holds escape sequences as plain
// characters. The kernel writes the + of a time offset as \u002B, which is valid
// JSON, but time.Time reads the raw bytes of a string without unescaping it
// before Go 1.27. Anything that is not a string is returned unchanged.
func unescapedString(data []byte) []byte {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) < 2 || trimmed[0] != '"' {
		return data
	}
	if !bytes.Contains(trimmed, []byte{'\\'}) {
		return data
	}
	var text string
	if err := json.Unmarshal(trimmed, &text); err != nil {
		return data
	}
	plain, err := json.Marshal(text)
	if err != nil {
		return data
	}
	return plain
}
