// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package serialization compiles one immutable field plan for JSON and JSON Schema.
// Primitive named values, structs, pointers, slices, arrays, string-keyed maps,
// time.Time and uuid.UUID are supported. Unsupported custom/protected shapes fail closed.
package serialization

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/google/uuid"
)

// Plan is an immutable, concurrency-safe serializer/schema pair. Use Compile.
type Plan struct {
	root   *node
	schema string
	typ    reflect.Type
}
type node struct {
	typ    reflect.Type
	fields []field
	item   *node
	schema map[string]any
	scalar bool
}
type field struct {
	index               int
	name                string
	value               *node
	omitEmpty, omitZero bool
}

// Compile validates a struct shape before registration. Embedded fields, recursive
// types, custom marshalers, interface values and chronicle field tags are rejected
// rather than generating a schema that disagrees with serialization.
func Compile(typ reflect.Type) (*Plan, error) {
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: event must be a named struct", faults.ErrInvalidConfiguration)
	}
	root, err := compile(typ, make(map[reflect.Type]bool))
	if err != nil {
		return nil, err
	}
	root.schema["$schema"] = "http://json-schema.org/draft-07/schema#"
	root.schema["title"] = typ.Name()
	data, err := json.Marshal(root.schema)
	if err != nil {
		return nil, err
	}
	return &Plan{root: root, schema: string(data), typ: typ}, nil
}

// Schema returns the immutable JSON Schema string with the same property names as Marshal.
func (p *Plan) Schema() string { return p.schema }

func compile(typ reflect.Type, active map[reflect.Type]bool) (*node, error) {
	if active[typ] {
		return nil, unsupported(typ, "recursive shape")
	}
	active[typ] = true
	defer delete(active, typ)
	n := &node{typ: typ, schema: make(map[string]any)}
	if typ == reflect.TypeFor[time.Time]() {
		n.scalar = true
		n.schema["type"], n.schema["format"] = "string", "date-time"
		return n, nil
	}
	if typ == reflect.TypeFor[uuid.UUID]() {
		n.scalar = true
		n.schema["type"], n.schema["format"] = "string", "uuid"
		return n, nil
	}
	for _, contract := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
		if typ.Implements(contract) || reflect.PointerTo(typ).Implements(contract) {
			return nil, unsupported(typ, "custom marshalers need an explicit schema codec (not yet supported)")
		}
	}
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		if typ.Kind() == reflect.Map && typ.Key().Kind() != reflect.String {
			return nil, unsupported(typ, "map key must be a string")
		}
		if typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Uint8 {
			return nil, unsupported(typ, "byte slices need an explicit wire format")
		}
		item, err := compile(typ.Elem(), active)
		if err != nil {
			return nil, err
		}
		n.item = item
		switch typ.Kind() {
		case reflect.Pointer:
			n.schema = item.schema
		case reflect.Map:
			n.schema["type"], n.schema["additionalProperties"] = "object", item.schema
		default:
			n.schema["type"], n.schema["items"] = "array", item.schema
		}
	case reflect.Struct:
		if err := n.compileFields(active); err != nil {
			return nil, err
		}
	case reflect.Bool:
		n.schema["type"] = "boolean"
	case reflect.String:
		n.schema["type"] = "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n.schema["type"] = "integer"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n.schema["type"], n.schema["minimum"] = "integer", 0
	case reflect.Float32, reflect.Float64:
		n.schema["type"] = "number"
	default:
		return nil, unsupported(typ, "unsupported JSON shape")
	}
	return n, nil
}

func (n *node) compileFields(active map[reflect.Type]bool) error {
	properties := make(map[string]any)
	required := []string{}
	for i := 0; i < n.typ.NumField(); i++ {
		f := n.typ.Field(i)
		if !f.IsExported() {
			continue
		}
		tags := strings.Split(f.Tag.Get("json"), ",")
		if tags[0] == "-" {
			continue
		}
		if f.Anonymous {
			return unsupported(n.typ, "embedded fields require an explicit nested property")
		}
		if f.Tag.Get("chronicle") != "" {
			return unsupported(n.typ, "classification/routing field tags are not yet supported")
		}
		name := tags[0]
		if name == "" {
			name = camelCase(f.Name)
		}
		if _, exists := properties[name]; exists {
			return unsupported(n.typ, "duplicate JSON property: "+name)
		}
		value, err := compile(f.Type, active)
		if err != nil {
			return err
		}
		entry := field{index: i, name: name, value: value}
		for _, option := range tags[1:] {
			switch option {
			case "omitempty":
				entry.omitEmpty = true
			case "omitzero":
				entry.omitZero = true
			default:
				return unsupported(n.typ, "unsupported JSON tag option")
			}
		}
		properties[name] = value.schema
		if !entry.omitEmpty && !entry.omitZero && f.Type.Kind() != reflect.Pointer && f.Type.Kind() != reflect.Map && f.Type.Kind() != reflect.Slice {
			required = append(required, name)
		}
		n.fields = append(n.fields, entry)
	}
	n.schema["type"], n.schema["properties"], n.schema["required"] = "object", properties, required
	return nil
}

func unsupported(typ reflect.Type, reason string) error {
	return fmt.Errorf("%w: %s: %s", faults.ErrUnsupported, typ, reason)
}

// camelCase matches System.Text.Json's acronym boundary behavior (URLValue -> urlValue).
func camelCase(value string) string {
	runes := []rune(value)
	for i := range runes {
		if !unicode.IsUpper(runes[i]) {
			break
		}
		if i > 0 && i+1 < len(runes) && !unicode.IsUpper(runes[i+1]) {
			break
		}
		runes[i] = unicode.ToLower(runes[i])
	}
	return string(runes)
}
