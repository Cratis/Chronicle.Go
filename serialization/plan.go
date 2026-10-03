// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package serialization compiles one immutable field plan for JSON and JSON Schema.
// Primitive named values, structs, pointers, slices, arrays, string-keyed maps,
// time.Time, uuid.UUID, Fundamentals scalars and concepts are supported. Integers nested under maps are restricted
// to -2^53 through 2^53 by the kernel's dictionary conversion; unsigned values
// above MaxInt64 are rejected by the pinned kernel's MongoDB append path. Unsupported
// custom/protected shapes fail closed.
package serialization

import (
	"crypto/sha256"
	"encoding"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/fundamentals.go/concepts"
	"github.com/google/uuid"
)

// Plan is an immutable, concurrency-safe serializer/schema pair. Use Compile.
type Plan struct {
	root   *node
	schema string
	typ    reflect.Type
}
type node struct {
	typ           reflect.Type
	fields        []field
	item          *node
	schema        map[string]any
	scalar        bool
	concept       *concepts.Representation
	reference     *node
	readModelRoot bool
}
type field struct {
	index               []int
	name                string
	goName, tag         string
	value               *node
	omitEmpty, omitZero bool
	isZero              func(reflect.Value) bool
}

// Compile validates a struct shape before registration. Recursive types use schema
// references. Embedded fields follow encoding/json promotion and declaration order.
// Custom marshalers, interface values and unsupported chronicle directives are
// rejected rather than generating a schema that disagrees with serialization.
// Recognized directives are metadata; artifact registries must also ValidateRole.
// Naming defaults to PreservePropertyNames; the last optional policy wins.
func Compile(typ reflect.Type, policies ...NamingPolicy) (*Plan, error) {
	return compilePlan(typ, false, policies...)
}

// CompileReadModel applies Compile's contract, translating only an untagged root
// Go ID field to C#'s Id before applying the naming policy. MongoDB read-model
// keys round-trip through the kernel only when the schema declares id or Id.
// Explicit json tags, nested fields and all other initialisms are unchanged.
func CompileReadModel(typ reflect.Type, policies ...NamingPolicy) (*Plan, error) {
	return compilePlan(typ, true, policies...)
}

func compilePlan(typ reflect.Type, readModel bool, policies ...NamingPolicy) (*Plan, error) {
	policy := PreservePropertyNames
	for _, candidate := range policies {
		policy = candidate
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: event must be a named struct", faults.ErrInvalidConfiguration)
	}
	state := &compileState{active: map[reflect.Type]*node{}, definitions: map[string]any{}}
	root, err := compile(typ, state, policy, readModel)
	if err != nil {
		return nil, err
	}
	if len(state.definitions) > 0 {
		// Clone before adding definitions: a recursive root may itself be a target.
		root.schema = maps.Clone(root.schema)
		root.schema["definitions"] = state.definitions
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

type compileState struct {
	active      map[reflect.Type]*node
	definitions map[string]any
}

func compile(typ reflect.Type, state *compileState, policy NamingPolicy, readModelRoot bool) (*node, error) {
	if previous := state.active[typ]; previous != nil && !previous.readModelRoot {
		if typ.Kind() == reflect.Struct {
			// Instantiated generic names can contain package paths and brackets.
			// Keep definition names safe for both JSON Pointers and URI fragments;
			// the hash still distinguishes types with the same sanitized name.
			name := fmt.Sprintf("%s_%x", schemaTypeName(typ.Name()), sha256.Sum256([]byte(typ.PkgPath()+"."+typ.String())))
			state.definitions[name] = previous.schema
			return &node{typ: typ, reference: previous, schema: map[string]any{"$ref": "#/definitions/" + name}}, nil
		}
		// A repeated collection/pointer wrapping an active struct must reach
		// that struct so the reference targets the object, not its container.
		element := typ.Elem()
		if element.Kind() == reflect.Pointer {
			element = element.Elem()
		}
		if element.Kind() != reflect.Struct || state.active[element] == nil {
			return nil, unsupported(typ, "recursive non-object shape")
		}
	}
	n := &node{typ: typ, schema: make(map[string]any), readModelRoot: readModelRoot}
	previous := state.active[typ]
	state.active[typ] = n
	defer func() {
		if previous == nil {
			delete(state.active, typ)
		} else {
			state.active[typ] = previous
		}
	}()
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
	// Traverse pointers before testing marshaler interfaces: pointers to the
	// supported built-ins inherit their marshaling methods too.
	if typ.Kind() == reflect.Pointer {
		item, err := compile(typ.Elem(), state, policy, false)
		if err != nil {
			return nil, err
		}
		n.item, n.schema = item, maps.Clone(item.schema)
		if format, ok := n.schema["format"].(string); ok {
			n.schema["format"] = strings.TrimSuffix(format, "?") + "?"
		} else if kind, ok := n.schema["type"].(string); ok {
			n.schema["type"] = []string{kind, "null"}
		}
		return n, nil
	}
	if representation, ok, err := concepts.Underlying(typ); err != nil {
		return nil, fmt.Errorf("%w: %w", faults.ErrInvalidConfiguration, err)
	} else if ok {
		return compileConcept(n, representation, state, policy)
	}
	for _, contract := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
		if typ.Implements(contract) || reflect.PointerTo(typ).Implements(contract) {
			return nil, unsupported(typ, "custom marshalers need an explicit schema codec (not yet supported)")
		}
	}
	switch typ.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		if typ.Kind() == reflect.Map {
			if _, _, err := concepts.Underlying(typ.Key()); err != nil {
				return nil, fmt.Errorf("%w: %w", faults.ErrInvalidConfiguration, err)
			}
			if typ.Key().Kind() != reflect.String || typ.Key().Implements(reflect.TypeFor[encoding.TextMarshaler]()) {
				return nil, unsupported(typ, "map key must be a string without a custom codec")
			}
		}
		if typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Uint8 {
			return nil, unsupported(typ, "byte slices need an explicit wire format")
		}
		item, err := compile(typ.Elem(), state, policy, false)
		if err != nil {
			return nil, err
		}
		n.item = item
		switch typ.Kind() {
		case reflect.Map:
			n.schema["type"], n.schema["additionalProperties"] = "object", item.schema
		default:
			n.schema["type"], n.schema["items"] = "array", item.schema
		}
	case reflect.Struct:
		if err := n.compileFields(state, policy, readModelRoot); err != nil {
			return nil, err
		}
	case reflect.Bool:
		n.schema["type"] = "boolean"
	case reflect.String:
		n.schema["type"] = "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n.schema["type"], n.schema["format"] = "integer", integerFormat(typ)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n.schema["type"], n.schema["format"], n.schema["minimum"] = "integer", integerFormat(typ), 0
		if typ.Kind() == reflect.Uint || typ.Kind() == reflect.Uint64 {
			n.schema["maximum"] = uint64(1<<63 - 1)
		}
	case reflect.Float32:
		n.schema["type"], n.schema["format"] = "number", "float"
	case reflect.Float64:
		n.schema["type"], n.schema["format"] = "number", "double"
	default:
		return nil, unsupported(typ, "unsupported JSON shape")
	}
	return n, nil
}

func schemaTypeName(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, name)
}

func (n *node) compileFields(state *compileState, policy NamingPolicy, readModelRoot bool) error {
	properties := make(map[string]any)
	required := []string{}
	fields, err := serializedFields(n.typ, policy, readModelRoot)
	if err != nil {
		return err
	}
	for _, candidate := range fields {
		f, name := candidate.field, candidate.name
		value, err := compile(f.Type, state, policy, false)
		if err != nil {
			return err
		}
		entry := field{index: candidate.index, name: name, goName: candidate.goName, tag: f.Tag.Get("chronicle"), value: value}
		for _, option := range strings.Split(f.Tag.Get("json"), ",")[1:] {
			switch option {
			case "omitempty":
				entry.omitEmpty = true
			case "omitzero":
				entry.omitZero, entry.isZero = true, zeroFunc(f.Type)
			default:
				return unsupported(n.typ, "unsupported JSON tag option")
			}
		}
		properties[name] = value.schema
		if !candidate.optional && !entry.omitEmpty && !entry.omitZero && f.Type.Kind() != reflect.Pointer && f.Type.Kind() != reflect.Map && f.Type.Kind() != reflect.Slice {
			required = append(required, name)
		}
		n.fields = append(n.fields, entry)
	}
	n.schema["type"], n.schema["properties"], n.schema["required"] = "object", properties, required
	return nil
}

// Chronicle has no int8 or uint16 format. Widen those to the nearest supported
// CLR format. int/uint use stable 64-bit schemas on every Go architecture.
func integerFormat(typ reflect.Type) string {
	switch typ.Kind() {
	case reflect.Int8, reflect.Int16:
		return "int16"
	case reflect.Int32:
		return "int32"
	case reflect.Uint8:
		return "byte"
	case reflect.Uint16, reflect.Uint32:
		return "uint32"
	case reflect.Uint, reflect.Uint64:
		return "uint64"
	default:
		return "int64"
	}
}

func unsupported(typ reflect.Type, reason string) error {
	return fmt.Errorf("%w: %s: %s", faults.ErrUnsupported, typ, reason)
}
