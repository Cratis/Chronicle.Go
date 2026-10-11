// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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

type compileKey struct {
	typ           reflect.Type
	derived, root bool
}
type compileState struct {
	active          map[compileKey]*node
	definitions     map[string]any
	codecs          *Codecs
	duplicateNames  error
	deferDuplicates bool
}

// enter marks the key as being compiled and returns the function that restores the previous state.
func (s *compileState) enter(key compileKey, n *node) func() {
	previous := s.active[key]
	s.active[key] = n
	return func() {
		if previous == nil {
			delete(s.active, key)
		} else {
			s.active[key] = previous
		}
	}
}

// duplicateHandler records the first duplicate name when duplicates are deferred, otherwise it is nil.
func (s *compileState) duplicateHandler() func(error) {
	if !s.deferDuplicates {
		return nil
	}
	return func(err error) {
		if s.duplicateNames == nil {
			s.duplicateNames = err
		}
	}
}

func compile(typ reflect.Type, state *compileState, policy NamingPolicy, readModelRoot bool) (*node, error) {
	return compileContext(typ, state, policy, readModelRoot, false)
}

func compileContext(typ reflect.Type, state *compileState, policy NamingPolicy, readModelRoot, derived bool) (*node, error) {
	key := compileKey{typ, derived, readModelRoot}
	if previous := state.active[key]; previous != nil {
		if reference, err := compileActiveReference(previous, typ, state, key); reference != nil || err != nil {
			return reference, err
		}
	}
	n := &node{typ: typ, schema: make(map[string]any), readModelRoot: readModelRoot}
	defer state.enter(key, n)()
	if err := n.compileType(state, policy, readModelRoot, derived); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *node) compileType(state *compileState, policy NamingPolicy, readModelRoot, derived bool) error {
	typ := n.typ
	if compileKnownScalar(n, typ) {
		return nil
	}
	// Traverse pointers before testing marshaler interfaces: pointers to the
	// supported built-ins inherit their marshaling methods too.
	if typ.Kind() == reflect.Pointer {
		return n.compilePointer(state, policy, derived)
	}
	if definition := state.codecs.enumeration(typ); definition != nil {
		n.enum, n.schema = definition, definition.schema()
		return nil
	}
	if handled, err := n.compileConceptType(state, policy); handled || err != nil {
		return err
	}
	if hasCustomMarshaler(typ) {
		return unsupported(typ, "custom marshalers need an explicit schema codec (not yet supported)")
	}
	return n.compileByKind(state, policy, readModelRoot, derived)
}

func (n *node) compileConceptType(state *compileState, policy NamingPolicy) (bool, error) {
	representation, ok, err := concepts.Underlying(n.typ)
	if err != nil {
		return false, fmt.Errorf("%w: %w", faults.ErrInvalidConfiguration, err)
	}
	if !ok {
		return false, nil
	}
	_, err = compileConcept(n, representation, state, policy)
	return true, err
}

func hasCustomMarshaler(typ reflect.Type) bool {
	for _, contract := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
		if typ.Implements(contract) || reflect.PointerTo(typ).Implements(contract) {
			return true
		}
	}
	return false
}

// compileActiveReference handles a type that is already being compiled. It
// returns a reference node, an error, or neither when compilation must continue.
func compileActiveReference(previous *node, typ reflect.Type, state *compileState, key compileKey) (*node, error) {
	switch typ.Kind() {
	case reflect.Interface:
		return &node{typ: typ, reference: previous, schema: map[string]any{"type": "object"}}, nil
	case reflect.Struct:
		return structReference(previous, typ, state, key), nil
	}
	return nil, checkActiveContainer(typ, state, key.derived)
}

// structReference registers the active struct as a definition and returns a node that refers to it.
func structReference(previous *node, typ reflect.Type, state *compileState, key compileKey) *node {
	name := definitionName(typ, key)
	state.definitions[name] = previous.schema
	return &node{typ: typ, reference: previous, schema: map[string]any{"$ref": "#/definitions/" + name}}
}

// definitionName keeps definition names safe for both JSON Pointers and URI fragments.
// Instantiated generic names can contain package paths and brackets;
// the hash still distinguishes types with the same sanitized name.
func definitionName(typ reflect.Type, key compileKey) string {
	identity := typ.PkgPath() + "." + typ.String()
	if key.derived {
		identity += ":derived"
	}
	if key.root {
		identity += ":root"
	}
	return fmt.Sprintf("%s_%x", schemaTypeName(typ.Name()), sha256.Sum256([]byte(identity)))
}

func isObjectKind(kind reflect.Kind) bool {
	return kind == reflect.Struct || kind == reflect.Interface
}

// checkActiveContainer ensures a repeated collection/pointer wrapping an active struct
// can reach that struct so the reference targets the object, not its container.
func checkActiveContainer(typ reflect.Type, state *compileState, derived bool) error {
	element := typ.Elem()
	if element.Kind() == reflect.Pointer {
		element = element.Elem()
	}
	if !isObjectKind(element.Kind()) || state.active[compileKey{element, derived, false}] == nil {
		return unsupported(typ, "recursive non-object shape")
	}
	return nil
}

// compileKnownScalar fills the schema of the built-in time and UUID types.
func compileKnownScalar(n *node, typ reflect.Type) bool {
	switch typ {
	case reflect.TypeFor[time.Time]():
		n.scalar = true
		n.schema["type"], n.schema["format"] = "string", "date-time"
	case reflect.TypeFor[uuid.UUID]():
		n.scalar = true
		n.schema["type"], n.schema["format"] = "string", "uuid"
	default:
		return false
	}
	return true
}

func (n *node) compilePointer(state *compileState, policy NamingPolicy, derived bool) error {
	item, err := compileContext(n.typ.Elem(), state, policy, false, derived)
	if err != nil {
		return err
	}
	n.item, n.schema = item, maps.Clone(item.schema)
	if format, ok := n.schema["format"].(string); ok {
		n.schema["format"] = strings.TrimSuffix(format, "?") + "?"
		if item.binary {
			n.schema["type"] = []string{"string", "null"}
		}
	} else if kind, ok := n.schema["type"].(string); ok {
		n.schema["type"] = []string{kind, "null"}
	}
	return nil
}

func (n *node) compileByKind(state *compileState, policy NamingPolicy, readModelRoot, derived bool) error {
	switch n.typ.Kind() {
	case reflect.Interface:
		return n.compileFamily(state, policy)
	case reflect.Slice, reflect.Array, reflect.Map:
		return n.compileCollection(state, policy)
	case reflect.Struct:
		return n.compileFields(state, policy, readModelRoot, derived)
	default:
		return n.compilePrimitive()
	}
}

func checkMapKey(typ reflect.Type) error {
	if _, _, err := concepts.Underlying(typ.Key()); err != nil {
		return fmt.Errorf("%w: %w", faults.ErrInvalidConfiguration, err)
	}
	if typ.Key().Kind() != reflect.String || typ.Key().Implements(reflect.TypeFor[encoding.TextMarshaler]()) {
		return unsupported(typ, "map key must be a string without a custom codec")
	}
	return nil
}

func (n *node) compileBinary() error {
	if n.typ.Elem() != reflect.TypeFor[byte]() {
		return unsupported(n.typ, "binary requires the built-in byte element type")
	}
	n.binary, n.scalar = true, true
	n.schema["type"], n.schema["format"] = "string", "byte-array"
	return nil
}

func (n *node) compileCollection(state *compileState, policy NamingPolicy) error {
	typ := n.typ
	if typ.Kind() == reflect.Map {
		if err := checkMapKey(typ); err != nil {
			return err
		}
	}
	if typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Uint8 {
		return n.compileBinary()
	}
	item, err := compile(typ.Elem(), state, policy, false)
	if err != nil {
		return err
	}
	n.item = item
	if typ.Kind() == reflect.Map {
		n.schema["type"], n.schema["additionalProperties"] = "object", item.schema
	} else {
		n.schema["type"], n.schema["items"] = "array", item.schema
	}
	return nil
}

type primitiveSchema struct{ typ, format string }

var primitiveSchemas = map[reflect.Kind]primitiveSchema{
	reflect.Bool:    {"boolean", ""},
	reflect.String:  {"string", ""},
	reflect.Float32: {"number", "float"},
	reflect.Float64: {"number", "double"},
}

func (n *node) compilePrimitive() error {
	typ := n.typ
	if schema, ok := primitiveSchemas[typ.Kind()]; ok {
		n.schema["type"] = schema.typ
		if schema.format != "" {
			n.schema["format"] = schema.format
		}
		return nil
	}
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n.schema["type"], n.schema["format"] = "integer", integerFormat(typ)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n.compileUnsigned()
	default:
		return unsupported(typ, "unsupported JSON shape")
	}
	return nil
}

func (n *node) compileUnsigned() {
	typ := n.typ
	n.schema["type"], n.schema["format"], n.schema["minimum"] = "integer", integerFormat(typ), 0
	if typ.Kind() == reflect.Uint || typ.Kind() == reflect.Uint64 {
		n.schema["maximum"] = uint64(1<<63 - 1)
	}
}

func isSchemaNameRune(r rune) bool {
	return r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_'
}

func schemaTypeName(name string) string {
	return strings.Map(func(r rune) rune {
		if isSchemaNameRune(r) {
			return r
		}
		return '_'
	}, name)
}

func fieldPolicyFor(policy NamingPolicy, derived bool) NamingPolicy {
	if derived {
		return CamelCase
	}
	return policy
}

func (n *node) compileFields(state *compileState, policy NamingPolicy, readModelRoot, derived bool) error {
	properties := make(map[string]any)
	required := []string{}
	fields, err := serializedFieldsWithDuplicateHandler(n.typ, fieldPolicyFor(policy, derived), readModelRoot, state.duplicateHandler())
	if err != nil {
		return err
	}
	for _, candidate := range fields {
		entry, err := n.compileField(candidate, state, policy)
		if err != nil {
			return err
		}
		properties[entry.name] = entry.value.schema
		if isRequiredField(candidate, entry) {
			required = append(required, entry.name)
		}
		n.fields = append(n.fields, entry)
	}
	n.schema["type"], n.schema["properties"], n.schema["required"] = "object", properties, required
	return nil
}

func (n *node) compileField(candidate fieldCandidate, state *compileState, policy NamingPolicy) (field, error) {
	f := candidate.field
	value, err := compile(f.Type, state, policy, false)
	if err != nil {
		return field{}, err
	}
	entry := field{index: candidate.index, name: candidate.name, goName: candidate.goName, tag: f.Tag.Get("chronicle"), value: value, optionalAncestor: candidate.optional}
	return entry, applyJSONOptions(&entry, f, n.typ)
}

// applyJSONOptions applies the omitempty and omitzero options of the json tag.
func applyJSONOptions(entry *field, f reflect.StructField, owner reflect.Type) error {
	for _, option := range strings.Split(f.Tag.Get("json"), ",")[1:] {
		switch option {
		case "omitempty":
			entry.omitEmpty = true
		case "omitzero":
			entry.omitZero, entry.isZero = true, zeroFunc(f.Type)
		default:
			return unsupported(owner, "unsupported JSON tag option")
		}
	}
	return nil
}

func isNullableKind(kind reflect.Kind) bool {
	return kind == reflect.Pointer || kind == reflect.Map || kind == reflect.Slice || kind == reflect.Interface
}

func isRequiredField(candidate fieldCandidate, entry field) bool {
	if hasEnum(entry.value) {
		return true
	}
	optional := candidate.optional || entry.omitEmpty || entry.omitZero
	return !optional && !isNullableKind(candidate.field.Type.Kind())
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
