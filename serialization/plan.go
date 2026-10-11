// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package serialization compiles one immutable field plan for JSON and JSON Schema.
// Primitive named values, structs, pointers, slices, arrays, string-keyed maps,
// time.Time, uuid.UUID, binary byte slices, Fundamentals scalars and concepts are supported. Integers nested under maps are restricted
// to -2^53 through 2^53 by the kernel's dictionary conversion; unsigned values
// above MaxInt64 are rejected by the pinned kernel's MongoDB append path. Unsupported
// custom/protected shapes fail closed.
package serialization

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/fundamentals.go/concepts"
)

// Plan is an immutable, concurrency-safe serializer/schema pair. Use Compile.
type Plan struct {
	root     *node
	schema   string
	typ      reflect.Type
	config   Config
	families []*node
	binary   bool
}
type node struct {
	typ            reflect.Type
	fields         []field
	item           *node
	schema         map[string]any
	scalar         bool
	binary         bool
	containsBinary bool
	concept        *concepts.Representation
	enum           *enumDefinition
	reference      *node
	readModelRoot  bool
	family         bool
	derivatives    []derivative
}
type field struct {
	index               []int
	name                string
	goName, tag         string
	value               *node
	omitEmpty, omitZero bool
	optionalAncestor    bool
	isZero              func(reflect.Value) bool
}

// Compile validates a struct shape before registration. Binary-free recursive
// types use schema references; recursive graphs containing binary are refused.
// Embedded fields follow encoding/json promotion and declaration order.
// Custom marshalers, interface values, unsupported chronicle directives and
// non-simple, reserved or case-insensitively duplicate property names in binary-
// containing graphs are rejected rather than generating a schema that disagrees
// with serialization or kernel property-path resolution.
// Recognized directives are metadata; artifact registries must also ValidateRole.
// Naming defaults to PreservePropertyNames; the last optional policy wins.
func Compile(typ reflect.Type, policies ...NamingPolicy) (*Plan, error) {
	return compilePlan(typ, false, policies...)
}

// CompileReadModel applies Compile's contract, translating only an untagged root
// Go ID field to C#'s Id before applying the naming policy. MongoDB read-model
// keys round-trip through the kernel only when the schema declares id or Id.
// Explicit json tags, nested fields and all other initialisms are unchanged.
// Binary is qualified only for emitted root leaves, including promoted embedded
// fields; objects containing binary below the root return ErrUnsupported.
func CompileReadModel(typ reflect.Type, policies ...NamingPolicy) (*Plan, error) {
	return compilePlan(typ, true, policies...)
}

func compilePlan(typ reflect.Type, readModel bool, policies ...NamingPolicy) (*Plan, error) {
	config := Config{}
	for _, candidate := range policies {
		config.NamingPolicy = candidate
	}
	return compileConfigured(typ, readModel, config)
}

// CompileWith compiles an event/document plan with explicit immutable codecs.
func CompileWith(typ reflect.Type, config Config) (*Plan, error) {
	return compileConfigured(typ, false, config)
}

// CompileReadModelWith applies CompileReadModel's root ID convention with codecs.
func CompileReadModelWith(typ reflect.Type, config Config) (*Plan, error) {
	return compileConfigured(typ, true, config)
}

// WithNamingPolicy recompiles names while retaining the frozen codec set.
func (p *Plan) WithNamingPolicy(policy NamingPolicy) (*Plan, error) {
	config := p.config
	config.NamingPolicy = policy
	return compileConfigured(p.typ, p.root.readModelRoot, config)
}

func compileConfigured(typ reflect.Type, readModel bool, config Config) (*Plan, error) {
	plan, err := buildConfigured(typ, readModel, config)
	var declaration *CodecError
	if errors.As(err, &declaration) {
		declaration.Role, declaration.Artifact = "document", typ
		if readModel {
			declaration.Role = "read model"
		}
	}
	return plan, err
}

func buildConfigured(typ reflect.Type, readModel bool, config Config) (*Plan, error) {
	if config.Codecs != nil {
		owned := *config.Codecs
		config.Codecs = &owned
	}
	policy := config.NamingPolicy
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: event must be a named struct", faults.ErrInvalidConfiguration)
	}
	state := &compileState{
		active: map[compileKey]*node{}, definitions: map[string]any{}, codecs: config.Codecs,
		deferDuplicates: binaryTypeCandidate(typ, policy, readModel, config.Codecs),
	}
	root, families, err := compileGraph(typ, readModel, config, state)
	if err != nil {
		return nil, err
	}
	data, err := finishSchema(typ, root, state)
	if err != nil {
		return nil, err
	}
	plan := &Plan{root: root, schema: data, typ: typ, config: config, families: families, binary: root.containsBinary}
	plan.schema, err = plan.ProtectedSchema()
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// compileGraph compiles the root and its derived-type families and validates the resulting graphs.
func compileGraph(typ reflect.Type, readModel bool, config Config, state *compileState) (*node, []*node, error) {
	policy := config.NamingPolicy
	root, err := compile(typ, state, policy, readModel)
	if err != nil {
		return nil, nil, err
	}
	if err = validateEnumPlacement(root); err != nil {
		return nil, nil, err
	}
	families, err := compileFamilies(config, state, policy)
	if err != nil {
		return nil, nil, err
	}
	if err = validateCandidates(typ, readModel, state, append([]*node{root}, families...)); err != nil {
		return nil, nil, err
	}
	return root, families, nil
}

// validateCandidates validates binary placement and graphs for the root and family nodes.
func validateCandidates(typ reflect.Type, readModel bool, state *compileState, candidates []*node) error {
	binary, err := analyzeBinary(candidates)
	if err != nil {
		return err
	}
	if state.duplicateNames != nil {
		return state.duplicateNames
	}
	if readModel {
		if err = validateReadModelBinary(candidates[0], typ); err != nil {
			return err
		}
	}
	if binary {
		return validateBinaryGraphs(candidates)
	}
	return nil
}

// finishSchema adds definitions and metadata to the root schema and returns it as JSON.
func finishSchema(typ reflect.Type, root *node, state *compileState) (string, error) {
	state.definitions = reachableDefinitions(root.schema, state.definitions)
	if len(state.definitions) > 0 {
		// Clone before adding definitions: a recursive root may itself be a target.
		root.schema = maps.Clone(root.schema)
		root.schema["definitions"] = state.definitions
	}
	root.schema["$schema"] = "http://json-schema.org/draft-07/schema#"
	root.schema["title"] = typ.Name()
	data, err := json.Marshal(root.schema)
	return string(data), err
}

// compileFamilies compiles each distinct derived-type family once, in registration order.
func compileFamilies(config Config, state *compileState, policy NamingPolicy) ([]*node, error) {
	var families []*node
	if config.Codecs == nil {
		return families, nil
	}
	seen := map[reflect.Type]bool{}
	for _, registration := range config.Codecs.ordered {
		if registration.kind != derivedCodec || seen[registration.family] {
			continue
		}
		seen[registration.family] = true
		family, err := compile(registration.family, state, policy, false)
		if err != nil {
			return nil, err
		}
		families = append(families, family)
	}
	return families, nil
}

// analyzeBinary computes capability metadata and validates binary placement for every candidate.
func analyzeBinary(candidates []*node) (bool, error) {
	binary := false
	for _, candidate := range candidates {
		// Compute capability metadata once, including recursive/reference edges.
		if err := visitNodes(candidate, map[*node]bool{}, func(n *node) error {
			n.containsBinary = hasBinary(n)
			return nil
		}); err != nil {
			return false, err
		}
		if err := validateBinaryPlacement(candidate); err != nil {
			return false, err
		}
		binary = binary || candidate.containsBinary
	}
	return binary, nil
}

func validateReadModelBinary(root *node, typ reflect.Type) error {
	for _, f := range root.fields {
		scalar, _ := classify(f.value)
		if f.value.containsBinary && scalar != Binary {
			return unsupported(typ, "read-model binary is only qualified at emitted root leaves")
		}
	}
	return nil
}

func validateBinaryGraphs(candidates []*node) error {
	for _, candidate := range candidates {
		if err := validateBinaryRecursion(candidate); err != nil {
			return err
		}
		if err := validateBinaryPropertyNames(candidate); err != nil {
			return err
		}
	}
	return nil
}

// Schema returns the immutable JSON Schema string with the same property names as Marshal.
func (p *Plan) Schema() string { return p.schema }

func unsupported(typ reflect.Type, reason string) error {
	return fmt.Errorf("%w: %s: %s", faults.ErrUnsupported, typ, reason)
}
