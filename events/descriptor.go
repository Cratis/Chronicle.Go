// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// Descriptor is immutable event schema and serialization metadata. Its zero value is invalid.
type Descriptor struct {
	typ           reflect.Type
	ref           TypeRef
	plan          *serialization.Plan
	tags          []Tag
	subject       func(any) (Subject, bool)
	sourceStore   string
	unique        []Unique
	removes       []string
	tombstone     bool
	compensation  *Descriptor
	schema        string
	historicalFor *Descriptor
	protection    []compliance.Declaration
}

// Ref returns the persisted identity and generation.
func (d Descriptor) Ref() TypeRef { return d.ref }

// GoType returns the normalized non-pointer Go struct type.
func (d Descriptor) GoType() reflect.Type { return d.typ }

// Schema returns the JSON Schema registered with the kernel.
func (d Descriptor) Schema() string {
	if d.schema != "" {
		return d.schema
	}
	return d.plan.Schema()
}

// Fields returns detached metadata from the shared serialization/schema plan.
func (d Descriptor) Fields() []serialization.Field { return d.plan.Fields() }

// SourceStore returns the declared origin store; empty means unspecified/local.
// It affects observer inference, not append routing or subscription provisioning.
func (d Descriptor) SourceStore() string { return d.sourceStore }

// Tags returns a copy of static event tags.
func (d Descriptor) Tags() []Tag { return append([]Tag(nil), d.tags...) }

// Marshal serializes a value or non-nil pointer of this descriptor's type.
func (d Descriptor) Marshal(value any) ([]byte, error) { return d.plan.Marshal(value) }

// Unmarshal decodes an object using this generation's configured serialization
// plan. target must be a non-nil pointer to the descriptor's Go type.
func (d Descriptor) Unmarshal(data []byte, target any) error { return d.plan.Unmarshal(data, target) }

// Type is a typed immutable descriptor returned by chronicle.RegisterEvent.
type Type[T any] struct{ descriptor Descriptor }

// Descriptor returns the immutable untyped descriptor.
func (t Type[T]) Descriptor() Descriptor { return t.descriptor }

// Ref returns the persisted type identity and generation.
func (t Type[T]) Ref() TypeRef { return t.descriptor.Ref() }

// TypeOption configures an event declaration. Scalar options are last-wins;
// nil options and invalid final values fail declaration. Tag inputs are copied.
type TypeOption func(*typeConfig)
type typeConfig struct {
	id           TypeID
	generation   Generation
	tags         []Tag
	sourceStore  string
	subjectType  reflect.Type
	subject      func(any) (Subject, bool)
	unique       []Unique
	removes      []string
	tombstone    bool
	compensation *Descriptor
	protection   []compliance.Declaration
}

// WithID overrides the default simple Go type name; use a stable ID across languages.
func WithID(id TypeID) TypeOption { return func(c *typeConfig) { c.id = id } }

// WithGeneration selects a positive schema generation (default one).
// Like C#, clients default to disabling generation validation, allowing a current
// generation above one without migrations. Enabling client generation validation
// requires a complete adjacent migration chain starting at generation one.
func WithGeneration(generation Generation) TypeOption {
	return func(c *typeConfig) { c.generation = generation }
}

// WithSourceStore declares the origin store for projection inbox inference.
// Empty resets to unspecified. This never reroutes appends or provisions an inbox.
func WithSourceStore(name string) TypeOption {
	return func(c *typeConfig) { c.sourceStore = name }
}

// WithTags sets static tags merged distinctly with append tags, in input order.
func WithTags(tags ...Tag) TypeOption {
	copy := append([]Tag(nil), tags...)
	return func(c *typeConfig) { c.tags = copy }
}

// Define builds an immutable event descriptor without registration or I/O. Most
// callers use chronicle.RegisterEvent instead. T must be a named non-pointer struct.
func Define[T any](options ...TypeOption) (Type[T], error) {
	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Struct || typ.Name() == "" {
		return Type[T]{}, fmt.Errorf("%w: event must be a named non-pointer struct", faults.ErrInvalidConfiguration)
	}
	config := typeConfig{id: TypeID(typ.Name()), generation: 1}
	for _, option := range options {
		if option == nil {
			return Type[T]{}, fmt.Errorf("%w: nil event option", faults.ErrInvalidConfiguration)
		}
		option(&config)
	}
	if config.sourceStore != "" && strings.TrimSpace(config.sourceStore) == "" {
		return Type[T]{}, fmt.Errorf("%w: source store must be nonblank", faults.ErrInvalidConfiguration)
	}
	if strings.TrimSpace(string(config.id)) == "" || strings.Contains(string(config.id), ",") || config.generation == 0 {
		return Type[T]{}, fmt.Errorf("%w: nonblank comma-free type ID and positive generation required", faults.ErrInvalidConfiguration)
	}
	if config.subjectType != nil && (config.subjectType != typ || config.subject == nil) {
		return Type[T]{}, fmt.Errorf("%w: subject resolver must be non-nil and match the declared event type", faults.ErrInvalidConfiguration)
	}
	plan, err := serialization.Compile(typ)
	if err != nil {
		return Type[T]{}, err
	}
	if err := plan.ValidateRole(declarations.Event); err != nil {
		return Type[T]{}, err
	}
	descriptor := Descriptor{typ: typ, ref: TypeRef{ID: config.id, Generation: config.generation}, plan: plan, tags: append([]Tag(nil), config.tags...), subject: config.subject, sourceStore: config.sourceStore,
		unique: config.unique, removes: config.removes, tombstone: config.tombstone, compensation: config.compensation}
	descriptor.protection = append([]compliance.Declaration(nil), config.protection...)
	descriptor, err = descriptor.withCompensationSchema()
	if err != nil {
		return Type[T]{}, err
	}
	return Type[T]{descriptor: descriptor}, nil
}

// Catalog is a frozen, concurrency-safe set of event descriptors. Use NewCatalog.
type Catalog struct {
	types      map[reflect.Type]Descriptor
	refs       map[TypeRef]Descriptor
	current    map[TypeID]Descriptor
	ordered    []Descriptor
	migrations []MigrationDefinition
}

// NewCatalog validates and copies descriptors, rejecting duplicate Go types,
// (ID, generation) pairs and current IDs. Historical descriptors must reference
// the current descriptor in this catalog; their generation must be lower.
func NewCatalog(descriptors ...Descriptor) (*Catalog, error) {
	c := &Catalog{types: make(map[reflect.Type]Descriptor), refs: make(map[TypeRef]Descriptor), current: make(map[TypeID]Descriptor), ordered: append([]Descriptor(nil), descriptors...)}
	for _, descriptor := range descriptors {
		if descriptor.typ == nil {
			return nil, fmt.Errorf("%w: empty event descriptor", faults.ErrInvalidConfiguration)
		}
		_, duplicateRef := c.refs[descriptor.ref]
		_, duplicateCurrent := c.current[descriptor.ref.ID]
		if _, exists := c.types[descriptor.typ]; exists || duplicateRef || (descriptor.historicalFor == nil && duplicateCurrent) {
			return nil, fmt.Errorf("%w: duplicate event type %s", faults.ErrInvalidConfiguration, descriptor.ref.ID)
		}
		c.types[descriptor.typ], c.refs[descriptor.ref] = descriptor, descriptor
		if descriptor.historicalFor == nil {
			c.current[descriptor.ref.ID] = descriptor
		}
	}
	for _, descriptor := range descriptors {
		if target := descriptor.historicalFor; target != nil {
			current, ok := c.current[descriptor.ref.ID]
			if !ok || current.ref != target.ref || current.typ != target.typ || descriptor.ref.Generation >= current.ref.Generation {
				return nil, fmt.Errorf("%w: historical generation requires its current event", faults.ErrInvalidConfiguration)
			}
		}
	}
	return c, nil
}

// Lookup resolves values and non-nil pointers to registered structs. Nil and nil
// pointers do not match. It performs no serialization or I/O.
func (c *Catalog) Lookup(value any) (Descriptor, bool) {
	if value == nil {
		return Descriptor{}, false
	}
	typ := reflect.TypeOf(value)
	if typ.Kind() == reflect.Pointer {
		if reflect.ValueOf(value).IsNil() {
			return Descriptor{}, false
		}
		typ = typ.Elem()
	}
	descriptor, found := c.types[typ]
	return descriptor, found
}

// Descriptors returns a defensive copy in registration order.
func (c *Catalog) Descriptors() []Descriptor { return append([]Descriptor(nil), c.ordered...) }
