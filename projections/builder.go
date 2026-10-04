// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"encoding/json"
	"reflect"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

// Field describes a serialized path and its declared value type. Validation never
// invokes callbacks on fabricated model/event values. The zero value is invalid.
type Field[T, V any] struct {
	path  string
	owner reflect.Type
}

// Path creates a typed field descriptor; Build/Compile validates exact spelling
// and V against the serialization plan. Go rename does not rewrite path strings.
func Path[T, V any](path string) Field[T, V] {
	return Field[T, V]{path: path, owner: reflect.TypeFor[T]()}
}

// Builder authors one fluent projection. It is not safe for concurrent mutation.
// Build returns an immutable snapshot; later builder changes cannot affect it.
type Builder[M any] struct{ data *declaration }

// NewBuilder creates a fluent builder. Empty id uses the model's observer identity
// when set, otherwise the full model type name.
// Model mapping tags cannot be mixed with fluent writes; use ModelBound instead.
func NewBuilder[M any](id string, model readmodels.Model[M], options ...Option) *Builder[M] {
	d := newDeclaration(model.Descriptor(), options)
	if id != "" {
		d.id = id
	}
	return &Builder[M]{data: d}
}

// FromBuilder configures mappings for one event. Retaining it after From returns
// is harmless: From snapshots its mappings before returning.
type FromBuilder[M, E any] struct{ subscription subscription }

// From subscribes and invokes define exactly once. Nil define means AutoMap only.
// Generic operations are package functions because Go has no generic methods.
func From[M, E any](builder *Builder[M], event events.Type[E], define func(*FromBuilder[M, E]), options ...FromOption) {
	from := &FromBuilder[M, E]{subscription: newSubscription(event.Descriptor(), options)}
	if define != nil {
		define(from)
	}
	from.subscription.writes = append([]write(nil), from.subscription.writes...)
	builder.data.subscriptions = append(builder.data.subscriptions, from.subscription)
}

// Map assigns an event field to a model field of the same declared Go type.
func Map[M, E, V any](builder *FromBuilder[M, E], target Field[M, V], source Field[E, V]) {
	builder.add(target.path, reflect.TypeFor[V](), reflect.TypeFor[V](), expression{kind: pathExpression, text: source.path}, "set")
}

// MapAs assigns an event field to a model field with a different declared Go type.
// Build/Compile validates both field types and their serialized representations
// using the same compatibility check as model-bound set (for example string to
// *string). It does not perform arbitrary conversions or invoke user code.
func MapAs[M, E, T, S any](from *FromBuilder[M, E], target Field[M, T], source Field[E, S]) {
	from.add(target.path, reflect.TypeFor[T](), reflect.TypeFor[S](), expression{kind: pathExpression, text: source.path}, "set")
}

// Context assigns a kernel EventContext property (for example occurred). Context
// paths use the kernel's contract names, not the Go events.Context field names.
func Context[M, E, V any](builder *FromBuilder[M, E], target Field[M, V], path string) {
	builder.add(target.path, reflect.TypeFor[V](), nil, expression{kind: contextExpression, text: path}, "context")
}

// EventSourceID assigns the event-source identity to a string or UUID-backed field.
func EventSourceID[M, E, V any](builder *FromBuilder[M, E], target Field[M, V]) {
	builder.add(target.path, reflect.TypeFor[V](), nil, expression{kind: sourceExpression}, "source")
}

// Value snapshots a typed scalar literal. Nil nullable scalar pointers and direct
// compiled slice/string-keyed map pointers emit $null. Collection pointers preserve
// typed nil versus empty in the C# nullable collection profile without initializers;
// raw materialized reads may omit the cleared property. Bare slices/maps, fixed
// arrays, interfaces, structural objects and collection-element paths are excluded.
// Unsupported values and kernel-unrepresentable literals fail Build/Compile.
func Value[M, E, V any](builder *FromBuilder[M, E], target Field[M, V], value V) {
	expression := expression{kind: invalidExpression}
	data, err := json.Marshal(value)
	if err == nil {
		// Reuse the versioned JSON scalar parser, not a separate quoting grammar.
		parsed, parseErr := declarations.Parse(declarations.V1, "value(E,value="+string(data)+")")
		if parseErr == nil && len(parsed) == 1 && len(parsed[0].Args) == 2 {
			literal := parsed[0].Args[1].Value
			if literal.Kind != declarations.Call && literal.Kind != declarations.Name {
				expression = literalValue(literal)
			}
		}
	}
	builder.add(target.path, reflect.TypeFor[V](), nil, expression, "value")
}
func (b *FromBuilder[M, E]) add(path string, target, source reflect.Type, expression expression, directive string) {
	b.subscription.writes = append(b.subscription.writes, write{path: path, targetType: target, sourceType: source, expression: expression, provenance: Provenance{Path: path, Directive: directive, Offset: -1, FrontEnd: "fluent"}})
}

// Build validates locally available metadata and snapshots the declaration. Event
// membership is validated again against the actual frozen store catalog at NewClient.
func (b *Builder[M]) Build() (Declaration, error) {
	if b == nil || b.data == nil {
		return Declaration{}, invalid("builder required")
	}
	declaration := Declaration{data: cloneDeclaration(b.data)}
	var descriptors []events.Descriptor
	seen := map[reflect.Type]bool{}
	for _, s := range declarationSubscriptions(declaration.data) {
		if s.event.GoType() == nil {
			return Declaration{}, invalid("event handle required")
		}
		if !seen[s.event.GoType()] {
			descriptors = append(descriptors, s.event)
			seen[s.event.GoType()] = true
		}
	}
	catalog, err := events.NewCatalog(descriptors...)
	if err != nil {
		return Declaration{}, err
	}
	if declaration.IsGlobal() {
		_, err = compileOrdinary(declaration, catalog)
	} else {
		_, err = Compile(declaration, catalog)
	}
	if err != nil {
		return Declaration{}, err
	}
	return declaration, nil
}
