// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"encoding/json"
	"reflect"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/serialization"
)

// UsingKeyFromContext correlates an event using a scalar kernel context property.
func UsingKeyFromContext(path string) FromOption {
	return func(s *subscription) {
		s.key = expression{kind: contextExpression, text: path}
		s.keyType = nil
		s.keySet = true
	}
}

// UsingParentKeyFromContext selects the parent using a scalar context property.
func UsingParentKeyFromContext(path string) FromOption {
	return func(s *subscription) {
		s.parent = expression{kind: contextExpression, text: path}
		s.parentType = nil
		s.parentSet = true
	}
}

// CompositeKeyBuilder builds ordered, named key parts for an event. It is only
// valid within UsingCompositeKey's callback; that function snapshots its result.
// Part names are serialized names on K, not arbitrary kernel expression strings.
type CompositeKeyBuilder[K, E any] struct {
	parts []keyPart
	err   error
}

// KeyPart assigns an event field to one composite-key field of the same Go type.
func KeyPart[K, E, V any](b *CompositeKeyBuilder[K, E], target Field[K, V], source Field[E, V]) {
	b.add(target.path, reflect.TypeFor[V](), keyPart{name: target.path, expression: expression{kind: pathExpression, text: source.path}, sourceType: reflect.TypeFor[V]()})
}

// KeyPartFromContext assigns a scalar context property to a composite-key field.
func KeyPartFromContext[K, E, V any](b *CompositeKeyBuilder[K, E], target Field[K, V], path string) {
	b.add(target.path, reflect.TypeFor[V](), keyPart{name: target.path, expression: expression{kind: contextExpression, text: path}})
}

// KeyPartFromSource assigns the event-source identity to a string/UUID key part.
func KeyPartFromSource[K, E, V any](b *CompositeKeyBuilder[K, E], target Field[K, V]) {
	b.add(target.path, reflect.TypeFor[V](), keyPart{name: target.path, expression: expression{kind: sourceExpression}})
}

// KeyPartValue snapshots a scalar constant as one named composite-key part.
func KeyPartValue[K, E, V any](b *CompositeKeyBuilder[K, E], target Field[K, V], value V) {
	e := expression{kind: invalidExpression}
	data, err := json.Marshal(value)
	if err == nil {
		parsed, parseErr := declarations.Parse(declarations.V1, "value(E,value="+string(data)+")")
		if parseErr == nil && len(parsed) == 1 && len(parsed[0].Args) == 2 {
			e = literalValue(parsed[0].Args[1].Value)
		}
	}
	b.add(target.path, reflect.TypeFor[V](), keyPart{name: target.path, expression: e})
}

// UsingCompositeParentKey configures an ordered composite parent key.
func UsingCompositeParentKey[K, E any](define func(*CompositeKeyBuilder[K, E])) FromOption {
	option := UsingCompositeKey(define)
	return func(s *subscription) {
		parent := newSubscription(s.event, []FromOption{option})
		if parent.err != nil {
			s.err = parent.err
		}
		s.parent, s.parentType = parent.key, nil
		s.parentSet = true
	}
}

func (b *CompositeKeyBuilder[K, E]) add(path string, typ reflect.Type, part keyPart) {
	plan, err := serialization.Compile(reflect.TypeFor[K]())
	if err != nil {
		b.err = err
		return
	}
	if err := plan.ValidateRole(declarations.Model); err != nil {
		b.err = err
		return
	}
	fields := plan.Fields()
	for _, field := range fields {
		directives, err := declarations.Parse(declarations.V1, field.Tag)
		if err != nil {
			b.err = err
			return
		}
		for _, directive := range directives {
			if directive.Name == "index" || directive.Name == "subject" || directive.Name == "pii" || directive.Name == "encrypted" || directive.Name == "compliance-details" {
				b.err = &DeclarationError{Artifact: reflect.TypeFor[K]().String(), GoField: field.GoField, Path: field.Path, Directive: directive.Name, Offset: directive.Offset, Message: "unsupported directive on a composite key", Cause: invalid("unsupported composite key directive")}
				return
			}
		}
	}
	field, ok := binaryMappingField(fields, path)
	if ok && binaryField(field) {
		b.err = binaryUnsupported("binary correlation keys are not supported")
		return
	}
	if !ok || validateTarget(field, typ) != nil || !field.Scalar.IsPrimitive() || field.Nullable {
		b.err = invalid("composite part requires a non-nullable scalar key field")
		return
	}
	if part.expression.kind != pathExpression {
		if err := validateExpression(part.expression, field, plan.Fields(), nil, nil); err != nil {
			b.err = err
		}
	}
	b.parts = append(b.parts, part)
}

// UsingCompositeKey correlates by named parts in callback order. Duplicate parts,
// missing paths and kernel-unrepresentable expressions fail Build or NewClient.
// K uses serialization's default spelling (explicit json tags are recommended).
func UsingCompositeKey[K, E any](define func(*CompositeKeyBuilder[K, E])) FromOption {
	b := &CompositeKeyBuilder[K, E]{}
	if define == nil {
		b.err = invalid("composite key callback required")
	} else {
		define(b)
	}
	parts := append([]keyPart(nil), b.parts...)
	err := b.err
	return func(s *subscription) {
		if err != nil {
			s.err = err
		}
		if s.event.GoType() != reflect.TypeFor[E]() {
			s.err = invalid("composite event type mismatch")
		}
		s.key, s.keyType = expression{kind: compositeExpression, parts: parts}, nil
		s.keySet = true
	}
}

func parsedKey(v declarations.Value) expression {
	if v.Kind == declarations.Name {
		if v.Text == "source" {
			return expression{kind: sourceExpression}
		}
		return expression{kind: pathExpression, text: v.Text}
	}
	if v.Kind != declarations.Call {
		return expression{kind: invalidExpression}
	}
	switch v.Text {
	case "value":
		return literalValue(v.Args[0].Value)
	case "context":
		return expression{kind: contextExpression, text: v.Args[0].Value.Text}
	case "composite":
		e := expression{kind: compositeExpression}
		for _, a := range v.Args {
			e.parts = append(e.parts, keyPart{name: a.Name, expression: parsedKey(a.Value)})
		}
		return e
	default:
		return expression{kind: invalidExpression}
	}
}

func numeric(field serialization.Field) bool {
	if field.IsEnum() {
		return false
	}
	scalar, ok := scalarRepresentation(field)
	return ok && (scalar == serialization.Integer || scalar == serialization.Number)
}

func contextField(path string) (serialization.Field, bool) {
	field, err := resolveContext(path)
	return field, err == nil && field.Scalar.IsPrimitive()
}
