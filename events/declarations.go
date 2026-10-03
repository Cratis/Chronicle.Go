// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// DeclarationError locates invalid event metadata. Error text redacts literals.
type DeclarationError = declarations.DeclarationError

// Unique declares one event-type lifecycle constraint (not property uniqueness).
// Events sharing Name are mutually exclusive per source until a remover occurs.
// Empty Name defaults to the Go type name; empty EventSequences means all sequences.
// Message is selected for the violating event type, then the first nonempty message.
type Unique struct {
	// Name is the stable shared constraint name, or empty for the Go type name.
	Name string
	// Message is a client-side violation template; empty preserves kernel text.
	Message string
	// EventSequences restricts enforcement; empty means every sequence.
	EventSequences []SequenceID
}

// WithUnique declares event-type uniqueness. Only one declaration is allowed per
// type, matching [Unique]. Inputs are copied; NewClient validates the frozen graph.
func WithUnique(declaration Unique) TypeOption {
	declaration.EventSequences = slices.Clone(declaration.EventSequences)
	return func(c *typeConfig) { c.unique = append(c.unique, declaration) }
}

// WithRemoveConstraints declares names released by this event for its source.
// Calls accumulate, duplicate names are ignored, and blank/unknown names fail at
// NewClient. Names refer to model-bound constraints; for explicit definitions use
// the builder's RemovedWith method instead.
func WithRemoveConstraints(names ...string) TypeOption {
	owned := slices.Clone(names)
	return func(c *typeConfig) { c.removes = append(c.removes, owned...) }
}

// WithTombstone marks the registration's EventType.Tombstone flag. It does not
// erase data or change append routing; historical generation APIs are separate.
func WithTombstone() TypeOption { return func(c *typeConfig) { c.tombstone = true } }

// WithCompensationFor adds the target's persisted ID as top-level compensationFor
// schema metadata. It never performs compensation. Last option wins; the handle
// must belong to the same frozen registry, validated by NewClient.
func WithCompensationFor[T any](event Type[T]) TypeOption {
	descriptor := event.Descriptor()
	return func(c *typeConfig) { c.compensation = &descriptor }
}

// IsTombstone reports the explicit registration flag.
func (d Descriptor) IsTombstone() bool { return d.tombstone }

// CompensationFor returns the referenced identity and whether it was declared.
func (d Descriptor) CompensationFor() (TypeRef, bool) {
	if d.compensation == nil {
		return TypeRef{}, false
	}
	return d.compensation.Ref(), true
}

// UniqueDeclarations returns detached type-level lifecycle metadata.
func (d Descriptor) UniqueDeclarations() []Unique {
	result := slices.Clone(d.unique)
	for i := range result {
		result[i].EventSequences = slices.Clone(result[i].EventSequences)
	}
	return result
}

// RemovedConstraints returns detached, ordered removal names.
func (d Descriptor) RemovedConstraints() []string { return slices.Clone(d.removes) }

// ValidateDeclarations validates catalog-relative descriptor references without I/O.
// NewClient calls it for every store registry before any connection work.
func (c *Catalog) ValidateDeclarations() error {
	for _, event := range c.ordered {
		if target := event.compensation; target != nil {
			registered, ok := c.LookupRef(target.Ref())
			if !ok || registered.GoType() != target.GoType() {
				return event.declarationError(serialization.Field{}, declarations.Directive{Name: "compensation-for", Offset: -1}, "compensated event is not registered in this store")
			}
		}
	}
	return nil
}

func (d Descriptor) compileDeclarations() (Descriptor, error) {
	var subject *serialization.Field
	for _, field := range d.Fields() {
		directives, err := declarations.Parse(declarations.V1, field.Tag)
		if err != nil {
			return Descriptor{}, err
		}
		seen := map[string]bool{}
		for _, directive := range directives {
			if seen[directive.Name] {
				return Descriptor{}, d.declarationError(field, directive, "duplicate field declaration")
			}
			seen[directive.Name] = true
			if len(field.Index) != 1 || field.Collection {
				return Descriptor{}, d.declarationError(field, directive, "event declarations require a top-level field")
			}
			if directive.Name == "subject" {
				if subject != nil {
					return Descriptor{}, d.declarationError(field, directive, "only one subject field is allowed")
				}
				if field.Scalar == serialization.NotScalar {
					return Descriptor{}, d.declarationError(field, directive, "subject requires a scalar or scalar concept")
				}
				subject = &field
			}
		}
	}
	if subject != nil && d.subject == nil {
		d.subject = taggedSubject(d.typ, *subject)
	}
	return d.withCompensationSchema()
}

func (d Descriptor) withCompensationSchema() (Descriptor, error) {
	if d.compensation != nil {
		var schema map[string]any
		if err := json.Unmarshal([]byte(d.plan.Schema()), &schema); err != nil {
			return Descriptor{}, err
		}
		schema["compensationFor"] = string(d.compensation.Ref().ID)
		data, err := json.Marshal(schema)
		if err != nil {
			return Descriptor{}, err
		}
		d.schema = string(data)
	}
	return d, nil
}

func (d Descriptor) declarationError(field serialization.Field, directive declarations.Directive, message string) error {
	return &DeclarationError{Artifact: d.typ.String(), GoField: field.GoField, Path: field.Path, Directive: directive.Name, Offset: directive.Offset, Message: message, Cause: faults.ErrInvalidConfiguration}
}

func taggedSubject(typ reflect.Type, field serialization.Field) func(any) (Subject, bool) {
	fieldType := field.Type
	for fieldType.Kind() == reflect.Pointer {
		fieldType = fieldType.Elem()
	}
	// Metadata discovery never calls ConceptValue on a fabricated value.
	_, concept := fieldType.MethodByName("ConceptValue") // Validated by the serialization plan.
	// Shared Fundamentals scalars have codecs and String, but no ConceptValue.
	return func(value any) (Subject, bool) {
		v := reflect.ValueOf(value)
		if v.IsValid() && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return "", false
			}
			v = v.Elem()
		}
		if !v.IsValid() || v.Type() != typ {
			return "", false
		}
		v = v.Field(field.Index[0])
		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return "", false
			}
			v = v.Elem()
		}
		if concept {
			v = v.MethodByName("ConceptValue").Call(nil)[0]
		}
		return Subject(fmt.Sprint(v.Interface())), true
	}
}

// ValidateUnique validates copied type-level metadata before constraint compilation.
func (d Descriptor) ValidateUnique() error {
	fail := func(message string) error {
		return d.declarationError(serialization.Field{}, declarations.Directive{Name: "unique", Offset: -1}, message)
	}
	if len(d.unique) > 1 {
		return fail("only one type-level unique declaration is allowed")
	}
	for _, unique := range d.unique {
		if unique.Name != "" && strings.TrimSpace(unique.Name) == "" {
			return fail("constraint name must be nonblank")
		}
		for _, sequence := range unique.EventSequences {
			if strings.TrimSpace(string(sequence)) == "" {
				return fail("event sequence must be nonblank")
			}
		}
	}
	return nil
}
