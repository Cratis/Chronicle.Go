// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/cratis/chronicle.go/internal/faults"
)

const derivedTypeID = "_derivedTypeId"

// Codec is an immutable registration declaration. Construct it with Derived.
// Its zero value is invalid. No application callbacks or discovery are involved.
type Codec struct {
	family, concrete reflect.Type
	id               string
}

// Derived admits exactly Concrete as an implementation of Family with a stable
// wire discriminator. Family must be an interface; Concrete a named struct or a
// pointer to one. NewCodecs validates declarations, including assignability.
func Derived[Family, Concrete any](stableID string) Codec {
	return Codec{reflect.TypeFor[Family](), reflect.TypeFor[Concrete](), stableID}
}

// Codecs is an immutable, concurrency-safe set of explicit family registrations.
// Its zero value (and a nil *Codecs) admits no families. Registrations are never
// global and cannot be modified after construction.
type Codecs struct {
	ordered  []Codec
	families map[reflect.Type][]Codec
}

// NewCodecs copies and validates registrations in declaration order. Duplicate
// pairs, conflicting IDs and pointer/value duplicates are errors. One concrete
// type may belong to multiple families only with the same globally unique ID.
func NewCodecs(registrations ...Codec) (*Codecs, error) {
	c := &Codecs{ordered: append([]Codec(nil), registrations...), families: map[reflect.Type][]Codec{}}
	ids := map[string]reflect.Type{}
	types := map[reflect.Type]Codec{}
	for _, r := range c.ordered {
		if r.family == nil || r.family.Kind() != reflect.Interface || r.concrete == nil {
			return nil, codecError(r, "", "interface family and concrete struct required")
		}
		base := r.concrete
		if base.Kind() == reflect.Pointer {
			base = base.Elem()
		}
		if base.Kind() != reflect.Struct || base.Name() == "" || !r.concrete.AssignableTo(r.family) {
			return nil, codecError(r, "", "concrete must be a named struct or pointer assignable to family")
		}
		if strings.TrimSpace(r.id) == "" || !utf8.ValidString(r.id) {
			return nil, codecError(r, "", "nonblank UTF-8 discriminator required")
		}
		if previous, exists := ids[r.id]; exists && previous != r.concrete {
			return nil, codecError(r, "", "duplicate discriminator")
		}
		if previous, exists := types[base]; exists && (previous.concrete != r.concrete || previous.id != r.id) {
			return nil, codecError(r, "", "conflicting concrete identity or pointer/value registration")
		}
		for _, previous := range c.families[r.family] {
			if previous.concrete == r.concrete {
				return nil, codecError(r, "", "duplicate family registration")
			}
		}
		ids[r.id], types[base] = r.concrete, r
		c.families[r.family] = append(c.families[r.family], r)
	}
	return c, nil
}

// CodecError identifies an invalid codec declaration without formatting payloads
// or discriminator values. Family, Concrete and Field describe declared types and
// Go member identity. It matches ErrInvalidConfiguration.
type CodecError struct {
	Role     string
	Artifact reflect.Type
	Family   reflect.Type
	Concrete reflect.Type
	Field    string
	Message  string
}

// Error returns configuration-only diagnostics; discriminator values are redacted.
func (e *CodecError) Error() string {
	return fmt.Sprintf("chronicle: %s %v derived codec family %v concrete %v field %s (%s): %s", e.Role, e.Artifact, e.Family, e.Concrete, e.Field, derivedTypeID, e.Message)
}

// Is reports the invalid configuration category.
func (*CodecError) Is(target error) bool { return target == faults.ErrInvalidConfiguration }

func codecError(r Codec, field, message string) error {
	return &CodecError{Role: "codec registration", Family: r.family, Concrete: r.concrete, Field: field, Message: message}
}

// Config selects immutable codec and naming configuration for CompileWith.
// The zero value preserves property names and admits no families.
type Config struct {
	Codecs       *Codecs
	NamingPolicy NamingPolicy
}

type derivative struct {
	registration Codec
	node         *node
}

func (c *Codecs) registrations(typ reflect.Type) []Codec {
	if c == nil {
		return nil
	}
	return c.families[typ]
}

func (n *node) compileFamily(state *compileState, policy NamingPolicy) error {
	registrations := state.codecs.registrations(n.typ)
	if len(registrations) == 0 {
		return unsupported(n.typ, "interface requires explicit derived codecs")
	}
	// The kernel deliberately preserves this open object verbatim. The closed
	// validation graph stays in the plan; it is not a oneOf wire schema.
	n.schema["type"] = "object"
	n.family = true
	for _, registration := range registrations {
		fields, err := serializedFields(dereference(registration.concrete), CamelCase, false)
		if err != nil {
			return err
		}
		for _, f := range fields {
			if strings.EqualFold(f.name, derivedTypeID) {
				return codecError(registration, f.goName, "property collides with discriminator")
			}
			if dereference(f.field.Type).Kind() == reflect.Interface {
				return codecError(registration, f.goName, "direct family-valued derivative property loses its discriminator in Fundamentals")
			}
		}
		child, err := compileContext(registration.concrete, state, policy, false, true)
		if err != nil {
			return err
		}
		object := child
		for object.item != nil {
			object = object.item
		}
		if object.reference != nil {
			object = object.reference
		}
		if object.scalar || object.concept != nil {
			return codecError(registration, "", "derivative requires an ordinary object codec")
		}
		n.derivatives = append(n.derivatives, derivative{registration, child})
	}
	return nil
}
