// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/serialization"
)

// derivedTypeDiscriminator is the property the C# DerivedTypeJsonConverter and
// the Go derived-family codec use to select a concrete type on read.
const derivedTypeDiscriminator = "_derivedTypeId"

// stampDerivedChild adds the constant discriminator write that C#
// ChildrenDefinitionExtensions.AddDerivedTypeDiscriminatorMapping adds to the
// creating event of a derived children collection. AutoMap cannot discover it:
// the discriminator is a serialization artifact, not a model field. C# stamps
// only the model-bound children(...) creator, but the kernel adds a child for
// any non-join From whose identity is absent, such as a keyed update after
// removal or before creation. Go therefore stamps every From of the node, in
// both front ends; rewriting the constant on an existing child is idempotent.
// Joins never add a child and stay unstamped.
func (c *compiler) stampDerivedChild(n *nodeDefinition) error {
	e := literalValue(declarations.Value{Kind: declarations.String, Text: n.derivative.ID})
	if err := validateLiteral(e, serialization.Field{Type: reflect.TypeFor[string](), Scalar: serialization.String}); err != nil {
		return declarationFailure(c.result.id, Provenance{Directive: "derived-child", Offset: -1}, invalid("derived type identifier cannot be represented as a kernel literal"))
	}
	for i := range n.from {
		from := &n.from[i]
		p := Provenance{FrontEnd: "convention", Path: derivedTypeDiscriminator, Directive: "derived-child", Offset: -1, Event: from.event}
		if err := mergeWrite(c.result, &from.writes, write{path: derivedTypeDiscriminator, expression: e, provenance: p, synthetic: true}, false); err != nil {
			return err
		}
		slices.SortFunc(from.writes, func(a, b write) int { return strings.Compare(a.path, b.path) })
	}
	return nil
}

// derivativeHasDirective reports whether a derivative registered for the field
// (or its collection element) declares a directive matching the predicate. An
// unparsable tag matches, retaining fail-closed behavior; tags are validated
// earlier.
func derivativeHasDirective(field serialization.Field, match func(name string) bool) bool {
	for _, derivative := range field.Derivatives() {
		for _, f := range derivative.Fields() {
			directives, err := declarations.Parse(declarations.V1, f.Tag)
			if err != nil {
				return true
			}
			if slices.ContainsFunc(directives, func(d declarations.Directive) bool { return match(d.Name) }) {
				return true
			}
		}
	}
	return false
}

// derivedDeclarationDirective reports whether a derivative field directive
// declares projection behavior that only a children collection of the family
// consumes. Anywhere else it would be silently ignored, so compilation refuses
// it. Key, exclusion, protection and index tags are type metadata: a fluent
// derivative needs a key, and its family may be held as a whole property.
func derivedDeclarationDirective(name string) bool {
	switch name {
	case "key", "no-auto", "not-projected", "index", "pii", "encrypted", "compliance-details":
		return false
	}
	return true
}

// validateDerivedChildGlobals rejects a bare-name global write that the kernel
// would apply to a derived child's discriminator. The kernel matches property
// names case-insensitively, so any casing of the discriminator conflicts.
func validateDerivedChildGlobals(n *nodeDefinition, inherited []write) error {
	if n.derivative != nil {
		for _, w := range append(slices.Clone(inherited), n.all...) {
			if strings.EqualFold(w.path, derivedTypeDiscriminator) {
				return invalid("global write conflicts with a derived child discriminator")
			}
		}
	}
	if n.includeChildren {
		inherited = append(slices.Clone(inherited), n.all...)
	}
	for _, nodes := range []map[string]*nodeDefinition{n.children, n.nested} {
		for _, path := range sortedKeys(nodes) {
			if err := validateDerivedChildGlobals(nodes[path], inherited); err != nil {
				return err
			}
		}
	}
	return nil
}
