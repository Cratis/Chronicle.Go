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
// the discriminator is a serialization artifact, not a model field. Model-bound
// nodes stamp only their children(...) creators, as C# does. Fluent nodes stamp
// every From of the node because each fluent child From can create the child.
func (c *compiler) stampDerivedChild(n *nodeDefinition, d *declaration, creators []subscription) error {
	e := literalValue(declarations.Value{Kind: declarations.String, Text: n.derivative.ID})
	if err := validateLiteral(e, serialization.Field{Type: reflect.TypeFor[string](), Scalar: serialization.String}); err != nil {
		return declarationFailure(c.result.id, Provenance{Directive: "derived-child", Offset: -1}, invalid("derived type identifier cannot be represented as a kernel literal"))
	}
	for i := range n.from {
		from := &n.from[i]
		if d.modelBound && !slices.ContainsFunc(creators, func(s subscription) bool { return s.event.Ref() == from.event }) {
			continue
		}
		p := Provenance{FrontEnd: "convention", Path: derivedTypeDiscriminator, Directive: "derived-child", Offset: -1, Event: from.event}
		if err := mergeWrite(c.result, &from.writes, write{path: derivedTypeDiscriminator, expression: e, provenance: p, synthetic: true}, false); err != nil {
			return err
		}
		slices.SortFunc(from.writes, func(a, b write) int { return strings.Compare(a.path, b.path) })
	}
	return nil
}

// derivativeHasProjectionDirective reports whether a derivative registered for
// the field (or its collection element) declares projection directives. Only a
// children collection of the family consumes them; anywhere else they would be
// silently ignored, so compilation refuses them.
func derivativeHasProjectionDirective(field serialization.Field) bool {
	for _, derivative := range field.Derivatives() {
		for _, f := range derivative.Fields() {
			directives, err := declarations.Parse(declarations.V1, f.Tag)
			if err != nil {
				return true // Validated earlier; retain fail-closed behavior.
			}
			for _, directive := range directives {
				switch directive.Name {
				case "index", "pii", "encrypted", "compliance-details":
					// Protection and indexes are audited by the read-model plan.
				default:
					return true
				}
			}
		}
	}
	return false
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
