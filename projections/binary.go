// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

// Binary is a compiled string leaf, not a Go-slice collection. Include object
// owners and binary arrays without reconstructing representation from Go kinds.
func binaryField(field serialization.Field) bool {
	if field.Format == "byte-array" {
		return true
	}
	if field.Scalar == serialization.NotScalar {
		if item, ok := field.Element(); ok && item.Format == "byte-array" {
			return true
		}
	}
	return slices.ContainsFunc(field.Fields(), func(f serialization.Field) bool { return f.Format == "byte-array" })
}

func validateBinaryGraph(d *definition, catalog *events.Catalog) error {
	fields := d.model.Fields()
	for _, f := range serialization.RootFields(fields) {
		if strings.EqualFold(lastGoName(f.GoField), "Id") && binaryField(f) {
			return enumMappingFailure(d, f, events.TypeRef{}, "key", "binary identities are not supported")
		}
	}
	if d.subscribesAll {
		for _, w := range d.all {
			if target, ok := serialization.FieldAt(fields, w.path); ok && binaryField(target) {
				return enumMappingFailure(d, target, events.TypeRef{}, "all", "all-event binary mappings cannot validate unknown representations")
			}
		}
	}
	noAuto := d.noAuto && !d.inheritAuto
	return validateBinaryNode(d, &d.nodeDefinition, fields, catalog, noAuto, noAuto)
}

func validateBinaryNode(d *definition, n *nodeDefinition, fields []serialization.Field, catalog *events.Catalog, noAuto, projectionNoAuto bool) error {
	check := func(from fromDefinition, join bool) error {
		event, ok := catalog.LookupRef(from.event)
		if !ok {
			return invalid("binary mapping event is not registered")
		}
		sources := enumRootFields(event.Fields())
		targets := enumRootFields(fields)
		for _, w := range append(slices.Clone(n.all), from.writes...) {
			target, _ := serialization.FieldAt(fields, w.path)
			source, _ := serialization.FieldAt(event.Fields(), w.expression.text)
			if !binaryField(target) && !binaryField(source) {
				continue
			}
			if w.expression.kind != pathExpression || !eventPropertyPath(w.expression.text) || !enumPropertySegments(target, fields) || !enumPropertySegments(source, event.Fields()) || !target.SameRepresentation(source) {
				return enumMappingFailure(d, target, from.event, "set", "binary mappings require the same compiled representation")
			}
		}
		if noAuto || !join && aggregateOnly(from.writes) {
			return nil
		}
		for _, source := range sources {
			matches := matchingASCIIFields(targets, source.Name)
			if len(matches) == 0 || !binaryField(source) && !slices.ContainsFunc(matches, binaryField) {
				continue
			}
			target := matches[0]
			if slices.Contains(n.exclusions, target.Path) || autoMapWritten(from.writes, source.Name, join) {
				continue
			}
			if !asciiFieldNames(targets) || !asciiFieldNames(sources) || !eventPropertyPath(source.Name) || !enumPropertySegments(source, event.Fields()) || !enumPropertySegments(target, fields) || len(matches) != 1 || len(matchingASCIIFields(sources, source.Name)) != 1 || !target.SameRepresentation(source) {
				return enumMappingFailure(d, target, from.event, "AutoMap", "auto-map binary representations must match unambiguously")
			}
		}
		return nil
	}
	for _, from := range n.from {
		if err := check(from, false); err != nil {
			return err
		}
	}
	for _, join := range n.joins {
		if target, ok := serialization.FieldAt(fields, join.on); ok && binaryField(target) {
			return enumMappingFailure(d, target, join.event, "join", "binary correlation keys are not supported")
		}
		if err := check(join.fromDefinition, true); err != nil {
			return err
		}
	}
	for _, path := range sortedKeys(n.children) {
		child := n.children[path]
		childNoAuto := child.noAuto && !child.inheritAuto
		if err := validateBinaryNode(d, child, scopedFields(fields, path), catalog, childNoAuto, childNoAuto); err != nil {
			return err
		}
	}
	for _, path := range sortedKeys(n.nested) {
		nested := n.nested[path]
		nestedNoAuto := nested.noAuto
		if nested.inheritAuto {
			nestedNoAuto = projectionNoAuto
		}
		if err := validateBinaryNode(d, nested, scopedFields(fields, path), catalog, nestedNoAuto, projectionNoAuto); err != nil {
			return err
		}
	}
	return nil
}
