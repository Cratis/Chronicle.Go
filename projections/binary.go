// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

// Binary capability metadata comes from the compiled codec, never Go kinds or
// the JSON string shape. Objects containing binary need the same qualification.
func binaryField(field serialization.Field) bool { return field.ContainsBinary() }

func binaryMappingField(fields []serialization.Field, path string) (serialization.Field, bool) {
	return serialization.FieldAtWithCapability(fields, path, binaryField)
}

func validateBinaryGraph(d *definition, catalog *events.Catalog) error {
	fields := d.model.Fields()
	for _, f := range serialization.EmittedRootFields(fields) {
		if (strings.EqualFold(f.Name, "id") || f.Name == "_id" || strings.EqualFold(lastGoName(f.GoField), "Id")) && binaryField(f) {
			return enumMappingFailure(d, f, events.TypeRef{}, "key", "binary identities are not supported")
		}
	}
	if d.subscribesAll {
		for _, w := range d.all {
			if target, ok := binaryMappingField(fields, w.path); ok && binaryField(target) {
				return enumMappingFailure(d, target, events.TypeRef{}, "all", "all-event binary mappings cannot validate unknown representations")
			}
		}
	}
	noAuto := d.noAuto && !d.inheritAuto
	return validateBinaryNode(d, &d.nodeDefinition, fields, catalog, noAuto, noAuto)
}

func validateBinaryNode(d *definition, n *nodeDefinition, fields []serialization.Field, catalog *events.Catalog, noAuto, projectionNoAuto bool) error {
	for _, path := range []string{n.keyField, n.identifiedBy} {
		if path == "" || path == "$eventSourceId" || path == "*NotSet*" {
			continue
		}
		if field, ok := binaryMappingField(fields, path); ok && binaryField(field) {
			return enumMappingFailure(d, field, events.TypeRef{}, "key", "binary identities are not supported")
		}
	}
	checkKeys := func(ref events.TypeRef, key, parent expression) error {
		event, ok := catalog.LookupRef(ref)
		if !ok {
			return invalid("binary mapping event is not registered")
		}
		for _, e := range []expression{key, parent} {
			if field, ok := binaryKeyField(event.Fields(), e); ok {
				return enumMappingFailure(d, field, ref, "key", "binary correlation keys are not supported")
			}
		}
		return nil
	}
	for _, removal := range n.removals {
		if err := checkKeys(removal.event, removal.key, removal.parent); err != nil {
			return err
		}
	}
	check := func(from fromDefinition, join bool) error {
		if err := checkKeys(from.event, from.key, from.parent); err != nil {
			return err
		}
		event, ok := catalog.LookupRef(from.event)
		if !ok {
			return invalid("binary mapping event is not registered")
		}
		sources := enumRootFields(event.Fields())
		targets := enumRootFields(fields)
		for _, w := range append(slices.Clone(n.all), from.writes...) {
			target, _ := binaryMappingField(fields, w.path)
			var source serialization.Field
			switch w.expression.kind {
			case pathExpression, addExpression, subtractExpression:
				source, _ = binaryMappingField(event.Fields(), w.expression.text)
			}
			if !binaryField(target) && !binaryField(source) {
				continue
			}
			return enumMappingFailure(d, target, from.event, "set", "binary is only qualified for same-representation AutoMap")
		}
		if noAuto || !join && aggregateOnly(from.writes) {
			return nil
		}
		if slices.ContainsFunc(targets, binaryField) || slices.ContainsFunc(sources, binaryField) {
			// Kernel AutoMap uses CLR OrdinalIgnoreCase, whose Unicode pairs
			// are not the ASCII-only matcher's pairs. Qualify before matching.
			if !asciiFieldNames(targets) || !asciiFieldNames(sources) || !asciiWrites(from.writes) {
				candidates := append(slices.Clone(targets), sources...)
				field := candidates[slices.IndexFunc(candidates, binaryField)]
				return enumMappingFailure(d, field, from.event, "AutoMap", "binary auto-map requires ASCII property names; CLR Unicode comparison is not qualified")
			}
		}
		for _, source := range sources {
			matches := matchingASCIIFields(targets, source.Name)
			if len(matches) == 0 {
				continue
			}
			// AutoMap emits property paths from raw root names. Inspect every
			// candidate for those paths, not just the matched emitted fields.
			sourceCandidate, _ := binaryMappingField(event.Fields(), source.Path)
			binaryTarget := slices.ContainsFunc(matches, func(target serialization.Field) bool {
				candidate, _ := binaryMappingField(fields, target.Path)
				return binaryField(candidate)
			})
			if !binaryField(sourceCandidate) && !binaryTarget {
				continue
			}
			target := matches[0]
			if slices.Contains(n.exclusions, target.Path) || autoMapWritten(from.writes, source.Name, join) {
				continue
			}
			if !eventPropertyPath(source.Name) || !enumPropertySegments(source, event.Fields()) || !enumPropertySegments(target, fields) || len(matches) != 1 || len(matchingASCIIFields(sources, source.Name)) != 1 || !target.SameRepresentation(source) {
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
		if target, ok := binaryMappingField(fields, join.on); ok && binaryField(target) {
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
