// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

// Validate the graph that will actually be encoded, after shared mappings and
// variant handlers have been lowered. Generated joins must obey the same enum
// correlation boundary as explicit joins, even with AutoMap disabled.
func validateEnumGraph(d *definition, catalog *events.Catalog) error {
	noAuto := d.noAuto && !d.inheritAuto
	return validateEnumNode(d, &d.nodeDefinition, d.model.Fields(), catalog, noAuto, noAuto)
}

func validateEnumNode(d *definition, n *nodeDefinition, fields []serialization.Field, catalog *events.Catalog, noAuto, projectionNoAuto bool) error {
	for _, join := range n.joins {
		if target, ok := serialization.FieldAt(fields, join.on); ok && target.IsEnum() {
			return enumMappingFailure(d, target, join.event, "join", "enum correlation keys are not supported")
		}
	}
	for _, from := range n.from {
		if err := validateEnumHandler(d, from, false, n, fields, catalog, noAuto); err != nil {
			return err
		}
	}
	for _, join := range n.joins {
		if err := validateEnumHandler(d, join.fromDefinition, true, n, fields, catalog, noAuto); err != nil {
			return err
		}
	}
	for _, path := range sortedKeys(n.children) {
		child := n.children[path]
		// The pinned factory creates a separate Projection for each collection,
		// retaining encoded Inherit. Its property merger disables AutoMap only
		// for Disabled, so parent NoAutoMap must not skip this child's checks.
		childNoAuto := child.noAuto && !child.inheritAuto
		if err := validateEnumNode(d, child, scopedFields(fields, path), catalog, childNoAuto, childNoAuto); err != nil {
			return err
		}
	}
	for _, path := range sortedKeys(n.nested) {
		nested := n.nested[path]
		nestedNoAuto := nested.noAuto
		if nested.inheritAuto {
			// Recursive nested subscriptions use the containing Projection's
			// AutoMap, not the immediately enclosing nested definition's mode.
			nestedNoAuto = projectionNoAuto
		}
		if err := validateEnumNode(d, nested, scopedFields(fields, path), catalog, nestedNoAuto, projectionNoAuto); err != nil {
			return err
		}
	}
	return nil
}

func enumMappingFailure(d *definition, target serialization.Field, event events.TypeRef, directive, message string) error {
	return declarationFailure(d.id, Provenance{FrontEnd: "compiled", GoField: target.GoField, Path: target.Path, Directive: directive, Offset: -1, Event: event}, invalid(message))
}

func sameEventEnumProfiles(a, b events.Descriptor) bool {
	for _, f := range a.Fields() {
		other, exists := serialization.FieldAt(b.Fields(), f.Path)
		if (f.IsEnum() || other.IsEnum()) && (!exists || !f.SameRepresentation(other)) {
			return false
		}
	}
	return true
}

func sortedKeys[V any](entries map[string]V) []string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
