// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

// Schema matching uses raw root property names, including literal dots. Index
// ancestry identifies emitted owners without confusing those names with nested
// paths, or flattened ordinary embeddings with emitted object properties.
// Keep this local: serialization.RootFields has other, path-oriented consumers.
func enumRootFields(fields []serialization.Field) []serialization.Field {
	var roots []serialization.Field
	for _, field := range fields {
		if !slices.ContainsFunc(fields, func(owner serialization.Field) bool {
			return len(owner.Index) < len(field.Index) && fieldIndexContains(owner, field)
		}) {
			roots = append(roots, field)
		}
	}
	return roots
}

func fieldIndexContains(owner, field serialization.Field) bool {
	return len(owner.Index) > 0 && len(owner.Index) <= len(field.Index) && slices.Equal(owner.Index, field.Index[:len(owner.Index)])
}

// A literal dotted JSON name and a genuine nested path can have the same Path.
// Do not let a preceding ordinary field hide the enum involved in that ambiguity.
func enumMappingField(fields []serialization.Field, path string) (serialization.Field, bool) {
	for _, field := range fields {
		if field.Path == path && field.IsEnum() {
			return field, true
		}
	}
	return serialization.FieldAt(fields, path)
}

func enumPropertySegments(field serialization.Field, fields []serialization.Field) bool {
	for _, owner := range fields {
		if fieldIndexContains(owner, field) && (strings.Contains(owner.Name, ".") || !declarations.Path(owner.Name)) {
			return false
		}
	}
	return true
}

func enumMappingPathIssue(target, source serialization.Field, e expression, fields, eventFields []serialization.Field) string {
	if !target.IsEnum() && !source.IsEnum() {
		return ""
	}
	if !enumPropertySegments(target, fields) {
		return "enum mapping target has an unrepresentable JSON property name"
	}
	// Sparse Every intentionally permits an absent source. Expression dispatch
	// must still be safe: a literal is evaluated even when the property is absent.
	if e.kind == pathExpression && (!eventPropertyPath(e.text) || !enumPropertySegments(source, eventFields)) {
		return "enum mapping source is not a safe event property expression"
	}
	return ""
}

func validateEnumWrite(d *definition, w write, fields, eventFields []serialization.Field, event events.TypeRef, directive string) error {
	target, _ := enumMappingField(fields, w.path)
	var source serialization.Field
	var exists bool
	if w.expression.kind == pathExpression {
		source, exists = enumMappingField(eventFields, w.expression.text)
	}
	if issue := enumMappingPathIssue(target, source, w.expression, fields, eventFields); issue != "" {
		return enumMappingFailure(d, target, event, directive, issue)
	}
	if exists && (target.IsEnum() || source.IsEnum()) && !target.SameRepresentation(source) {
		return enumMappingFailure(d, target, event, directive, "enum mapping profiles must match exactly")
	}
	return nil
}
