// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

func validateEnumHandler(d *definition, from fromDefinition, join bool, n *nodeDefinition, fields []serialization.Field, catalog *events.Catalog, noAuto bool) error {
	event, ok := catalog.LookupRef(from.event)
	if !ok {
		return invalid("enum auto-map event is not registered")
	}
	sources := serialization.RootFields(event.Fields())
	targets := serialization.RootFields(fields)
	// Every can acquire additional handlers through globals or EntersOn. Check
	// known source profiles again against the final subscription population.
	for _, w := range n.all {
		target, _ := serialization.FieldAt(fields, w.path)
		if source, exists := serialization.FieldAt(event.Fields(), w.expression.text); w.expression.kind == pathExpression && exists && (source.IsEnum() || target.IsEnum()) && !target.SameRepresentation(source) {
			return enumMappingFailure(d, target, from.event, "every", "global enum profiles must match exactly")
		}
	}
	if noAuto || !join && aggregateOnly(from.writes) {
		return nil
	}
	if !slices.ContainsFunc(targets, serialization.Field.IsEnum) && !slices.ContainsFunc(sources, serialization.Field.IsEnum) {
		return nil
	}
	// CLR OrdinalIgnoreCase is not Go EqualFold (for example long-s/Kelvin).
	// Qualify ASCII only; Unicode names in enum-active AutoMap handlers are
	// explicitly unsupported rather than guessing a CLR equivalence table.
	if !asciiFieldNames(targets) || !asciiFieldNames(sources) || !asciiWrites(from.writes) {
		target := firstEnumField(targets, sources)
		return enumMappingFailure(d, target, from.event, "AutoMap", "enum auto-map requires ASCII property names; CLR Unicode comparison is not qualified")
	}
	for _, source := range sources {
		matches := matchingASCIIFields(targets, source.Name)
		if len(matches) == 0 {
			continue
		}
		target := matches[0]
		enum := source.IsEnum() || slices.ContainsFunc(matches, serialization.Field.IsEnum)
		if !enum || slices.Contains(n.exclusions, target.Path) || autoMapWritten(from.writes, source.Name, join) {
			continue
		}
		if len(matches) != 1 || len(matchingASCIIFields(sources, source.Name)) != 1 {
			return enumMappingFailure(d, firstEnumField(matches, []serialization.Field{source}), from.event, "AutoMap", "ambiguous case-insensitive enum auto-map properties")
		}
		if !target.SameRepresentation(source) {
			return enumMappingFailure(d, target, from.event, "AutoMap", "auto-map enum profiles must match exactly")
		}
	}
	return nil
}

// Compare only the qualified ASCII case pairs, never Unicode SimpleFold pairs.
func equalASCIIName(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		if lowerASCII(a[i]) != lowerASCII(b[i]) {
			return false
		}
	}
	return true
}

func lowerASCII(value byte) byte {
	if value >= 'A' && value <= 'Z' {
		return value + 'a' - 'A'
	}
	return value
}

func matchingASCIIFields(fields []serialization.Field, name string) []serialization.Field {
	var matches []serialization.Field
	for _, field := range fields {
		if equalASCIIName(field.Name, name) {
			matches = append(matches, field)
		}
	}
	return matches
}

// Kernel GetMergedFromProperties suppresses targets by their last segment;
// GetMergedJoinProperties additionally suppresses explicit source expressions.
func autoMapWritten(writes []write, name string, join bool) bool {
	for _, w := range writes {
		parts := strings.Split(w.path, ".")
		if equalASCIIName(parts[len(parts)-1], name) || join && equalASCIIName(w.expression.encode(), name) {
			return true
		}
	}
	return false
}

func aggregateOnly(writes []write) bool {
	return len(writes) > 0 && !slices.ContainsFunc(writes, func(w write) bool {
		switch w.expression.kind {
		case addExpression, subtractExpression, countExpression, incrementExpression, decrementExpression:
			return false
		default:
			return true
		}
	})
}

func asciiFieldNames(fields []serialization.Field) bool {
	return !slices.ContainsFunc(fields, func(f serialization.Field) bool { return !asciiName(f.Name) })
}

func asciiWrites(writes []write) bool {
	return !slices.ContainsFunc(writes, func(w write) bool {
		return !asciiName(w.path) || w.expression.kind == pathExpression && !asciiName(w.expression.text)
	})
}

func asciiName(name string) bool {
	for _, r := range name {
		if r > 127 {
			return false
		}
	}
	return true
}

func firstEnumField(targets, sources []serialization.Field) serialization.Field {
	for _, field := range targets {
		if field.IsEnum() {
			return field
		}
	}
	for _, field := range sources {
		if field.IsEnum() {
			return field
		}
	}
	return serialization.Field{}
}
