// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

// Condition is an immutable append condition; zero is invalid.
type Condition struct {
	text string
	err  error
}

// PropertyChanges detects a single property's change.
func PropertyChanges(property string) Condition { return conditionPaths([]string{property}, "") }

// AnyOf detects a change to any listed property, preserving order.
func AnyOf(properties ...string) Condition { return conditionPaths(properties, " or ") }

// AllOf detects changes to all listed properties, preserving order.
func AllOf(properties ...string) Condition { return conditionPaths(properties, " and ") }
func conditionPaths(paths []string, join string) Condition {
	if len(paths) == 0 {
		return Condition{err: invalid("condition properties required")}
	}
	operands := make([]string, len(paths))
	for i, path := range paths {
		if !validPath(path) {
			return Condition{err: invalid("invalid condition property")}
		}
		operands[i] = propertyOperand(path)
	}
	return Condition{text: strings.Join(operands, join)}
}

// Transition detects a property's change between two literal string values.
func Transition(property, from, to string) Condition {
	if !validPath(property) || !validLiteral(from) || !validLiteral(to) {
		return Condition{err: invalid("invalid transition property or literal")}
	}
	return Condition{text: propertyOperand(property) + " from " + quoteLiteral(from) + " to " + quoteLiteral(to)}
}

// Added detects a newly observed item.
func Added() Condition { return Condition{text: "added"} }

// Removed detects a disappeared item.
func Removed() Condition { return Condition{text: "removed"} }

// Expression authors a language expression. The pinned runtime rejects it;
// Validate/Save report that capability error instead of claiming activation.
func Expression(expression string) Condition {
	if !line(expression) || strings.ContainsRune(expression, '`') {
		return Condition{err: invalid("invalid condition expression")}
	}
	return Condition{text: "`" + expression + "`"}
}

// AppendRule is immutable. Construct using Append; the zero value is invalid.
type AppendRule struct {
	event       events.TypeID
	when        Condition
	assignments map[string]string
}

// Append snapshots an event's persisted ID (not C#'s CLR simple-name quirk),
// condition and serialized target-path assignments. Expressions are CDL text.
// IDs outside CDL's uppercase-leading single-identifier grammar fail at Build,
// never silently rename.
func Append[T any](event events.Type[T], when Condition, assignments map[string]string) AppendRule {
	return AppendRule{event: event.Descriptor().Ref().ID, when: when, assignments: maps.Clone(assignments)}
}

// Mapping is an immutable map operation. Operations run in declaration order.
type Mapping struct {
	lines []string
	err   error
}

// Rename maps a source property to a target property.
func Rename(source, target string) Mapping {
	if !validPath(source) || !validMapTarget(target) {
		return Mapping{err: invalid("invalid mapping path")}
	}
	return Mapping{lines: []string{target + " = " + source}}
}

// Template assigns a template without surrounding backticks.
func Template(target, template string) Mapping {
	if !validMapTarget(target) || !line(template) || strings.ContainsRune(template, '`') {
		return Mapping{err: invalid("invalid mapping template")}
	}
	return Mapping{lines: []string{target + " = `" + template + "`"}}
}

// Translation is an ordered literal-string translation pair.
type Translation struct{ From, To string }

// Translate maps values in order; duplicate entries are retained like C#.
// Targets must be nonempty CDL word tokens (letters, digits, underscores and
// Unicode word characters), not quoted literals or paths.
func Translate(target, source string, entries ...Translation) Mapping {
	if !validMapTarget(target) || !validPath(source) || len(entries) == 0 {
		return Mapping{err: invalid("invalid translation")}
	}
	result := Mapping{lines: []string{target + " = " + source + " translate"}}
	for _, entry := range entries {
		if !validLiteral(entry.From) {
			return Mapping{err: invalid("invalid translation literal")}
		}
		if !validWord(entry.To) {
			return Mapping{err: fmt.Errorf("%w: capture translation target must fit CDL's word grammar", faults.ErrUnsupported)}
		}
		result.lines = append(result.lines, "  "+quoteLiteral(entry.From)+" => "+entry.To)
	}
	return result
}

// Split splits a source property's value into ordered target properties.
func Split(source, separator string, targets ...string) Mapping {
	if !validPath(source) || len(targets) == 0 || !validLiteral(separator) {
		return Mapping{err: invalid("invalid split")}
	}
	result := Mapping{lines: []string{"split " + source + " by " + quoteLiteral(separator)}}
	for _, target := range targets {
		if !validPath(target) {
			return Mapping{err: invalid("invalid split target")}
		}
		result.lines = append(result.lines, "  "+target)
	}
	return result
}

// Scope is an immutable nested/child set of map operations and append rules.
// NewScope copies slices; contained values are themselves immutable.
type Scope struct {
	mappings []Mapping
	appends  []AppendRule
}

// NewScope constructs a scope without nested scopes (matching C#).
func NewScope(mappings []Mapping, appends ...AppendRule) Scope {
	return Scope{slices.Clone(mappings), slices.Clone(appends)}
}
