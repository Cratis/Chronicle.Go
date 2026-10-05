// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package captures authors explicit capture declarations for the kernel. There
// is no automatic discovery, registration or local capture worker. The kernel
// currently runs only API sources and root append rules; validation reports
// unsupported language features before activation.
package captures

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/google/uuid"
)

// Capturer is the explicit equivalent of C# ICapturer. Define is called once by
// Prepare, never on reconnect. It must not retain the builder or perform I/O.
type Capturer interface{ Define(*Builder) error }

// Prepare invokes a borrowed capturer and freezes its definition. No container,
// reflection discovery or automatic registration is involved.
func Prepare(name string, capturer Capturer) (Definition, error) {
	if capturer == nil {
		return Definition{}, invalid("capturer required")
	}
	b := &Builder{}
	if err := capturer.Define(b); err != nil {
		return Definition{}, err
	}
	return b.Build(name)
}

// Definition is immutable. Build assigns a fresh UUID, matching C#. Copies are
// safe for concurrent use. Formatting is redacted; Declaration explicitly exports text.
type Definition struct {
	id   uuid.UUID
	text string
}

// ID returns this declaration's identity; repeated saves update this capture.
func (d Definition) ID() uuid.UUID { return d.id }

// Declaration returns the frozen CDL text. No credentials are embedded in it.
func (d Definition) Declaration() string { return d.text }

// Format redacts configuration, including mapping literals, for every fmt verb.
func (d Definition) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("captures.Definition{redacted}"))
}

// Source is an immutable source declaration. Zero is invalid.
type Source struct{ kind, name, route, poll string }

// API refers to an external HTTP service by name. Poll uses kernel units s/m/h/d;
// an empty interval or route is preserved for server validation, like C#.
func API(service, route, poll string) Source {
	return Source{kind: "api", name: service, route: route, poll: poll}
}

// Webhook declares an inbound path. The pinned kernel cannot run webhook sources.
func Webhook(path string) Source { return Source{kind: "webhook", name: path} }

// MessageTopic declares a topic. The pinned kernel cannot run message sources.
func MessageTopic(topic string) Source { return Source{kind: "message", name: topic} }

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

type nested struct {
	path, key string
	scope     Scope
}

// Builder is a local mutable authoring builder, not safe for concurrent mutation.
// Source, key and Map are last-wins; append/nested/children accumulate. Build
// validates atomically and returns a frozen snapshot. Zero is usable.
type Builder struct {
	source           Source
	key              string
	scope            Scope
	nested, children []nested
}

// From selects the source, replacing the previous source.
func (b *Builder) From(source Source) *Builder { b.source = source; return b }

// Key sets the identity property used by kernel diffing.
func (b *Builder) Key(path string) *Builder { b.key = path; return b }

// Map replaces root map operations, preserving their order.
func (b *Builder) Map(operations ...Mapping) *Builder {
	b.scope.mappings = slices.Clone(operations)
	return b
}

// Append adds rules in declaration order.
func (b *Builder) Append(rules ...AppendRule) *Builder {
	b.scope.appends = append(b.scope.appends, rules...)
	return b
}

// Nested adds a scoped object declaration.
func (b *Builder) Nested(path string, scope Scope) *Builder {
	b.nested = append(b.nested, nested{path: path, scope: scope})
	return b
}

// Children adds an identified collection declaration.
func (b *Builder) Children(path, key string, scope Scope) *Builder {
	b.children = append(b.children, nested{path: path, key: key, scope: scope})
	return b
}

// Build validates source/key/conditions and renders a new named CDL definition.
// It does not claim runtime support for every authorable language construct.
func (b *Builder) Build(name string) (Definition, error) {
	if b == nil || !validCaptureName(name) || !validPath(b.key) || b.source.kind == "" || !line(b.source.name) {
		return Definition{}, invalid("capture name, source and key required")
	}
	var out strings.Builder
	write := func(indent int, text string) {
		out.WriteString(strings.Repeat(" ", indent))
		out.WriteString(text)
		out.WriteByte('\n')
	}
	write(0, "capture "+name)
	write(2, "source "+b.source.kind)
	switch b.source.kind {
	case "api":
		write(4, "api "+b.source.name)
		for _, entry := range []struct{ key, value string }{{"route", b.source.route}, {"poll", b.source.poll}} {
			if entry.value != "" {
				if !line(entry.value) {
					return Definition{}, invalid("invalid source setting")
				}
				write(4, entry.key+" "+entry.value)
			}
		}
	case "webhook":
		write(4, "path "+b.source.name)
	case "message":
		write(4, "topic "+b.source.name)
	default:
		return Definition{}, invalid("invalid source")
	}
	write(2, "key "+b.key)
	if err := renderScope(b.scope, 2, write); err != nil {
		return Definition{}, err
	}
	for _, group := range []struct {
		kind   string
		values []nested
	}{{"nested", b.nested}, {"children", b.children}} {
		for _, n := range group.values {
			if !validPath(n.path) || (group.kind == "children" && (!validMapTarget(n.path) || !validPath(n.key))) {
				return Definition{}, invalid("invalid scope path or child key")
			}
			header := group.kind + " " + n.path
			if group.kind == "children" {
				header += " identified by " + n.key
			}
			write(2, header)
			if err := renderScope(n.scope, 4, write); err != nil {
				return Definition{}, err
			}
		}
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return Definition{}, err
	}
	return Definition{id: id, text: out.String()}, nil
}
func renderScope(scope Scope, indent int, write func(int, string)) error {
	if len(scope.mappings) > 0 {
		write(indent, "map")
	}
	for _, mapping := range scope.mappings {
		if mapping.err != nil {
			return mapping.err
		}
		if len(mapping.lines) == 0 {
			return invalid("map operation required")
		}
		for _, line := range mapping.lines {
			write(indent+2, line)
		}
	}
	for _, rule := range scope.appends {
		if !validEventID(string(rule.event)) {
			return fmt.Errorf("%w: capture event ID must fit CDL's uppercase-leading single identifier grammar", faults.ErrUnsupported)
		}
		if rule.when.err != nil {
			return rule.when.err
		}
		if rule.when.text == "" {
			return invalid("append condition required")
		}
		write(indent, "append "+string(rule.event))
		write(indent+2, "when "+rule.when.text)
		keys := make([]string, 0, len(rule.assignments))
		for key := range rule.assignments {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			value := rule.assignments[key]
			if !validPath(key) || !line(value) {
				return invalid("invalid assignment path or expression")
			}
			write(indent+2, key+" = "+value)
		}
	}
	return nil
}
func line(value string) bool {
	return strings.TrimSpace(value) != "" && !strings.ContainsAny(value, "\r\n\x00")
}
func validPath(value string) bool {
	for _, part := range strings.Split(value, ".") {
		if part == "" {
			return false
		}
		for i, r := range part {
			letter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
			continuation := i > 0 && (r == '_' || (r >= '0' && r <= '9'))
			if !letter && !continuation {
				return false
			}
		}
	}
	return true
}
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
