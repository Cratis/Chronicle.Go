// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package captures authors explicit capture declarations for the kernel. There
// is no automatic discovery, registration or local capture worker. The kernel
// currently runs only API sources and root append rules; validation reports
// unsupported language features before activation. Source authorization is
// authorable but cannot be submitted through the CDL-only transport.
package captures

import (
	"slices"
	"strings"

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
// safe for concurrent use. Formatting/logging is redacted; Declaration explicitly
// exports CDL only. JSON import/export fails with ErrUnsupported.
type Definition struct {
	id            uuid.UUID
	text          string
	authorization SourceAuthorization
}

// ID returns this declaration's identity; repeated saves update this capture.
func (d Definition) ID() uuid.UUID { return d.id }

// Declaration returns the frozen CDL text. No credentials are embedded in it.
func (d Definition) Declaration() string { return d.text }

type nested struct {
	path, key string
	scope     Scope
}

// Builder is a local mutable authoring builder, not safe for concurrent mutation.
// Source, key and Map are last-wins; append/nested/children accumulate. Build
// validates atomically and returns a frozen snapshot. Zero is usable. Formatting
// and logging are redacted; JSON import/export fails with ErrUnsupported.
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
// Invalid webhook authorization fails with ErrInvalidConfiguration. Authorization
// is retained outside CDL; authorized definitions cannot yet be submitted.
// Build does not claim runtime support for every authorable language construct.
func (b *Builder) Build(name string) (Definition, error) {
	if b != nil && b.source.invalidAuthorization {
		return Definition{}, invalid("invalid capture source authorization configuration")
	}
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
	return Definition{id: id, text: out.String(), authorization: b.source.authorization}, nil
}
