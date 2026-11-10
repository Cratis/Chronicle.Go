// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"

	"github.com/cratis/chronicle.go/serialization"
)

// CausedBy assigns an identity property of the user who caused the event with
// the kernel's $causedBy(...) expression. The target must be an unformatted string.
func CausedBy[M, E, V any](b *FromBuilder[M, E], target Field[M, V], property CausedByProperty) {
	b.add(target.path, reflect.TypeFor[V](), nil, expression{kind: causedByExpression, text: string(property)}, "caused-by")
}

// EveryCausedBy maps an identity property of the causing user for Every or All.
func EveryCausedBy[M, V any](b *EveryBuilder[M], target Field[M, V], property CausedByProperty) {
	b.add(target.path, reflect.TypeFor[V](), expression{kind: causedByExpression, text: string(property)})
}

// MapOnce is Map wrapped in the kernel's $setOnce(...): the event value is only
// assigned while the target (or dictionary entry) has no value yet.
func MapOnce[M, E, V any](b *FromBuilder[M, E], target Field[M, V], source Field[E, V]) {
	b.add(target.path, reflect.TypeFor[V](), reflect.TypeFor[V](), expression{kind: pathExpression, text: source.path, once: true}, "set-once")
}

// MapAsOnce is MapAs wrapped in the kernel's $setOnce(...).
func MapAsOnce[M, E, T, S any](b *FromBuilder[M, E], target Field[M, T], source Field[E, S]) {
	b.add(target.path, reflect.TypeFor[T](), reflect.TypeFor[S](), expression{kind: pathExpression, text: source.path, once: true}, "set-once")
}

// ContextOnce is Context wrapped in the kernel's $setOnce(...), for example to
// keep the first occurred timestamp.
func ContextOnce[M, E, V any](b *FromBuilder[M, E], target Field[M, V], path string) {
	b.add(target.path, reflect.TypeFor[V](), nil, expression{kind: contextExpression, text: path, once: true}, "set-once")
}

// CausedByOnce is CausedBy wrapped in the kernel's $setOnce(...), for example to
// keep the user who created an instance.
func CausedByOnce[M, E, V any](b *FromBuilder[M, E], target Field[M, V], property CausedByProperty) {
	b.add(target.path, reflect.TypeFor[V](), nil, expression{kind: causedByExpression, text: string(property), once: true}, "set-once")
}

// EventSourceIDOnce is EventSourceID wrapped in the kernel's $setOnce(...).
func EventSourceIDOnce[M, E, V any](b *FromBuilder[M, E], target Field[M, V]) {
	b.add(target.path, reflect.TypeFor[V](), nil, expression{kind: sourceExpression, once: true}, "set-once")
}

// ValueOnce is Value wrapped in the kernel's $setOnce(...). A nil value is
// rejected: setting null once has no effect.
func ValueOnce[M, E, V any](b *FromBuilder[M, E], target Field[M, V], value V) {
	Value(b, target, value)
	last := &b.subscription.writes[len(b.subscription.writes)-1]
	last.expression.once, last.provenance.Directive = true, "set-once"
}

// EveryMapOnce is EveryMap wrapped in the kernel's $setOnce(...).
func EveryMapOnce[M, V any](b *EveryBuilder[M], target Field[M, V], source string) {
	b.add(target.path, reflect.TypeFor[V](), expression{kind: pathExpression, text: source, once: true})
}

// EveryContextOnce is EveryContext wrapped in the kernel's $setOnce(...).
func EveryContextOnce[M, V any](b *EveryBuilder[M], target Field[M, V], path string) {
	b.add(target.path, reflect.TypeFor[V](), expression{kind: contextExpression, text: path, once: true})
}

// settableOnce reports whether the kernel resolves the expression as an event
// value provider, the only operand $setOnce(...) accepts.
func settableOnce(e expression) bool {
	switch e.kind {
	case pathExpression, contextExpression, sourceExpression, literalExpression, causedByExpression:
		return true
	default:
		return false
	}
}

func validateCausedBy(property string, target serialization.Field) error {
	if !CausedByProperty(property).valid() {
		return invalid("caused-by property must be subject, name or userName")
	}
	if target.IsEnum() || target.Scalar != serialization.String || target.Format != "" {
		return invalid("caused-by requires an unformatted string target")
	}
	return nil
}
