// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import "reflect"

// Add adds a numeric event field to the model's running value. Concept-backed
// numbers use their underlying scalar representation; no client-side fold runs.
func Add[M, E, T, S any](b *FromBuilder[M, E], target Field[M, T], source Field[E, S]) {
	b.add(target.path, reflect.TypeFor[T](), reflect.TypeFor[S](), expression{kind: addExpression, text: source.path}, "add")
}

// Subtract subtracts a numeric event field from the model's running value.
func Subtract[M, E, T, S any](b *FromBuilder[M, E], target Field[M, T], source Field[E, S]) {
	b.add(target.path, reflect.TypeFor[T](), reflect.TypeFor[S](), expression{kind: subtractExpression, text: source.path}, "subtract")
}

// Increment emits the kernel's $increment expression for a numeric property.
func Increment[M, E, V any](b *FromBuilder[M, E], target Field[M, V]) {
	b.add(target.path, reflect.TypeFor[V](), nil, expression{kind: incrementExpression}, "increment")
}

// Decrement emits the kernel's $decrement expression for a numeric property.
func Decrement[M, E, V any](b *FromBuilder[M, E], target Field[M, V]) {
	b.add(target.path, reflect.TypeFor[V](), nil, expression{kind: decrementExpression}, "decrement")
}

// Count emits $count, not a client-side or rewritten increment operation.
func Count[M, E, V any](b *FromBuilder[M, E], target Field[M, V]) {
	b.add(target.path, reflect.TypeFor[V](), nil, expression{kind: countExpression}, "count")
}

// Clear assigns null to a nullable scalar pointer. Collection clears are rejected
// because collection serialization normalizes nil; use RemovedWith for children.
func Clear[M, E, V any](b *FromBuilder[M, E], target Field[M, V]) {
	b.add(target.path, reflect.TypeFor[V](), nil, expression{kind: nullExpression}, "clear")
}

// EveryBuilder authors mappings shared by already-subscribed events. It does not
// represent contract FromEvery derivative groups (owned by the derivative slice).
type EveryBuilder[M any] struct {
	writes          []write
	includeChildren bool
}

// IncludeChildren controls the stored child-inclusion flag (default true).
// The pinned kernel does not run parent Every mappings for child-only events.
func (b *EveryBuilder[M]) IncludeChildren(include bool) { b.includeChildren = include }

// Every maps only existing subscriptions and never broadens event discovery.
func Every[M any](b *Builder[M], define func(*EveryBuilder[M])) { global(b, false, define) }

// All maps every event and sets SubscribesToAllEvents. Every and All merge into
// one contract All definition; duplicate fluent target writes are rejected.
func All[M any](b *Builder[M], define func(*EveryBuilder[M])) { global(b, true, define) }

func global[M any](b *Builder[M], all bool, define func(*EveryBuilder[M])) {
	g := &EveryBuilder[M]{includeChildren: true}
	if define == nil {
		b.data.err = invalid("global mapping callback required")
		return
	}
	define(g)
	for _, w := range g.writes {
		b.data.globals = append(b.data.globals, globalDeclaration{write: w, all: all, includeChildren: g.includeChildren})
	}
}

// EveryMap maps an exact serialized payload path. Events lacking that property
// retain the kernel's missing-value behavior; no new event type is subscribed.
func EveryMap[M, V any](b *EveryBuilder[M], target Field[M, V], source string) {
	b.writes = append(b.writes, write{path: target.path, targetType: reflect.TypeFor[V](), expression: expression{kind: pathExpression, text: source}, provenance: Provenance{FrontEnd: "fluent", Path: target.path, Directive: "every", Offset: -1}})
}

// EveryContext maps a scalar kernel EventContext property for Every or All.
func EveryContext[M, V any](b *EveryBuilder[M], target Field[M, V], path string) {
	b.writes = append(b.writes, write{path: target.path, targetType: reflect.TypeFor[V](), expression: expression{kind: contextExpression, text: path}, provenance: Provenance{FrontEnd: "fluent", Path: target.path, Directive: "every", Offset: -1}})
}
