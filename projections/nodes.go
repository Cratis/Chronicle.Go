// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

func declarationSubscriptions(d *declaration) []subscription {
	result := append([]subscription(nil), d.subscriptions...)
	result = append(result, d.entering...)
	for _, j := range d.joins {
		result = append(result, j.subscription)
	}
	for _, r := range d.removals {
		result = append(result, r.subscription)
	}
	for _, child := range d.children {
		result = append(result, declarationSubscriptions(child.data)...)
	}
	for _, node := range d.nodes {
		result = append(result, declarationSubscriptions(node)...)
	}
	return result
}

type joinDeclaration struct {
	subscription subscription
	on           string
	onType       reflect.Type
}
type removalDeclaration struct {
	subscription subscription
	join, clear  bool
}
type globalDeclaration struct {
	write                write
	all, includeChildren bool
}
type childDeclaration struct {
	path           string
	fieldType, typ reflect.Type
	identifiedBy   string
	identityType   reflect.Type
	nested         bool
	data           *declaration
}

// NodeDeclaration is immutable type-level metadata for a child or nested object.
// It is not a standalone read model or projection and needs no separate registry.
type NodeDeclaration struct {
	typ  reflect.Type
	data *declaration
}

// NodeOption configures a node using FromEvent, NoAutoMap, RemovedWith,
// RemovedWithJoin or ClearWith. Root-only identity/settings are rejected.
type NodeOption = Option

// Node declares type-level options next to a child/nested Go struct. Its field
// tags are discovered only when reachable through children/nested declarations.
func Node[T any](options ...NodeOption) NodeDeclaration {
	d := newDeclaration(readmodels.Descriptor{}, options)
	d.modelBound = true
	return NodeDeclaration{typ: reflect.TypeFor[T](), data: cloneDeclaration(d)}
}

// WithNodes supplies explicitly registered node metadata. Duplicate types fail;
// reachable field tags need no Node declaration unless type-level options apply.
func WithNodes(nodes ...NodeDeclaration) Option {
	owned := append([]NodeDeclaration(nil), nodes...)
	return func(d *declaration) {
		if d.nodes == nil {
			d.nodes = map[reflect.Type]*declaration{}
		}
		for _, node := range owned {
			if node.typ == nil || node.typ.Kind() != reflect.Struct || node.data == nil || d.nodes[node.typ] != nil {
				d.err = invalid("invalid or duplicate node declaration")
				continue
			}
			d.nodes[node.typ] = node.data
		}
	}
}

// RemovedWith removes the containing instance when E arrives. The default key
// and parent key are event-source identity; options redirect either explicitly.
func RemovedWith[E any](event events.Type[E], options ...FromOption) Option {
	s := newSubscription(event.Descriptor(), options)
	if s.parent.kind == emptyExpression {
		s.parent.kind = sourceExpression
	}
	return func(d *declaration) { d.removals = append(d.removals, removalDeclaration{subscription: s}) }
}

// RemovedWithJoin preserves the distinct join-removal contract. The pinned kernel
// has no root/nested removal implementation. Its MongoDB sink cannot remove
// collection children identified by id/Id through a join; other identifier names
// work (https://github.com/Cratis/Chronicle/issues/4538). See the parity map.
func RemovedWithJoin[E any](event events.Type[E], options ...FromOption) Option {
	s := newSubscription(event.Descriptor(), options)
	return func(d *declaration) { d.removals = append(d.removals, removalDeclaration{subscription: s, join: true}) }
}

// ClearWith removes a nested single object. It is valid only on a nested Node or
// Nested builder; scalar fields use Clear instead. It is not root removal.
func ClearWith[E any](event events.Type[E]) NodeOption {
	s := newSubscription(event.Descriptor(), nil)
	return func(d *declaration) {
		d.removals = append(d.removals, removalDeclaration{subscription: s, clear: true})
	}
}

// ChildOption configures identity discovery for a collection node.
type ChildOption func(*childDeclaration)

// IdentifiedBy selects the child identity field instead of Key/Id conventions.
func IdentifiedBy[C, V any](field Field[C, V]) ChildOption {
	return func(c *childDeclaration) {
		c.identifiedBy, c.identityType = field.path, reflect.TypeFor[V]()
		if c.typ != field.owner {
			c.data.err = invalid("child identity owner mismatch")
		}
	}
}

// Children declares a collection node. V must be a slice/array of C or *C.
// Child From subscriptions stay child-scoped. AutoMap defaults to Inherit;
// parent keys and identity mappings require explicit fluent declarations.
// The callback runs once and is snapshotted, including all recursive builders.
func Children[M, C, V any](b *Builder[M], field Field[M, V], define func(*Builder[C]), options ...ChildOption) {
	child := &Builder[C]{data: newDeclaration(readmodels.Descriptor{}, nil)}
	if define != nil {
		define(child)
	}
	c := childDeclaration{path: field.path, fieldType: reflect.TypeFor[V](), typ: reflect.TypeFor[C](), data: child.data}
	for _, option := range options {
		if option == nil {
			c.data.err = invalid("nil child option")
		} else {
			option(&c)
		}
	}
	c.data = cloneDeclaration(c.data)
	b.data.children = append(b.data.children, c)
}

// Nested declares a nullable single-object node. ClearWith registers whole-object
// removal; the object's own From subscriptions do not create root From entries.
func Nested[M, N any](b *Builder[M], field Field[M, *N], define func(*Builder[N]), options ...NodeOption) {
	child := &Builder[N]{data: newDeclaration(readmodels.Descriptor{}, options)}
	if define != nil {
		define(child)
	}
	b.data.children = append(b.data.children, childDeclaration{path: field.path, fieldType: reflect.TypeFor[*N](), typ: reflect.TypeFor[N](), nested: true, data: cloneDeclaration(child.data)})
}

// Configure applies node options to a fluent builder. Settings are last-wins;
// subscriptions and removals retain the same duplicate rules as root options.
func (b *Builder[M]) Configure(options ...Option) {
	for _, option := range options {
		if option == nil {
			b.data.err = invalid("nil projection option")
		} else {
			option(b.data)
		}
	}
}

// Join enriches a node from an event, applied by the kernel after local mappings.
// on names a model field, not an event field. The callback uses the same typed
// mapping operations as From. Overlap with local writes produces diagnostics.
func Join[M, E, V any](b *Builder[M], event events.Type[E], on Field[M, V], define func(*FromBuilder[M, E]), options ...FromOption) {
	from := &FromBuilder[M, E]{subscription: newSubscription(event.Descriptor(), options), model: b.data.model}
	if define != nil {
		define(from)
	}
	from.subscription.writes = append([]write(nil), from.subscription.writes...)
	b.data.joins = append(b.data.joins, joinDeclaration{subscription: from.subscription, on: on.path, onType: reflect.TypeFor[V]()})
}
