// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

// nodeBuilder holds the state shared by the stages that compile one node.
type nodeBuilder struct {
	c             *compiler
	d             *declaration
	n             *nodeDefinition
	fields        []serialization.Field
	parentFields  []serialization.Field
	creators      []subscription
	childEvents   map[events.TypeRef]bool
	froms         map[events.TypeRef]*fromDefinition
	nested        bool
	stopExpanding bool
	recursive     bool
}

func (b *nodeBuilder) ensure(event events.Descriptor) *fromDefinition {
	ref := event.Ref()
	if b.froms[ref] == nil {
		b.froms[ref] = &fromDefinition{event: ref, key: expression{kind: sourceExpression}}
	}
	return b.froms[ref]
}

func (b *nodeBuilder) failure(p Provenance, err error) error {
	return declarationFailure(b.c.result.id, p, err)
}

func (b *nodeBuilder) addCreators() error {
	for _, s := range b.creators {
		if err := b.c.validateSubscription(s); err != nil {
			return err
		}
		from := b.ensure(s.event)
		from.key, from.parent = s.key, s.parent
	}
	return nil
}

// isModelBoundChild reports whether the node is a model-bound child that may leave events to other nodes.
func (b *nodeBuilder) isModelBoundChild() bool {
	return b.d.modelBound && b.parentFields != nil && !b.nested
}

func (b *nodeBuilder) isCreatorEvent(ref events.TypeRef) bool {
	return slices.ContainsFunc(b.creators, func(s subscription) bool { return s.event.Ref() == ref })
}

// skipRecursive reports whether a recursive child leaves the event to an ancestor.
func (b *nodeBuilder) skipRecursive(sub subscription) bool {
	ref := sub.event.Ref()
	if b.isCreatorEvent(ref) {
		return false
	}
	return b.c.ancestorCreators[ref] > 0 || sub.keySet && !sub.parentSet
}

// skipSubscription reports whether a model-bound child leaves the event to its own children or ancestors.
func (b *nodeBuilder) skipSubscription(sub subscription) bool {
	if !b.isModelBoundChild() {
		return false
	}
	if b.recursive {
		return b.skipRecursive(sub)
	}
	return !sub.keySet && b.childEvents[sub.event.Ref()]
}

func (b *nodeBuilder) addSubscriptions() error {
	seen := map[events.TypeRef]bool{}
	for _, sub := range b.d.subscriptions {
		if err := b.addSubscription(sub, seen); err != nil {
			return err
		}
	}
	return nil
}

func (b *nodeBuilder) addSubscription(sub subscription, seen map[events.TypeRef]bool) error {
	p := Provenance{FrontEnd: "typed", Directive: "FromEvent", Offset: -1, Event: sub.event.Ref()}
	if err := b.c.validateSubscription(sub); err != nil {
		return b.failure(p, err)
	}
	if b.skipSubscription(sub) {
		return nil
	}
	if seen[sub.event.Ref()] {
		return b.failure(p, invalid("duplicate event subscription"))
	}
	seen[sub.event.Ref()] = true
	from := b.ensure(sub.event)
	applySubscriptionKeys(from, sub)
	b.c.result.provenance = append(b.c.result.provenance, p)
	return b.addSubscriptionWrites(from, sub)
}

func applySubscriptionKeys(from *fromDefinition, sub subscription) {
	if sub.keySet {
		from.key = sub.key
	}
	if sub.parentSet || sub.parent.kind != emptyExpression {
		from.parent = sub.parent
	}
}

func (b *nodeBuilder) addSubscriptionWrites(from *fromDefinition, sub subscription) error {
	for _, w := range sub.writes {
		w.provenance.Event = sub.event.Ref()
		if err := addWrite(b.c.result, from, w, b.fields, sub.event.Fields(), false); err != nil {
			return err
		}
	}
	return nil
}

func (b *nodeBuilder) addDeclaredJoinsAndRemovals() error {
	if err := b.addDeclaredJoins(); err != nil {
		return err
	}
	return b.addDeclaredRemovals()
}

func (b *nodeBuilder) addDeclaredJoins() error {
	for _, j := range b.d.joins {
		if err := b.c.addJoin(b.n, j, b.fields, false); err != nil {
			return err
		}
	}
	return nil
}

func (b *nodeBuilder) addDeclaredRemovals() error {
	for _, r := range b.d.removals {
		if r.clear && !b.nested {
			return invalid("ClearWith requires a nested object node")
		}
		if err := b.c.addRemoval(b.n, r, b.d.modelBound); err != nil {
			return err
		}
	}
	return nil
}

func (b *nodeBuilder) addDeclaredChildren() error {
	for _, child := range b.d.children {
		field, ok := binaryMappingField(b.fields, child.path)
		if !isCompatibleChildField(field, ok, child) {
			return invalid("unknown or incompatible child field")
		}
		if err := b.c.compileChild(b.n, childSpec{child: child, field: field, fields: b.fields}); err != nil {
			return err
		}
	}
	return nil
}

func isCompatibleChildField(field serialization.Field, found bool, child childDeclaration) bool {
	return found && field.Type == child.fieldType && !strings.Contains(field.Path, ".")
}

func (b *nodeBuilder) collectFroms() error {
	for _, from := range b.froms {
		if b.c.result.passive && from.key.kind != sourceExpression {
			return invalid("passive key redirection is not supported by immediate instance reads")
		}
		slices.SortFunc(from.writes, func(a, b write) int { return strings.Compare(a.path, b.path) })
		b.n.from = append(b.n.from, *from)
	}
	return nil
}

func (b *nodeBuilder) sortNode() {
	n := b.n
	slices.SortFunc(n.from, func(a, b fromDefinition) int { return compareEvent(a.event, b.event) })
	slices.SortFunc(n.joins, func(a, b joinDefinition) int { return compareEvent(a.event, b.event) })
	slices.SortFunc(n.removals, compareRemovals)
	slices.Sort(n.exclusions)
}

func compareRemovals(a, b removalDefinition) int {
	if v := compareEvent(a.event, b.event); v != 0 {
		return v
	}
	if a.join == b.join {
		return 0
	}
	if a.join {
		return 1
	}
	return -1
}

func (b *nodeBuilder) addGlobals() error {
	for _, g := range b.d.globals {
		if b.parentFields != nil {
			g.all = false
		}
		if err := b.c.addGlobal(b.n, g, b.fields, false); err != nil {
			return err
		}
	}
	return nil
}

func (b *nodeBuilder) diagnoseRootJoinRemovals() {
	if b.parentFields != nil {
		return
	}
	for _, removal := range b.n.removals {
		if removal.join {
			b.c.result.diagnostics = append(b.c.result.diagnostics, Diagnostic{Message: "kernel ignores root RemovedWithJoin: root removal via join is not supported (Chronicle#4263)", Replacement: Provenance{Directive: "RemovedWithJoin", Event: removal.event, Offset: -1}})
		}
	}
}

func (b *nodeBuilder) diagnoseJoinOverrides() {
	for _, join := range b.n.joins {
		for _, w := range join.writes {
			b.diagnoseJoinWrite(w)
		}
	}
}

func (b *nodeBuilder) diagnoseJoinWrite(w write) {
	for _, from := range b.n.from {
		for _, local := range from.writes {
			if w.path == local.path {
				b.c.result.diagnostics = append(b.c.result.diagnostics, Diagnostic{Message: "join is applied after local mappings and overrides this write", Previous: local.provenance, Replacement: w.provenance})
			}
		}
	}
}
