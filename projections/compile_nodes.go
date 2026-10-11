// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

type compiler struct {
	result           *definition
	catalog          *events.Catalog
	declaration      *declaration
	globals          []boundGlobal
	usedNodes        map[reflect.Type]bool
	active           map[nodeIdentity]bool
	childDepth       int
	ancestorCreators map[events.TypeRef]int
}
type boundGlobal struct {
	globalDeclaration
	fields []serialization.Field
}

func (c *compiler) compileNode(d *declaration, fields, parentFields []serialization.Field, inheritedNoAuto, nested bool, identifiedBy string, creators []subscription, stopExpanding, recursive bool) (*nodeDefinition, error) {
	if err := validateNodeInputs(d, parentFields); err != nil {
		return nil, err
	}
	n := newNodeDefinition(d, parentFields != nil, inheritedNoAuto, nested, identifiedBy)
	childEvents, err := c.childEvents(fields)
	if err != nil {
		return nil, err
	}
	defer c.trackAncestorCreators(childEvents)()
	b := &nodeBuilder{c: c, d: d, n: n, fields: fields, parentFields: parentFields, creators: creators, childEvents: childEvents,
		froms: map[events.TypeRef]*fromDefinition{}, nested: nested, stopExpanding: stopExpanding, recursive: recursive}
	if err := b.build(); err != nil {
		return nil, err
	}
	return n, nil
}

func validateNodeInputs(d *declaration, parentFields []serialization.Field) error {
	if d.err != nil {
		return d.err
	}
	if parentFields != nil && hasRootOnlyState(d) {
		return invalid("initial values and labels belong to the root projection")
	}
	return nil
}

func hasRootOnlyState(d *declaration) bool {
	return d.initialState != "" || len(d.labels) != 0
}

func newNodeDefinition(d *declaration, isChild, inheritedNoAuto, nested bool, identifiedBy string) *nodeDefinition {
	n := &nodeDefinition{noAuto: inheritedNoAuto || d.noAuto, ownNoAuto: d.noAuto, inheritAuto: isChild && !d.modelBound && !d.autoSet, identifiedBy: identifiedBy, children: map[string]*nodeDefinition{}, nested: map[string]*nodeDefinition{}}
	if !d.modelBound {
		n.noAuto = d.noAuto
	}
	if nested {
		n.identifiedBy = "*NotSet*"
	}
	return n
}

// trackAncestorCreators counts the child events as ancestor creators and returns the function that undoes it.
func (c *compiler) trackAncestorCreators(childEvents map[events.TypeRef]bool) func() {
	for ref := range childEvents {
		c.ancestorCreators[ref]++
	}
	return func() {
		for ref := range childEvents {
			c.ancestorCreators[ref]--
		}
	}
}

func (b *nodeBuilder) build() error {
	for _, stage := range []func() error{
		b.addCreators, b.addSubscriptions, b.addDeclaredJoinsAndRemovals, b.addDeclaredChildren,
		b.addModelFields, b.addChildIdentity, b.collectFroms,
	} {
		if err := stage(); err != nil {
			return err
		}
	}
	b.sortNode()
	if err := b.addGlobals(); err != nil {
		return err
	}
	b.diagnoseRootJoinRemovals()
	b.diagnoseJoinOverrides()
	return nil
}

func (c *compiler) validateSubscription(s subscription) error {
	if s.err != nil {
		return s.err
	}
	if err := validateEvent(c.catalog, s.event); err != nil {
		return err
	}
	if err := validateKey(s.key, s.keyType, s.event.Fields(), false); err != nil {
		return err
	}
	return validateKey(s.parent, s.parentType, s.event.Fields(), true)
}

func (c *compiler) resolve(d declarations.Directive, p Provenance) (events.Descriptor, error) {
	event, err := resolveEvent(c.catalog, c.declaration.aliases, d.Args[0].Value)
	if err != nil {
		failure := declarationFailure(c.result.id, p, err)
		failure.EventReference = referenceName(d.Args[0].Value)
		return events.Descriptor{}, failure
	}
	return event, nil
}

func (c *compiler) addRemoval(n *nodeDefinition, r removalDeclaration, overwrite bool) error {
	if err := c.validateSubscription(r.subscription); err != nil {
		return err
	}
	if r.join && r.subscription.parent.kind != emptyExpression {
		return invalid("join removal has no parent key")
	}
	entry := removalDefinition{event: r.subscription.event.Ref(), key: r.subscription.key, parent: r.subscription.parent, join: r.join}
	for i, previous := range n.removals {
		if previous.event == entry.event && previous.join == entry.join {
			if !overwrite {
				return invalid("duplicate removal event")
			}
			n.removals[i] = entry
			return nil
		}
	}
	n.removals = append(n.removals, entry)
	return nil
}

func validateNodeOptions(d *declaration) error {
	if d.err != nil {
		return d.err
	}
	if hasRootOnlyOption(d) {
		return invalid("root-only option on a node; register aliases and WithNodes on the root")
	}
	return nil
}

func hasRootOnlyOption(d *declaration) bool {
	return hasRootIdentityOption(d) || hasRootBehaviorOption(d) || hasRootRegistration(d)
}

func hasRootIdentityOption(d *declaration) bool {
	return d.id != "" || d.variant != nil || d.variantKey != ""
}

func hasRootBehaviorOption(d *declaration) bool {
	return d.passive || d.notRewindable || d.sequence != events.EventLog || d.sequenceExplicit || d.globalFor != nil
}

func hasRootRegistration(d *declaration) bool {
	return len(d.entering) > 0 || len(d.nodes) > 0 || len(d.aliases) > 0
}

// findRootField returns the first root field matching the predicate.
func findRootField(fields []serialization.Field, match func(serialization.Field) bool) (serialization.Field, bool) {
	for _, f := range serialization.RootFields(fields) {
		if match(f) {
			return f, true
		}
	}
	return serialization.Field{}, false
}

func isIDField(f serialization.Field) bool {
	return strings.EqualFold(lastGoName(f.GoField), "Id")
}

func discoverIdentity(fields []serialization.Field, keyField string, key expression) string {
	if keyField != "" {
		return keyField
	}
	if f, ok := findRootField(fields, isIDField); ok {
		return f.Path
	}
	if key.kind == pathExpression {
		if f, ok := findRootField(fields, func(f serialization.Field) bool { return strings.EqualFold(f.Name, key.text) }); ok {
			return f.Path
		}
	}
	return "$eventSourceId"
}
func lastGoName(path string) string { parts := strings.Split(path, "."); return parts[len(parts)-1] }
func hasDirective(ds []declarations.Directive, name string) bool {
	return slices.ContainsFunc(ds, func(d declarations.Directive) bool { return d.Name == name })
}
func argument(d declarations.Directive, name string) (declarations.Value, bool) {
	for _, a := range d.Args {
		if a.Name == name {
			return a.Value, true
		}
	}
	return declarations.Value{}, false
}
func hasWrite(writes []write, path string) bool {
	return slices.ContainsFunc(writes, func(w write) bool { return w.path == path })
}

// anyChildNode reports whether the predicate holds for any child or nested node.
func anyChildNode(n *nodeDefinition, match func(*nodeDefinition) bool) bool {
	for _, child := range n.children {
		if match(child) {
			return true
		}
	}
	for _, child := range n.nested {
		if match(child) {
			return true
		}
	}
	return false
}

func nodeUsesEvent(n *nodeDefinition, event events.TypeRef) bool {
	if slices.ContainsFunc(n.from, func(f fromDefinition) bool { return f.event == event }) || slices.ContainsFunc(n.joins, func(j joinDefinition) bool { return j.event == event }) {
		return true
	}
	return anyChildNode(n, func(child *nodeDefinition) bool { return nodeUsesEvent(child, event) })
}

func hasSubscriptions(n *nodeDefinition) bool {
	if len(n.from)+len(n.joins)+len(n.removals) > 0 {
		return true
	}
	return anyChildNode(n, hasSubscriptions)
}

var mappingKinds = map[string]expressionKind{
	"add":       addExpression,
	"subtract":  subtractExpression,
	"increment": incrementExpression,
	"decrement": decrementExpression,
	"count":     countExpression,
	"clear":     nullExpression,
	"context":   contextExpression,
}

func mappingExpression(d declarations.Directive, target string) expression {
	e := expression{kind: pathExpression, text: target}
	if from, ok := argument(d, "from"); ok {
		e.text = from.Text
	}
	if kind, ok := mappingKinds[d.Name]; ok {
		e.kind = kind
	} else if d.Name == "value" {
		v, _ := argument(d, "value")
		e = literalValue(v)
	}
	return e
}
func removalFromTag(d declarations.Directive, event events.Descriptor) removalDeclaration {
	s := newSubscription(event, nil)
	if a, ok := argument(d, "key"); ok {
		s.key = parsedKey(a)
	}
	if d.Name == "remove" {
		s.parent.kind = sourceExpression
	}
	if a, ok := argument(d, "parent-key"); ok {
		s.parent = parsedKey(a)
	}
	return removalDeclaration{subscription: s, join: d.Name == "remove-join", clear: d.Name == "clear"}
}
