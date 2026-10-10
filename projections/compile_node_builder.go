// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"cmp"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
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

// skipSubscription reports whether a model-bound child leaves the event to its own children or ancestors.
func (b *nodeBuilder) skipSubscription(sub subscription) bool {
	if !b.d.modelBound || b.parentFields == nil || b.nested {
		return false
	}
	if b.recursive {
		isCreator := slices.ContainsFunc(b.creators, func(s subscription) bool { return s.event.Ref() == sub.event.Ref() })
		return !isCreator && (b.c.ancestorCreators[sub.event.Ref()] > 0 || sub.keySet && !sub.parentSet)
	}
	return !sub.keySet && b.childEvents[sub.event.Ref()]
}

func (b *nodeBuilder) addSubscriptions() error {
	seen := map[events.TypeRef]bool{}
	for _, sub := range b.d.subscriptions {
		p := Provenance{FrontEnd: "typed", Directive: "FromEvent", Offset: -1, Event: sub.event.Ref()}
		if err := b.c.validateSubscription(sub); err != nil {
			return b.failure(p, err)
		}
		if b.skipSubscription(sub) {
			continue
		}
		if seen[sub.event.Ref()] {
			return b.failure(p, invalid("duplicate event subscription"))
		}
		seen[sub.event.Ref()] = true
		from := b.ensure(sub.event)
		if sub.keySet {
			from.key = sub.key
		}
		if sub.parentSet || sub.parent.kind != emptyExpression {
			from.parent = sub.parent
		}
		b.c.result.provenance = append(b.c.result.provenance, p)
		for _, w := range sub.writes {
			w.provenance.Event = sub.event.Ref()
			if err := addWrite(b.c.result, from, w, b.fields, sub.event.Fields(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *nodeBuilder) addDeclaredJoinsAndRemovals() error {
	for _, j := range b.d.joins {
		if err := b.c.addJoin(b.n, j, b.fields, false); err != nil {
			return err
		}
	}
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
		if !ok || field.Type != child.fieldType || strings.Contains(field.Path, ".") {
			return invalid("unknown or incompatible child field")
		}
		if err := b.c.compileChild(b.n, child, field, b.fields, nil); err != nil {
			return err
		}
	}
	return nil
}

func (b *nodeBuilder) addModelFields() error {
	for _, field := range serialization.RootFields(b.fields) {
		if err := b.addModelField(field); err != nil {
			return err
		}
	}
	return nil
}

func (b *nodeBuilder) addModelField(field serialization.Field) error {
	directives, err := declarations.Parse(declarations.V1, field.Tag)
	if err != nil {
		return err
	}
	if err = declarations.Validate(declarations.Model, directives); err != nil {
		return err
	}
	slices.SortStableFunc(directives, func(a, b declarations.Directive) int { return cmp.Compare(priority(a.Name), priority(b.Name)) })
	structural := hasDirective(directives, "children") || hasDirective(directives, "nested")
	if structural {
		if !b.d.modelBound {
			return invalid("model mapping tags and fluent mappings cannot be mixed")
		}
		if !b.stopExpanding {
			if err = b.c.boundChild(b.n, field, b.fields, directives); err != nil {
				return err
			}
		}
	}
	for _, directive := range directives {
		if err = b.applyDirective(field, directive, structural); err != nil {
			return err
		}
	}
	return b.checkEmbeddedDeclarations(field, structural)
}

func (b *nodeBuilder) applyDirective(field serialization.Field, directive declarations.Directive, structural bool) error {
	p := Provenance{FrontEnd: "model-bound", GoField: field.GoField, Path: field.Path, Directive: directive.Name, Offset: directive.Offset}
	switch directive.Name {
	case "children", "nested", "index", "subject", "pii", "compliance-details", "encrypted":
		return nil
	case "key":
		return b.applyKey(field, p)
	case "no-auto", "not-projected":
		if !slices.Contains(b.n.exclusions, field.Path) {
			b.n.exclusions = append(b.n.exclusions, field.Path)
		}
		b.c.result.provenance = append(b.c.result.provenance, p)
		return nil
	}
	if !b.d.modelBound {
		return b.failure(p, invalid("model mapping tags and fluent mappings cannot be mixed"))
	}
	if structural && (directive.Name == "remove" || directive.Name == "remove-join" || directive.Name == "clear") {
		return nil
	}
	if directive.Name == "every" || directive.Name == "all" {
		b.addGlobalDirective(field, directive, p)
		return nil
	}
	return b.applyEventDirective(field, directive, p)
}

func (b *nodeBuilder) applyKey(field serialization.Field, p Provenance) error {
	if b.n.keyField != "" || field.IsEnum() || !field.Scalar.IsPrimitive() || field.Nullable {
		return b.failure(p, invalid("one non-nullable scalar key field is required"))
	}
	b.n.keyField = field.Path
	b.c.result.provenance = append(b.c.result.provenance, p)
	return nil
}

func (b *nodeBuilder) addGlobalDirective(field serialization.Field, directive declarations.Directive, p Provenance) {
	e := expression{kind: pathExpression, text: field.Name}
	for _, a := range directive.Args {
		e.text = a.Value.Text
		if a.Name == "context" {
			e.kind = contextExpression
		}
	}
	global := globalDeclaration{write: write{path: field.Name, expression: e, provenance: p}, all: directive.Name == "all" && b.parentFields == nil, includeChildren: true}
	b.c.globals = append(b.c.globals, boundGlobal{globalDeclaration: global, fields: b.fields})
}

// leavesDirectiveToChild reports whether a child node ignores the directive because the parent owns its event.
func (b *nodeBuilder) leavesDirectiveToChild(directive declarations.Directive, event events.Descriptor) bool {
	if b.parentFields == nil || b.nested || b.recursive || !b.childEvents[event.Ref()] {
		return false
	}
	return priority(directive.Name) >= 1 && priority(directive.Name) <= 6 || directive.Name == "clear"
}

func (b *nodeBuilder) applyEventDirective(field serialization.Field, directive declarations.Directive, p Provenance) error {
	event, err := b.c.resolve(directive, p)
	if err != nil {
		return err
	}
	p.Event = event.Ref()
	if b.leavesDirectiveToChild(directive, event) {
		return nil
	}
	if directive.Name == "remove" || directive.Name == "remove-join" {
		if err = b.c.addRemoval(b.n, removalFromTag(directive, event), true); err != nil {
			return b.failure(p, err)
		}
		return nil
	}
	w := write{path: field.Path, expression: mappingExpression(directive, field.Name), provenance: p}
	if directive.Name == "join" {
		return b.applyJoinDirective(field, directive, event, w, p)
	}
	from := b.ensure(event)
	if a, ok := argument(directive, "key"); ok {
		if err = b.overrideKey(from, event, parsedKey(a), p); err != nil {
			return err
		}
	}
	return addWrite(b.c.result, from, w, b.fields, event.Fields(), true)
}

func (b *nodeBuilder) applyJoinDirective(field serialization.Field, directive declarations.Directive, event events.Descriptor, w write, p Provenance) error {
	on := field.Path
	if a, ok := argument(directive, "on"); ok {
		on = a.Text
	}
	s := newSubscription(event, nil)
	s.writes = []write{w}
	if err := b.c.addJoin(b.n, joinDeclaration{subscription: s, on: on}, b.fields, true); err != nil {
		return b.failure(p, err)
	}
	return nil
}

func (b *nodeBuilder) overrideKey(from *fromDefinition, event events.Descriptor, key expression, p Provenance) error {
	if err := validateKey(key, nil, event.Fields(), false); err != nil {
		return b.failure(p, err)
	}
	if from.key.encode() != key.encode() && from.key.kind != sourceExpression {
		b.c.result.diagnostics = append(b.c.result.diagnostics, Diagnostic{Message: "mapping correlation key overrides an earlier event key", Replacement: p})
	}
	from.key = key
	return nil
}

// checkEmbeddedDeclarations rejects declarations on descendants of a field that is not an explicit node boundary.
// A plain embedded object can be mapped as a whole, but declarations on
// its descendants require an explicit node boundary, never flattened tags.
func (b *nodeBuilder) checkEmbeddedDeclarations(field serialization.Field, structural bool) error {
	if structural {
		return nil
	}
	if b.n.children[field.Path] == nil && b.n.nested[field.Path] == nil {
		for _, f := range b.fields {
			if strings.HasPrefix(f.Path, field.Path+".") && hasProjectionDirective(f.Tag) {
				return declarationFailure(b.c.result.id, Provenance{GoField: f.GoField, Path: f.Path, Offset: 0}, invalid("nested declarations require children or nested"))
			}
		}
	}
	if b.n.children[field.Path] == nil {
		for _, f := range b.fields {
			if (f.Path == field.Path || strings.HasPrefix(f.Path, field.Path+".")) && derivativeHasDirective(f, derivedDeclarationDirective) {
				return declarationFailure(b.c.result.id, Provenance{GoField: f.GoField, Path: f.Path, Offset: -1}, invalid("declarations on a derived type require a children collection of its family"))
			}
		}
	}
	return nil
}

func (b *nodeBuilder) defaultIdentity() string {
	key := expression{kind: sourceExpression}
	if len(b.creators) > 0 {
		key = b.creators[0].key
	} else if len(b.d.subscriptions) > 0 {
		key = b.d.subscriptions[0].key
	}
	return discoverIdentity(b.fields, b.n.keyField, key)
}

func (b *nodeBuilder) hasExplicitIdentity() bool {
	for _, from := range b.froms {
		for _, w := range from.writes {
			if w.path == b.n.identifiedBy && (w.provenance.Directive == "set" || w.provenance.Directive == "context" || w.provenance.Directive == "value" || w.provenance.Directive == "add" || w.provenance.Directive == "subtract") {
				return true
			}
		}
	}
	return false
}

func (b *nodeBuilder) addChildIdentity() error {
	if b.parentFields == nil || b.nested {
		return nil
	}
	if b.n.identifiedBy == "" {
		b.n.identifiedBy = b.defaultIdentity()
	}
	if b.n.identifiedBy == "$eventSourceId" {
		return nil
	}
	identity, ok := binaryMappingField(b.fields, b.n.identifiedBy)
	if !ok || identity.Collection || identity.Nullable || !identity.Scalar.IsPrimitive() {
		return invalid("child identity requires a non-nullable scalar field")
	}
	explicitIdentity := b.hasExplicitIdentity()
	for _, creator := range b.creators {
		if err := b.addCreatorIdentity(creator, explicitIdentity); err != nil {
			return err
		}
	}
	return nil
}

func (b *nodeBuilder) addCreatorIdentity(creator subscription, explicitIdentity bool) error {
	n := b.n
	from := b.ensure(creator.event)
	if !b.d.modelBound || from.key.kind == compositeExpression || n.noAuto || explicitIdentity || hasWrite(from.writes, n.identifiedBy) || strings.EqualFold(n.identifiedBy, from.key.text) && n.keyField != n.identifiedBy {
		return nil
	}
	e := from.key
	if e.kind == sourceExpression {
		e = expression{kind: contextExpression, text: "EventSourceId"}
	}
	w := write{path: n.identifiedBy, expression: e, provenance: Provenance{FrontEnd: "convention", Path: n.identifiedBy, Directive: "child-identity", Offset: -1, Event: creator.event.Ref()}}
	return addWrite(b.c.result, from, w, b.fields, creator.event.Fields(), false)
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
