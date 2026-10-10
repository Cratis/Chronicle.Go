// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
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
	if d.err != nil {
		return nil, d.err
	}
	if parentFields != nil && (d.initialState != "" || len(d.labels) != 0) {
		return nil, invalid("initial values and labels belong to the root projection")
	}
	n := &nodeDefinition{noAuto: inheritedNoAuto || d.noAuto, ownNoAuto: d.noAuto, inheritAuto: parentFields != nil && !d.modelBound && !d.autoSet, identifiedBy: identifiedBy, children: map[string]*nodeDefinition{}, nested: map[string]*nodeDefinition{}}
	if !d.modelBound {
		n.noAuto = d.noAuto
	}
	if nested {
		n.identifiedBy = "*NotSet*"
	}
	childEvents, err := c.childEvents(fields)
	if err != nil {
		return nil, err
	}
	for ref := range childEvents {
		c.ancestorCreators[ref]++
	}
	defer func() {
		for ref := range childEvents {
			c.ancestorCreators[ref]--
		}
	}()
	b := &nodeBuilder{c: c, d: d, n: n, fields: fields, parentFields: parentFields, creators: creators, childEvents: childEvents,
		froms: map[events.TypeRef]*fromDefinition{}, nested: nested, stopExpanding: stopExpanding, recursive: recursive}
	if err := b.build(); err != nil {
		return nil, err
	}
	return n, nil
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

func (c *compiler) addJoin(n *nodeDefinition, j joinDeclaration, fields []serialization.Field, overwrite bool) error {
	if err := c.validateSubscription(j.subscription); err != nil {
		return err
	}
	if j.subscription.parent.kind != emptyExpression {
		return invalid("join does not support a parent key")
	}
	on, ok := binaryMappingField(fields, j.on)
	if ok && binaryField(on) {
		return binaryUnsupported("binary correlation keys are not supported")
	}
	if !ok || on.IsEnum() || validateTarget(on, j.onType) != nil || !on.Scalar.IsPrimitive() {
		return invalid("join on requires a scalar model field")
	}
	var target *joinDefinition
	for i := range n.joins {
		if n.joins[i].event == j.subscription.event.Ref() {
			if !overwrite {
				return invalid("duplicate fluent join event")
			}
			target = &n.joins[i]
			if target.on != j.on {
				c.result.diagnostics = append(c.result.diagnostics, Diagnostic{Message: "first model-bound join On wins for this event", Replacement: Provenance{Path: j.on, Directive: "join", Event: target.event, Offset: -1}})
			}
			break
		}
	}
	if target == nil {
		n.joins = append(n.joins, joinDefinition{on: j.on, fromDefinition: fromDefinition{event: j.subscription.event.Ref(), key: j.subscription.key}})
		target = &n.joins[len(n.joins)-1]
	}
	for _, w := range j.subscription.writes {
		w.provenance.Event = j.subscription.event.Ref()
		if err := addWrite(c.result, &target.fromDefinition, w, fields, j.subscription.event.Fields(), overwrite); err != nil {
			return err
		}
	}
	return nil
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

func (c *compiler) addGlobal(n *nodeDefinition, g globalDeclaration, fields []serialization.Field, overwrite bool) error {
	w := g.write
	target, ok := binaryMappingField(fields, w.path)
	if !ok || validateTarget(target, w.targetType) != nil {
		return declarationFailure(c.result.id, w.provenance, invalid("unknown global target field"))
	}
	if target.IsEnum() && g.all {
		return declarationFailure(c.result.id, w.provenance, invalid("all-event enum mappings cannot validate unknown event profiles"))
	}
	if w.expression.kind == pathExpression {
		if !eventPropertyPath(w.expression.text) {
			return declarationFailure(c.result.id, w.provenance, invalid("unrepresentable global event path"))
		}
		// C# accepts sparse event fields; validate known representations without
		// requiring every event in the catalog to declare the property.
		for _, event := range c.catalog.Descriptors() {
			if !g.all && !nodeUsesEvent(n, event.Ref()) {
				continue
			}
			if source, exists := binaryMappingField(event.Fields(), w.expression.text); exists && !scalarCompatible(target, source, fields, event.Fields()) {
				return declarationFailure(c.result.id, w.provenance, invalid("incompatible global event property"))
			}
		}
	} else if err := validateExpression(w.expression, target, fields, nil, nil); err != nil {
		return declarationFailure(c.result.id, w.provenance, err)
	}
	if len(n.all) > 0 && n.includeChildren != g.includeChildren {
		return invalid("global mappings disagree on IncludeChildren")
	}
	n.includeChildren = g.includeChildren
	c.result.subscribesAll = c.result.subscribesAll || g.all
	return mergeWrite(c.result, &n.all, w, overwrite)
}

func (c *compiler) boundChild(n *nodeDefinition, field serialization.Field, fields []serialization.Field, directives []declarations.Directive) error {
	nested := hasDirective(directives, "nested")
	if nested && hasDirective(directives, "children") {
		return invalid("field cannot be both children and nested")
	}
	shape, err := childShape(field, nested)
	if err != nil {
		return err
	}
	typ := shape.typ
	d := newDeclaration(readmodels.Descriptor{}, nil)
	d.modelBound = true
	if registered := c.declaration.nodes[typ]; registered != nil {
		d = cloneDeclaration(registered)
		c.usedNodes[typ] = true
	}
	child := childDeclaration{path: field.Path, fieldType: field.Type, typ: typ, nested: nested, data: d}
	var creators []subscription
	for _, directive := range directives {
		p := Provenance{FrontEnd: "model-bound", GoField: field.GoField, Path: field.Path, Directive: directive.Name, Offset: directive.Offset}
		switch directive.Name {
		case "children", "clear", "remove", "remove-join":
			event, err := c.resolve(directive, p)
			if err != nil {
				return err
			}
			if directive.Name == "children" {
				s := newSubscription(event, nil)
				if a, ok := argument(directive, "key"); ok {
					s.key = parsedKey(a)
				}
				if a, ok := argument(directive, "parent-key"); ok {
					s.parent = parsedKey(a)
				} else {
					s.parent = c.inferParent(fields, event.Fields(), s.key, p)
				}
				if a, ok := argument(directive, "identified-by"); ok {
					if child.identifiedBy != "" && child.identifiedBy != a.Text {
						return invalid("children disagree on identity")
					}
					child.identifiedBy = a.Text
				}
				creators = append(creators, s)
			} else {
				if directive.Name == "clear" && !nested {
					return invalid("collection clear cannot represent null; remove keyed children instead")
				}
				d.removals = append(d.removals, removalFromTag(directive, event))
			}
		}
	}
	return c.compileChild(n, child, field, fields, creators)
}

func (c *compiler) compileChild(n *nodeDefinition, child childDeclaration, field serialization.Field, fields []serialization.Field, creators []subscription) error {
	shape, err := childShape(field, child.nested)
	if err != nil {
		directive := "children"
		if child.nested {
			directive = "nested"
		}
		return declarationFailure(c.result.id, Provenance{GoField: field.GoField, Path: field.Path, Directive: directive, Offset: -1}, err)
	}
	typ := shape.typ
	if typ != child.typ || n.children[field.Path] != nil || n.nested[field.Path] != nil {
		return invalid("duplicate or incompatible child node")
	}
	if err := validateNodeOptions(child.data); err != nil {
		return err
	}
	local := shape.fields
	if child.identifiedBy != "" && child.identityType != nil {
		identity, ok := binaryMappingField(local, child.identifiedBy)
		if !ok || identity.Type != child.identityType {
			return invalid("child identity descriptor has wrong type")
		}
	}
	// Only model-bound ChildrenFrom declarations supply creators for parent
	// inference and identity mappings. Fluent From retains its empty parent key.
	d := cloneDeclaration(child.data)
	if !d.modelBound {
		if registered := c.declaration.nodes[typ]; registered != nil {
			if err := validateNodeOptions(registered); err != nil {
				return err
			}
			c.usedNodes[typ] = true
			if !d.autoSet && registered.autoSet {
				d.noAuto, d.autoSet = registered.noAuto, true
			}
			d.subscriptions = append(slices.Clone(registered.subscriptions), d.subscriptions...)
			d.removals = append(slices.Clone(registered.removals), d.removals...)
		}
	}
	// A derived child is identified by its family and selected concrete type, so
	// an ordinary node of the same concrete type is not mistaken for recursion.
	identity := nodeIdentity{typ: typ}
	if shape.derivative != nil {
		identity.family = field.Type.Elem()
	}
	stopExpanding := c.active[identity] && d.modelBound
	// C# seeds traversal with the root type, but the first child level always
	// uses the normal filter (includeSelfReferencingEvents: false). A root
	// collection of its own type stops here without using the recursive filter.
	recursive := stopExpanding && c.childDepth > 0
	if !c.active[identity] {
		c.active[identity] = true
		defer delete(c.active, identity)
	}
	c.childDepth++
	defer func() { c.childDepth-- }()
	compiled, err := c.compileNode(d, local, fields, n.ownNoAuto, child.nested, child.identifiedBy, creators, stopExpanding, recursive)
	if err != nil {
		return err
	}
	if shape.derivative != nil {
		compiled.derivative = shape.derivative
		if err := c.stampDerivedChild(compiled); err != nil {
			return err
		}
	}
	if child.nested {
		n.nested[field.Path] = compiled
	} else {
		n.children[field.Path] = compiled
	}
	return nil
}

func validateNodeOptions(d *declaration) error {
	if d.err != nil {
		return d.err
	}
	if d.id != "" || d.passive || d.notRewindable || d.sequence != events.EventLog || d.sequenceExplicit || d.variant != nil || d.globalFor != nil || len(d.entering) > 0 || d.variantKey != "" || len(d.nodes) > 0 || len(d.aliases) > 0 {
		return invalid("root-only option on a node; register aliases and WithNodes on the root")
	}
	return nil
}

type nodeIdentity struct{ typ, family reflect.Type }

// nodeShape is the node type and local fields of a child or nested field. For
// a derived-type family element, derivative is the selected concrete type.
type nodeShape struct {
	typ        reflect.Type
	fields     []serialization.Field
	derivative *serialization.Derivative
}

// childShape selects only from the frozen serialization graph. A children
// collection of a derived-type family resolves to its single registered
// concrete derivative, as C# ResolveConcreteChildType does. C# keeps the
// unresolved family when zero or several derivatives exist and then emits no
// discriminator; Go refuses that ambiguous shape instead of degrading silently.
func childShape(field serialization.Field, nested bool) (nodeShape, error) {
	typ := field.Type
	if nested {
		if typ.Kind() != reflect.Pointer || typ.Elem().Kind() != reflect.Struct || field.Scalar != serialization.NotScalar {
			return nodeShape{}, invalid("nested requires a pointer to an object")
		}
		return nodeShape{typ: typ.Elem(), fields: field.Fields()}, nil
	}
	if typ.Kind() != reflect.Slice && typ.Kind() != reflect.Array {
		return nodeShape{}, invalid("children requires a slice or array of objects")
	}
	if typ.Elem().Kind() == reflect.Interface {
		derivatives := field.Derivatives()
		if len(derivatives) != 1 {
			return nodeShape{}, invalid("derived children require exactly one registered concrete derivative")
		}
		selected := derivatives[0]
		concrete := indirectType(selected.Type)
		if concrete.Kind() != reflect.Struct {
			return nodeShape{}, invalid("derived children require an object derivative")
		}
		return nodeShape{typ: concrete, fields: selected.Fields(), derivative: &selected}, nil
	}
	typ = indirectType(typ.Elem())
	if typ.Kind() != reflect.Struct {
		return nodeShape{}, invalid("children requires object elements")
	}
	return nodeShape{typ: typ, fields: field.Fields()}, nil
}

// scopedFields returns the local fields of a child or nested node. A children
// collection of a derived-type family with one registered derivative uses that
// derivative's fields; any other family has no addressable local fields.
func scopedFields(fields []serialization.Field, path string) []serialization.Field {
	field, ok := binaryMappingField(fields, path)
	if !ok {
		return nil
	}
	if (field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Array) && field.Type.Elem().Kind() == reflect.Interface {
		if derivatives := field.Derivatives(); len(derivatives) == 1 {
			return derivatives[0].Fields()
		}
		return nil
	}
	return field.Fields()
}

func (c *compiler) inferParent(parent, event []serialization.Field, key expression, p Provenance) expression {
	var id reflect.Type
	for _, f := range serialization.RootFields(parent) {
		if strings.EqualFold(lastGoName(f.GoField), "Id") {
			id = f.Type
			break
		}
	}
	result := expression{kind: sourceExpression}
	for _, f := range serialization.RootFields(event) {
		if f.Type != id || key.kind == pathExpression && strings.EqualFold(key.text, f.Path) {
			continue
		}
		if result.kind != sourceExpression {
			c.result.diagnostics = append(c.result.diagnostics, Diagnostic{Message: "ambiguous parent key: first declared exact ID-type match wins; specify parent-key", Replacement: p})
			break
		}
		result = expression{kind: pathExpression, text: f.Path}
	}
	return result
}
func discoverIdentity(fields []serialization.Field, keyField string, key expression) string {
	if keyField != "" {
		return keyField
	}
	for _, f := range serialization.RootFields(fields) {
		if strings.EqualFold(lastGoName(f.GoField), "Id") {
			return f.Path
		}
	}
	if key.kind == pathExpression {
		for _, f := range serialization.RootFields(fields) {
			if strings.EqualFold(f.Name, key.text) {
				return f.Path
			}
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
func nodeUsesEvent(n *nodeDefinition, event events.TypeRef) bool {
	if slices.ContainsFunc(n.from, func(f fromDefinition) bool { return f.event == event }) || slices.ContainsFunc(n.joins, func(j joinDefinition) bool { return j.event == event }) {
		return true
	}
	for _, child := range n.children {
		if nodeUsesEvent(child, event) {
			return true
		}
	}
	for _, child := range n.nested {
		if nodeUsesEvent(child, event) {
			return true
		}
	}
	return false
}

func hasSubscriptions(n *nodeDefinition) bool {
	if len(n.from)+len(n.joins)+len(n.removals) > 0 {
		return true
	}
	for _, child := range n.children {
		if hasSubscriptions(child) {
			return true
		}
	}
	for _, child := range n.nested {
		if hasSubscriptions(child) {
			return true
		}
	}
	return false
}
func mappingExpression(d declarations.Directive, target string) expression {
	e := expression{kind: pathExpression, text: target}
	if from, ok := argument(d, "from"); ok {
		e.text = from.Text
	}
	switch d.Name {
	case "add":
		e.kind = addExpression
	case "subtract":
		e.kind = subtractExpression
	case "increment":
		e.kind = incrementExpression
	case "decrement":
		e.kind = decrementExpression
	case "count":
		e.kind = countExpression
	case "clear":
		e.kind = nullExpression
	case "context":
		e.kind = contextExpression
	case "value":
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
