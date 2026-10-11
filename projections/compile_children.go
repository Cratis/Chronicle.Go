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

type nodeIdentity struct{ typ, family reflect.Type }

// childSpec is a child or nested field together with the creators that introduce it.
type childSpec struct {
	child    childDeclaration
	field    serialization.Field
	fields   []serialization.Field
	creators []subscription
}

// boundChildContext collects what the model-bound directives of one field contribute to its child declaration.
type boundChildContext struct {
	field    serialization.Field
	fields   []serialization.Field
	nested   bool
	child    *childDeclaration
	creators []subscription
}

func isChildDirective(name string) bool {
	switch name {
	case "children", "clear", "remove", "remove-join":
		return true
	}
	return false
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
	child := childDeclaration{path: field.Path, fieldType: field.Type, typ: shape.typ, nested: nested, data: c.boundChildDeclaration(shape.typ)}
	bc := &boundChildContext{field: field, fields: fields, nested: nested, child: &child}
	for _, directive := range directives {
		if err = c.applyChildDirective(bc, directive); err != nil {
			return err
		}
	}
	return c.compileChild(n, childSpec{child: child, field: field, fields: fields, creators: bc.creators})
}

func (c *compiler) boundChildDeclaration(typ reflect.Type) *declaration {
	registered := c.declaration.nodes[typ]
	if registered == nil {
		d := newDeclaration(readmodels.Descriptor{}, nil)
		d.modelBound = true
		return d
	}
	c.usedNodes[typ] = true
	return cloneDeclaration(registered)
}

func (c *compiler) applyChildDirective(bc *boundChildContext, directive declarations.Directive) error {
	if !isChildDirective(directive.Name) {
		return nil
	}
	p := Provenance{FrontEnd: "model-bound", GoField: bc.field.GoField, Path: bc.field.Path, Directive: directive.Name, Offset: directive.Offset}
	event, err := c.resolve(directive, p)
	if err != nil {
		return err
	}
	if directive.Name == "children" {
		return c.addChildCreator(bc, directive, event, p)
	}
	return bc.addRemoval(directive, event)
}

func (c *compiler) addChildCreator(bc *boundChildContext, directive declarations.Directive, event events.Descriptor, p Provenance) error {
	s := newSubscription(event, nil)
	if a, ok := argument(directive, "key"); ok {
		s.key = parsedKey(a)
	}
	if a, ok := argument(directive, "parent-key"); ok {
		s.parent = parsedKey(a)
	} else {
		s.parent = c.inferParent(bc.fields, event.Fields(), s.key, p)
	}
	if err := bc.setIdentity(directive); err != nil {
		return err
	}
	bc.creators = append(bc.creators, s)
	return nil
}

func (bc *boundChildContext) setIdentity(directive declarations.Directive) error {
	a, ok := argument(directive, "identified-by")
	if !ok {
		return nil
	}
	if bc.child.identifiedBy != "" && bc.child.identifiedBy != a.Text {
		return invalid("children disagree on identity")
	}
	bc.child.identifiedBy = a.Text
	return nil
}

func (bc *boundChildContext) addRemoval(directive declarations.Directive, event events.Descriptor) error {
	if directive.Name == "clear" && !bc.nested {
		return invalid("collection clear cannot represent null; remove keyed children instead")
	}
	d := bc.child.data
	d.removals = append(d.removals, removalFromTag(directive, event))
	return nil
}

func (c *compiler) compileChild(n *nodeDefinition, spec childSpec) error {
	child, field := spec.child, spec.field
	shape, err := c.checkedChildShape(n, spec)
	if err != nil {
		return err
	}
	d, err := c.childDeclarationFor(child, shape.typ)
	if err != nil {
		return err
	}
	identity := childNodeIdentity(shape, field)
	stopExpanding := c.active[identity] && d.modelBound
	// C# seeds traversal with the root type, but the first child level always
	// uses the normal filter (includeSelfReferencingEvents: false). A root
	// collection of its own type stops here without using the recursive filter.
	recursive := stopExpanding && c.childDepth > 0
	defer c.enterChild(identity)()
	compiled, err := c.compileNode(d, shape.fields, spec.fields, n.ownNoAuto, child.nested, child.identifiedBy, spec.creators, stopExpanding, recursive)
	if err != nil {
		return err
	}
	if err = c.stampDerivative(compiled, shape); err != nil {
		return err
	}
	attachChild(n, field.Path, compiled, child.nested)
	return nil
}

// checkedChildShape resolves the child shape and validates it against the node and the declaration.
func (c *compiler) checkedChildShape(n *nodeDefinition, spec childSpec) (nodeShape, error) {
	child, field := spec.child, spec.field
	shape, err := childShape(field, child.nested)
	if err != nil {
		return shape, c.childShapeFailure(field, child.nested, err)
	}
	if shape.typ != child.typ || n.hasChildAt(field.Path) {
		return shape, invalid("duplicate or incompatible child node")
	}
	if err = validateNodeOptions(child.data); err != nil {
		return shape, err
	}
	return shape, checkChildIdentityType(child, shape.fields)
}

func (n *nodeDefinition) hasChildAt(path string) bool {
	return n.children[path] != nil || n.nested[path] != nil
}

func (c *compiler) childShapeFailure(field serialization.Field, nested bool, err error) error {
	directive := "children"
	if nested {
		directive = "nested"
	}
	return declarationFailure(c.result.id, Provenance{GoField: field.GoField, Path: field.Path, Directive: directive, Offset: -1}, err)
}

func checkChildIdentityType(child childDeclaration, local []serialization.Field) error {
	if child.identifiedBy == "" || child.identityType == nil {
		return nil
	}
	identity, ok := binaryMappingField(local, child.identifiedBy)
	if !ok || identity.Type != child.identityType {
		return invalid("child identity descriptor has wrong type")
	}
	return nil
}

// childDeclarationFor clones the child declaration. Only model-bound ChildrenFrom declarations supply creators for parent
// inference and identity mappings. Fluent From retains its empty parent key.
func (c *compiler) childDeclarationFor(child childDeclaration, typ reflect.Type) (*declaration, error) {
	d := cloneDeclaration(child.data)
	if d.modelBound {
		return d, nil
	}
	registered := c.declaration.nodes[typ]
	if registered == nil {
		return d, nil
	}
	if err := validateNodeOptions(registered); err != nil {
		return nil, err
	}
	c.usedNodes[typ] = true
	mergeRegisteredNode(d, registered)
	return d, nil
}

func mergeRegisteredNode(d, registered *declaration) {
	if !d.autoSet && registered.autoSet {
		d.noAuto, d.autoSet = registered.noAuto, true
	}
	d.subscriptions = append(slices.Clone(registered.subscriptions), d.subscriptions...)
	d.removals = append(slices.Clone(registered.removals), d.removals...)
}

// childNodeIdentity identifies a derived child by its family and selected concrete type, so
// an ordinary node of the same concrete type is not mistaken for recursion.
func childNodeIdentity(shape nodeShape, field serialization.Field) nodeIdentity {
	identity := nodeIdentity{typ: shape.typ}
	if shape.derivative != nil {
		identity.family = field.Type.Elem()
	}
	return identity
}

// enterChild marks the node identity as active and deepens the child level; the result undoes both.
func (c *compiler) enterChild(identity nodeIdentity) func() {
	wasActive := c.active[identity]
	if !wasActive {
		c.active[identity] = true
	}
	c.childDepth++
	return func() {
		c.childDepth--
		if !wasActive {
			delete(c.active, identity)
		}
	}
}

func (c *compiler) stampDerivative(compiled *nodeDefinition, shape nodeShape) error {
	if shape.derivative == nil {
		return nil
	}
	compiled.derivative = shape.derivative
	return c.stampDerivedChild(compiled)
}

func attachChild(n *nodeDefinition, path string, compiled *nodeDefinition, nested bool) {
	if nested {
		n.nested[path] = compiled
	} else {
		n.children[path] = compiled
	}
}

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
	if nested {
		return nestedShape(field)
	}
	return collectionShape(field)
}

func isNestedObject(field serialization.Field) bool {
	typ := field.Type
	return typ.Kind() == reflect.Pointer && typ.Elem().Kind() == reflect.Struct && field.Scalar == serialization.NotScalar
}

func nestedShape(field serialization.Field) (nodeShape, error) {
	if !isNestedObject(field) {
		return nodeShape{}, invalid("nested requires a pointer to an object")
	}
	return nodeShape{typ: field.Type.Elem(), fields: field.Fields()}, nil
}

func isSliceOrArray(typ reflect.Type) bool {
	return typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array
}

// isFamilyCollection reports whether the type is a slice or array of a derived-type family.
func isFamilyCollection(typ reflect.Type) bool {
	return isSliceOrArray(typ) && typ.Elem().Kind() == reflect.Interface
}

func collectionShape(field serialization.Field) (nodeShape, error) {
	if !isSliceOrArray(field.Type) {
		return nodeShape{}, invalid("children requires a slice or array of objects")
	}
	if isFamilyCollection(field.Type) {
		return derivedShape(field)
	}
	typ := indirectType(field.Type.Elem())
	if typ.Kind() != reflect.Struct {
		return nodeShape{}, invalid("children requires object elements")
	}
	return nodeShape{typ: typ, fields: field.Fields()}, nil
}

func derivedShape(field serialization.Field) (nodeShape, error) {
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

// scopedFields returns the local fields of a child or nested node. A children
// collection of a derived-type family with one registered derivative uses that
// derivative's fields; any other family has no addressable local fields.
func scopedFields(fields []serialization.Field, path string) []serialization.Field {
	field, ok := binaryMappingField(fields, path)
	if !ok {
		return nil
	}
	if !isFamilyCollection(field.Type) {
		return field.Fields()
	}
	if derivatives := field.Derivatives(); len(derivatives) == 1 {
		return derivatives[0].Fields()
	}
	return nil
}

func (c *compiler) inferParent(parent, event []serialization.Field, key expression, p Provenance) expression {
	var id reflect.Type
	if f, ok := findRootField(parent, isIDField); ok {
		id = f.Type
	}
	result := expression{kind: sourceExpression}
	for _, f := range serialization.RootFields(event) {
		if !isParentCandidate(f, id, key) {
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

// isParentCandidate reports whether an event field has the parent ID type and is not the explicit key.
func isParentCandidate(f serialization.Field, id reflect.Type, key expression) bool {
	return f.Type == id && !(key.kind == pathExpression && strings.EqualFold(key.text, f.Path))
}
