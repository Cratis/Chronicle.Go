// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

func (c *compiler) addJoin(n *nodeDefinition, j joinDeclaration, fields []serialization.Field, overwrite bool) error {
	if err := c.validateJoin(j, fields); err != nil {
		return err
	}
	target, err := c.joinTarget(n, j, overwrite)
	if err != nil {
		return err
	}
	return c.addJoinWrites(target, j, fields, overwrite)
}

func (c *compiler) validateJoin(j joinDeclaration, fields []serialization.Field) error {
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
	if !isScalarJoinKey(on, ok, j) {
		return invalid("join on requires a scalar model field")
	}
	return nil
}

func isScalarJoinKey(on serialization.Field, found bool, j joinDeclaration) bool {
	return found && !on.IsEnum() && validateTarget(on, j.onType) == nil && on.Scalar.IsPrimitive()
}

// joinTarget returns the existing join for the event, or appends a new one.
func (c *compiler) joinTarget(n *nodeDefinition, j joinDeclaration, overwrite bool) (*joinDefinition, error) {
	for i := range n.joins {
		if n.joins[i].event == j.subscription.event.Ref() {
			return c.existingJoin(&n.joins[i], j, overwrite)
		}
	}
	n.joins = append(n.joins, joinDefinition{on: j.on, fromDefinition: fromDefinition{event: j.subscription.event.Ref(), key: j.subscription.key}})
	return &n.joins[len(n.joins)-1], nil
}

func (c *compiler) existingJoin(target *joinDefinition, j joinDeclaration, overwrite bool) (*joinDefinition, error) {
	if !overwrite {
		return nil, invalid("duplicate fluent join event")
	}
	if target.on != j.on {
		c.result.diagnostics = append(c.result.diagnostics, Diagnostic{Message: "first model-bound join On wins for this event", Replacement: Provenance{Path: j.on, Directive: "join", Event: target.event, Offset: -1}})
	}
	return target, nil
}

func (c *compiler) addJoinWrites(target *joinDefinition, j joinDeclaration, fields []serialization.Field, overwrite bool) error {
	for _, w := range j.subscription.writes {
		w.provenance.Event = j.subscription.event.Ref()
		if err := addWrite(c.result, &target.fromDefinition, w, fields, j.subscription.event.Fields(), overwrite); err != nil {
			return err
		}
	}
	return nil
}

func (c *compiler) addGlobal(n *nodeDefinition, g globalDeclaration, fields []serialization.Field, overwrite bool) error {
	w := g.write
	target, err := c.globalTarget(g, fields)
	if err != nil {
		return err
	}
	if err = c.validateGlobalExpression(n, g, target, fields); err != nil {
		return err
	}
	if len(n.all) > 0 && n.includeChildren != g.includeChildren {
		return invalid("global mappings disagree on IncludeChildren")
	}
	n.includeChildren = g.includeChildren
	c.result.subscribesAll = c.result.subscribesAll || g.all
	return mergeWrite(c.result, &n.all, w, overwrite)
}

func (c *compiler) writeFailure(w write, err error) error {
	return declarationFailure(c.result.id, w.provenance, err)
}

func (c *compiler) globalTarget(g globalDeclaration, fields []serialization.Field) (serialization.Field, error) {
	w := g.write
	target, ok := binaryMappingField(fields, w.path)
	if !ok || validateTarget(target, w.targetType) != nil {
		return target, c.writeFailure(w, invalid("unknown global target field"))
	}
	if target.IsEnum() && g.all {
		return target, c.writeFailure(w, invalid("all-event enum mappings cannot validate unknown event profiles"))
	}
	return target, nil
}

func (c *compiler) validateGlobalExpression(n *nodeDefinition, g globalDeclaration, target serialization.Field, fields []serialization.Field) error {
	w := g.write
	if w.expression.kind == pathExpression {
		return c.validateGlobalPath(n, g, target, fields)
	}
	if err := validateExpression(w.expression, target, fields, nil, nil); err != nil {
		return c.writeFailure(w, err)
	}
	return nil
}

func (c *compiler) validateGlobalPath(n *nodeDefinition, g globalDeclaration, target serialization.Field, fields []serialization.Field) error {
	w := g.write
	if !eventPropertyPath(w.expression.text) {
		return c.writeFailure(w, invalid("unrepresentable global event path"))
	}
	// C# accepts sparse event fields; validate known representations without
	// requiring every event in the catalog to declare the property.
	for _, event := range c.catalog.Descriptors() {
		if !g.all && !nodeUsesEvent(n, event.Ref()) {
			continue
		}
		if globalSourceIncompatible(target, w.expression.text, fields, event) {
			return c.writeFailure(w, invalid("incompatible global event property"))
		}
	}
	return nil
}

func globalSourceIncompatible(target serialization.Field, path string, fields []serialization.Field, event events.Descriptor) bool {
	source, exists := binaryMappingField(event.Fields(), path)
	return exists && !scalarCompatible(target, source, fields, event.Fields())
}
