// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

// Rebind returns a detached definition whose property paths come exclusively from
// the supplied plans. Authoring is resolved before rebinding, so tags and fluent
// paths use declaration names even when a client selects another naming policy.
// This registry-composition hook requires catalogs of the same declared events.
func (d Definition) Rebind(model readmodels.Descriptor, before, after *events.Catalog) (Definition, error) {
	if !d.canRebind(model, before, after) {
		return Definition{}, invalid("matching model and event catalogs required")
	}
	copy := *d.data
	copy.model = model
	state, err := model.RebindJSON([]byte(d.data.initialState), d.Model())
	if err != nil {
		return Definition{}, err
	}
	copy.initialState, err = prepareInitialState(model, string(state))
	if err != nil {
		return Definition{}, err
	}
	r := rebinder{before: before, after: after}
	copy.nodeDefinition = r.node(d.data.nodeDefinition, d.Model().Fields(), model.Fields())
	copy.provenance = slices.Clone(copy.provenance)
	for i, p := range copy.provenance {
		copy.provenance[i] = r.provenance(p, d.Model().Fields(), model.Fields())
	}
	copy.diagnostics = slices.Clone(copy.diagnostics)
	for i := range copy.diagnostics {
		copy.diagnostics[i].Previous = r.provenance(copy.diagnostics[i].Previous, d.Model().Fields(), model.Fields())
		copy.diagnostics[i].Replacement = r.provenance(copy.diagnostics[i].Replacement, d.Model().Fields(), model.Fields())
	}
	if r.err != nil {
		return Definition{}, r.err
	}
	if err := validateEnumGraph(&copy, after); err != nil {
		return Definition{}, err
	}
	if err := validateBinaryGraph(&copy, after); err != nil {
		return Definition{}, err
	}
	if err := validateNoSinkGraph(&copy); err != nil {
		return Definition{}, err
	}
	return Definition{data: &copy}, nil
}

func (d Definition) canRebind(model readmodels.Descriptor, before, after *events.Catalog) bool {
	if d.data == nil {
		return false
	}
	if before == nil || after == nil {
		return false
	}
	return model.GoType() == d.Model().GoType()
}

type rebinder struct {
	before, after *events.Catalog
	err           error
}

func (r *rebinder) path(path string, old, next []serialization.Field) string {
	if r.err != nil {
		return ""
	}
	result, err := serialization.RebindPath(path, old, next)
	if err != nil {
		r.err = err
	}
	return result
}
func (r *rebinder) provenance(p Provenance, old, next []serialization.Field) Provenance {
	if p.Path == "" {
		return p
	}
	if p.GoField != "" {
		for _, f := range next {
			if f.GoField == p.GoField {
				p.Path = f.Path
				return p
			}
		}
	}
	if _, ok := serialization.FieldAt(old, p.Path); ok {
		p.Path = r.path(p.Path, old, next)
	}
	return p
}
func (r *rebinder) expression(e expression, old, next []serialization.Field) expression {
	switch e.kind {
	case pathExpression, addExpression, subtractExpression:
		e.text = r.path(e.text, old, next)
	case compositeExpression:
		e.parts = slices.Clone(e.parts)
		for i := range e.parts {
			e.parts[i].expression = r.expression(e.parts[i].expression, old, next)
		}
	}
	return e
}
func (r *rebinder) event(ref events.TypeRef) ([]serialization.Field, []serialization.Field) {
	old, oldOK := r.before.LookupRef(ref)
	next, nextOK := r.after.LookupRef(ref)
	if !oldOK || !nextOK || old.GoType() != next.GoType() {
		r.err = invalid("projection event missing from catalog")
		return nil, nil
	}
	return old.Fields(), next.Fields()
}
func (r *rebinder) from(from fromDefinition, old, next []serialization.Field) fromDefinition {
	eventOld, eventNext := r.event(from.event)
	from.key, from.parent = r.expression(from.key, eventOld, eventNext), r.expression(from.parent, eventOld, eventNext)
	from.writes = slices.Clone(from.writes)
	for j, w := range from.writes {
		if !w.synthetic {
			w.path = r.path(w.path, old, next)
		}
		if w.entry.source == eventContentKey {
			w.entry.path = r.path(w.entry.path, eventOld, eventNext)
		}
		w.provenance.Path = w.entry.encode(w.path)
		w.expression = r.expression(w.expression, eventOld, eventNext)
		from.writes[j] = w
	}
	return from
}
func (r *rebinder) node(n nodeDefinition, old, next []serialization.Field) nodeDefinition {
	n.keyField = r.path(n.keyField, old, next)
	if n.identifiedBy != "$eventSourceId" && n.identifiedBy != "*NotSet*" {
		n.identifiedBy = r.path(n.identifiedBy, old, next)
	}
	if n.ordering != nil && n.ordering.sortBy != "" {
		ordering := *n.ordering
		ordering.sortBy = r.path(ordering.sortBy, old, next)
		n.ordering = &ordering
	}
	n.exclusions = slices.Clone(n.exclusions)
	for i, path := range n.exclusions {
		n.exclusions[i] = r.path(path, old, next)
	}
	n.from = r.froms(n.from, old, next)
	n.joins = r.joins(n.joins, old, next)
	n.removals = r.removals(n.removals)
	n.all = r.globals(n.all, old, next)
	n.children = r.children(n.children, old, next)
	n.nested = r.children(n.nested, old, next)
	return n
}

func (r *rebinder) froms(from []fromDefinition, old, next []serialization.Field) []fromDefinition {
	result := slices.Clone(from)
	for i := range result {
		result[i] = r.from(result[i], old, next)
	}
	return result
}

func (r *rebinder) joins(joins []joinDefinition, old, next []serialization.Field) []joinDefinition {
	result := slices.Clone(joins)
	for i := range result {
		result[i].on = r.path(result[i].on, old, next)
		result[i].fromDefinition = r.from(result[i].fromDefinition, old, next)
	}
	return result
}

func (r *rebinder) removals(removals []removalDefinition) []removalDefinition {
	result := slices.Clone(removals)
	for i := range result {
		eventOld, eventNext := r.event(result[i].event)
		result[i].key = r.expression(result[i].key, eventOld, eventNext)
		result[i].parent = r.expression(result[i].parent, eventOld, eventNext)
	}
	return result
}

func (r *rebinder) globals(writes []write, old, next []serialization.Field) []write {
	result := slices.Clone(writes)
	for i := range result {
		result[i].path = r.globalTarget(result[i], old, next)
		result[i].expression = r.globalExpression(result[i].expression)
	}
	return result
}

func (r *rebinder) globalTarget(w write, old, next []serialization.Field) string {
	// C# model-bound globals from child nodes are hoisted by bare name.
	// Preserve that behavior using the original field's metadata identity.
	if _, ok := serialization.FieldAt(old, w.path); ok {
		return r.path(w.path, old, next)
	}
	p := r.provenance(w.provenance, old, next)
	parts := strings.Split(p.Path, ".")
	return parts[len(parts)-1]
}

func (r *rebinder) globalExpression(e expression) expression {
	if e.kind != pathExpression {
		return e
	}
	resolved := ""
	for _, event := range r.before.Descriptors() {
		if _, ok := serialization.FieldAt(event.Fields(), e.text); !ok {
			continue
		}
		eventOld, eventNext := r.event(event.Ref())
		path := r.path(e.text, eventOld, eventNext)
		if resolved != "" && resolved != path {
			r.err = invalid("global payload path has inconsistent naming across events")
		}
		resolved = path
	}
	if resolved != "" {
		e.text = resolved
	}
	return e
}
func (r *rebinder) children(children map[string]*nodeDefinition, old, next []serialization.Field) map[string]*nodeDefinition {
	result := make(map[string]*nodeDefinition, len(children))
	for path, child := range children {
		// An earlier failure leaves no path to resolve; the caller discards the
		// partial result and reports r.err.
		newPath := r.path(path, old, next)
		if r.err != nil {
			return result
		}
		if child.derivative != nil && !sameReboundDerivative(child.derivative, next, newPath) {
			r.err = invalid("derived child selection changed during rebinding")
			return result
		}
		copy := r.node(*child, scopedFields(old, path), scopedFields(next, newPath))
		result[newPath] = &copy
	}
	return result
}

func sameReboundDerivative(selected *serialization.Derivative, fields []serialization.Field, path string) bool {
	replacement, ok := binaryMappingField(fields, path)
	if !ok {
		return false
	}
	shape, err := childShape(replacement, false)
	if err != nil || shape.derivative == nil {
		return false
	}
	if shape.derivative.Type != selected.Type {
		return false
	}
	return shape.derivative.ID == selected.ID
}
