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
	if d.data == nil || model.GoType() != d.Model().GoType() || before == nil || after == nil {
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
	return Definition{data: &copy}, nil
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
		w.provenance.Path = w.path
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
	n.exclusions = slices.Clone(n.exclusions)
	for i, path := range n.exclusions {
		n.exclusions[i] = r.path(path, old, next)
	}
	n.from = slices.Clone(n.from)
	for i, from := range n.from {
		n.from[i] = r.from(from, old, next)
	}
	n.joins = slices.Clone(n.joins)
	for i, join := range n.joins {
		n.joins[i].on = r.path(join.on, old, next)
		n.joins[i].fromDefinition = r.from(join.fromDefinition, old, next)
	}
	n.removals = slices.Clone(n.removals)
	for i, removal := range n.removals {
		eventOld, eventNext := r.event(removal.event)
		n.removals[i].key = r.expression(removal.key, eventOld, eventNext)
		n.removals[i].parent = r.expression(removal.parent, eventOld, eventNext)
	}
	n.all = slices.Clone(n.all)
	for i, w := range n.all {
		// C# model-bound globals from child nodes are hoisted by bare name.
		// Preserve that behavior using the original field's metadata identity.
		if _, ok := serialization.FieldAt(old, w.path); ok {
			w.path = r.path(w.path, old, next)
		} else {
			p := r.provenance(w.provenance, old, next)
			parts := strings.Split(p.Path, ".")
			w.path = parts[len(parts)-1]
		}
		if w.expression.kind == pathExpression {
			resolved := ""
			for _, event := range r.before.Descriptors() {
				if _, ok := serialization.FieldAt(event.Fields(), w.expression.text); !ok {
					continue
				}
				eventOld, eventNext := r.event(event.Ref())
				path := r.path(w.expression.text, eventOld, eventNext)
				if resolved != "" && resolved != path {
					r.err = invalid("global payload path has inconsistent naming across events")
				}
				resolved = path
			}
			if resolved != "" {
				w.expression.text = resolved
			}
		}
		n.all[i] = w
	}
	n.children = r.children(n.children, old, next)
	n.nested = r.children(n.nested, old, next)
	return n
}
func (r *rebinder) children(children map[string]*nodeDefinition, old, next []serialization.Field) map[string]*nodeDefinition {
	result := make(map[string]*nodeDefinition, len(children))
	for path, child := range children {
		newPath := r.path(path, old, next)
		if child.derivative != nil {
			replacement, ok := binaryMappingField(next, newPath)
			shape, err := childShape(replacement, false)
			if r.err == nil && (!ok || err != nil || shape.derivative == nil || shape.derivative.Type != child.derivative.Type || shape.derivative.ID != child.derivative.ID) {
				r.err = invalid("derived child selection changed during rebinding")
			}
		}
		copy := r.node(*child, scopedFields(old, path), scopedFields(next, newPath))
		result[newPath] = &copy
	}
	return result
}
