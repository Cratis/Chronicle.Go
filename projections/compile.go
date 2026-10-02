// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

// HasMappings reports whether a registered model bears subscription-producing
// tags. A key or exclusion alone never creates an empty projection.
func HasMappings(model readmodels.Descriptor) bool {
	for _, f := range model.Fields() {
		directives, _ := declarations.Parse(declarations.V1, f.Tag)
		for _, d := range directives {
			if d.Name == "set" || d.Name == "context" || d.Name == "value" {
				return true
			}
		}
	}
	return false
}

// Compile resolves and validates a declaration against one frozen event catalog.
// It performs no I/O, evaluates no user callbacks and publishes no partial result.
// Registry users get the same validation atomically during NewClient.
func Compile(declaration Declaration, catalog *events.Catalog) (Definition, error) {
	if declaration.data == nil {
		return Definition{}, invalid("projection declaration required")
	}
	d := declaration.data
	locate := func(err error) (Definition, error) {
		return Definition{}, declarationFailure(d.id, Provenance{Offset: -1}, err)
	}
	if d.err != nil {
		return locate(d.err)
	}
	if d.model.GoType() == nil || catalog == nil || blank(d.id) || blank(string(d.sequence)) {
		return locate(invalid("model, catalog, projection identity and sequence required"))
	}
	bound, err := readmodels.BindProjection(d.model, d.id, d.sequence, d.passive)
	if err != nil {
		return locate(err)
	}
	compiled := &definition{id: d.id, model: bound, sequence: d.sequence, passive: d.passive, notRewindable: d.notRewindable, noAuto: d.noAuto}
	fields := d.model.Fields()
	for _, event := range d.aliases {
		if err = validateEvent(catalog, event); err != nil {
			return locate(err)
		}
	}
	froms := map[events.TypeRef]*fromDefinition{}
	ensure := func(event events.Descriptor) *fromDefinition {
		ref := event.Ref()
		if froms[ref] == nil {
			froms[ref] = &fromDefinition{event: ref, key: expression{kind: sourceExpression}}
		}
		return froms[ref]
	}
	for _, sub := range d.subscriptions {
		p := Provenance{FrontEnd: "typed", Directive: "FromEvent", Offset: -1, Event: sub.event.Ref()}
		if sub.err != nil {
			return Definition{}, declarationFailure(d.id, p, sub.err)
		}
		if err = validateEvent(catalog, sub.event); err != nil {
			return Definition{}, declarationFailure(d.id, p, err)
		}
		if froms[sub.event.Ref()] != nil {
			return Definition{}, declarationFailure(d.id, p, invalid("duplicate event subscription"))
		}
		if err = validateKey(sub.key, sub.keyType, sub.event.Fields(), false); err != nil {
			return Definition{}, declarationFailure(d.id, p, err)
		}
		if err = validateKey(sub.parent, sub.parentType, sub.event.Fields(), true); err != nil {
			return Definition{}, declarationFailure(d.id, p, err)
		}
		from := ensure(sub.event)
		from.key, from.parent = sub.key, sub.parent
		compiled.provenance = append(compiled.provenance, p)
		for _, w := range sub.writes {
			w.provenance.Event = sub.event.Ref()
			if err = addWrite(compiled, from, w, fields, sub.event.Fields(), false); err != nil {
				return Definition{}, err
			}
		}
	}
	for _, field := range fields {
		directives, parseErr := declarations.Parse(declarations.V1, field.Tag)
		if parseErr != nil {
			return locate(parseErr)
		} // plan validation normally makes this unreachable
		// C# processes attribute families in set -> context -> value order, not in
		// textual order. Within a family the last attribute wins, with a diagnostic.
		slices.SortStableFunc(directives, func(a, b declarations.Directive) int { return cmp.Compare(priority(a.Name), priority(b.Name)) })
		for _, directive := range directives {
			p := Provenance{FrontEnd: "model-bound", GoField: field.GoField, Path: field.Path, Directive: directive.Name, Offset: directive.Offset}
			if field.Collection || strings.Contains(field.Path, ".") {
				return Definition{}, declarationFailure(d.id, p, invalid("nested-node declarations require the children/nested projection slice"))
			}
			switch directive.Name {
			case "key":
				if compiled.keyField != "" || field.Scalar == serialization.NotScalar || field.Nullable {
					return Definition{}, declarationFailure(d.id, p, invalid("one non-nullable scalar key field is required"))
				}
				compiled.keyField = field.Path
				compiled.provenance = append(compiled.provenance, p)
			case "no-auto", "not-projected":
				if !slices.Contains(compiled.exclusions, field.Path) {
					compiled.exclusions = append(compiled.exclusions, field.Path)
				}
				compiled.provenance = append(compiled.provenance, p)
			default:
				if !d.modelBound {
					return Definition{}, declarationFailure(d.id, p, invalid("model mapping tags and fluent mappings cannot be mixed"))
				}
				reference := directive.Args[0].Value
				event, resolveErr := resolveEvent(catalog, d.aliases, reference)
				if resolveErr != nil {
					failure := declarationFailure(d.id, p, resolveErr)
					failure.EventReference = referenceName(reference)
					return Definition{}, failure
				}
				p.Event = event.Ref()
				e := expression{kind: pathExpression, text: field.Name}
				if directive.Name == "context" {
					e.kind = contextExpression
				}
				if len(directive.Args) > 1 {
					e.text = directive.Args[1].Value.Text
				}
				if directive.Name == "value" {
					e = literalValue(directive.Args[1].Value)
				}
				w := write{path: field.Path, expression: e, provenance: p}
				if err = addWrite(compiled, ensure(event), w, fields, event.Fields(), true); err != nil {
					return Definition{}, err
				}
			}
		}
	}
	if len(froms) == 0 {
		return locate(invalid("projection must subscribe to at least one registered event"))
	}
	for _, from := range froms {
		if d.passive && from.key.kind != sourceExpression {
			return locate(invalid("passive key redirection is not supported by immediate instance reads"))
		}
		slices.SortFunc(from.writes, func(a, b write) int { return strings.Compare(a.path, b.path) })
		compiled.from = append(compiled.from, *from)
	}
	slices.SortFunc(compiled.from, func(a, b fromDefinition) int {
		if n := strings.Compare(string(a.event.ID), string(b.event.ID)); n != 0 {
			return n
		}
		return cmp.Compare(a.event.Generation, b.event.Generation)
	})
	slices.Sort(compiled.exclusions)
	return Definition{data: compiled}, nil
}

func addWrite(d *definition, from *fromDefinition, w write, modelFields, eventFields []serialization.Field, overwrite bool) error {
	target, ok := serialization.FieldAt(modelFields, w.path)
	if !ok {
		return declarationFailure(d.id, w.provenance, invalid("unknown serialized model property"))
	}
	w.provenance.GoField = target.GoField
	if err := validateTarget(target, w.targetType); err != nil {
		return declarationFailure(d.id, w.provenance, err)
	}
	if err := validateExpression(w.expression, target, eventFields, w.sourceType); err != nil {
		return declarationFailure(d.id, w.provenance, err)
	}
	for i, previous := range from.writes {
		if previous.path != w.path {
			continue
		}
		if !overwrite {
			return declarationFailure(d.id, w.provenance, invalid("duplicate property write for event"))
		}
		d.diagnostics = append(d.diagnostics, Diagnostic{Message: "model-bound mapping shadows an earlier mapping (C# attribute-family precedence)", Previous: previous.provenance, Replacement: w.provenance})
		from.writes[i] = w
		d.provenance = append(d.provenance, w.provenance)
		return nil
	}
	from.writes = append(from.writes, w)
	d.provenance = append(d.provenance, w.provenance)
	return nil
}
func priority(name string) int {
	switch name {
	case "set":
		return 1
	case "context":
		return 2
	case "value":
		return 3
	default:
		return 0
	}
}
func declarationFailure(id string, p Provenance, err error) *DeclarationError {
	// All compiler failures use controlled, literal-free messages.
	var existing *DeclarationError
	if errors.As(err, &existing) {
		copy := *existing
		copy.Artifact = id
		return &copy
	}
	reference := ""
	if p.Event.ID != "" {
		reference = fmt.Sprintf("%s,%d", p.Event.ID, p.Event.Generation)
	}
	return &DeclarationError{Artifact: id, GoField: p.GoField, Path: p.Path, Directive: p.Directive, Offset: p.Offset, EventReference: reference, Message: err.Error(), Cause: err}
}
func validateEvent(catalog *events.Catalog, event events.Descriptor) error {
	if event.GoType() == nil {
		return invalid("event handle is empty")
	}
	registered, ok := catalog.LookupRef(event.Ref())
	if !ok || registered.GoType() != event.GoType() || registered.Schema() != event.Schema() {
		return invalid("event handle does not belong to the frozen store catalog")
	}
	return nil
}
func resolveEvent(catalog *events.Catalog, aliases map[string]events.Descriptor, reference declarations.Value) (events.Descriptor, error) {
	if reference.Kind == declarations.Name && strings.HasPrefix(reference.Text, "@") {
		event, ok := aliases[strings.TrimPrefix(reference.Text, "@")]
		if !ok {
			return events.Descriptor{}, invalid("unknown event alias")
		}
		return event, nil
	}
	if reference.Kind == declarations.Call && reference.Text == "id" {
		id := events.TypeID(reference.Args[0].Value.Text)
		if len(reference.Args) == 1 {
			if event, ok := catalog.LookupID(id); ok {
				return event, nil
			}
		} else {
			generation, _ := strconv.ParseUint(reference.Args[1].Value.Text, 10, 32)
			if event, ok := catalog.LookupRef(events.TypeRef{ID: id, Generation: events.Generation(generation)}); ok {
				return event, nil
			}
		}
		return events.Descriptor{}, invalid("persisted event identity/generation is not registered")
	}
	var candidates []events.Descriptor
	for _, event := range catalog.Descriptors() {
		typ := event.GoType()
		if reference.Kind == declarations.Name && typ.Name() == reference.Text || reference.Kind == declarations.Call && reference.Text == "go" && typ.PkgPath()+"."+typ.Name() == reference.Args[0].Value.Text {
			candidates = append(candidates, event)
		}
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	if len(candidates) > 1 {
		names := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			typ := candidate.GoType()
			names = append(names, typ.PkgPath()+"."+typ.Name())
		}
		slices.Sort(names)
		return events.Descriptor{}, invalid(fmt.Sprintf("ambiguous event name; qualified candidates: %s", strings.Join(names, ", ")))
	}
	return events.Descriptor{}, invalid("event reference is not registered")
}
func referenceName(v declarations.Value) string {
	if v.Kind == declarations.Call && len(v.Args) > 0 {
		return v.Text + "(" + v.Args[0].Value.Text + ")"
	}
	return v.Text
}
