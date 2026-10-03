// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"cmp"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

// HasMappings reports whether a registered model bears subscription-producing
// tags. A key, exclusion or Every alone never creates an empty projection.
func HasMappings(model readmodels.Descriptor) bool {
	for _, f := range model.Fields() {
		directives, _ := declarations.Parse(declarations.V1, f.Tag)
		for _, d := range directives {
			switch d.Name {
			case "set", "context", "value", "add", "subtract", "increment", "decrement", "count", "clear", "children", "join", "remove", "remove-join", "all":
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
	definitions, err := CompileGroup([]Declaration{declaration}, catalog)
	if err != nil {
		return Definition{}, err
	}
	if len(definitions) != 1 {
		return Definition{}, invalid("standalone global handler requires CompileGroup")
	}
	return definitions[0], nil
}

func compileOrdinary(declaration Declaration, catalog *events.Catalog) (Definition, error) {
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
	if d.globalFor != nil && (d.initialState != "" || len(d.labels) != 0) {
		return locate(invalid("global handlers cannot author initial state or artifact labels"))
	}
	if d.model.GoType() == nil || catalog == nil || blank(d.id) || blank(string(d.sequence)) {
		return locate(invalid("model, catalog, projection identity and sequence required"))
	}
	bound, err := readmodels.BindProjection(d.model, d.id, d.sequence, d.passive)
	if err != nil {
		return locate(err)
	}
	initialState, err := prepareInitialState(d.model, d.initialState)
	if err != nil {
		return locate(err)
	}
	compiled := &definition{id: d.id, model: bound, sequence: d.sequence, passive: d.passive, notRewindable: d.notRewindable, variant: d.variant, sequenceExplicit: d.sequenceExplicit, initialState: initialState, labels: slices.Clone(d.labels)}
	for _, event := range d.aliases {
		if err = validateEvent(catalog, event); err != nil {
			return locate(err)
		}
	}
	c := compiler{result: compiled, catalog: catalog, declaration: d, usedNodes: map[reflect.Type]bool{}, active: map[reflect.Type]bool{d.model.GoType(): true}, ancestorCreators: map[events.TypeRef]int{}}
	node, err := c.compileNode(d, d.model.Fields(), nil, false, false, "", nil, false, false)
	if err != nil {
		return locate(err)
	}
	compiled.nodeDefinition = *node
	if d.variantKey != "" {
		field, ok := serialization.FieldAt(d.model.Fields(), d.variantKey)
		if !ok || field.Type != d.variantKeyType || field.Nullable || field.Collection || field.Scalar == serialization.NotScalar {
			return locate(invalid("variant key requires a non-nullable scalar model field"))
		}
		if compiled.keyField != "" && compiled.keyField != d.variantKey {
			return locate(invalid("conflicting variant keys"))
		}
		compiled.keyField = d.variantKey
	}
	for _, entering := range d.entering {
		if err := c.validateSubscription(entering); err != nil {
			return locate(err)
		}
		if compiled.passive && entering.key.kind != sourceExpression {
			return locate(invalid("passive key redirection is not supported by immediate instance reads"))
		}
		if entering.parent.kind != emptyExpression {
			return locate(invalid("EntersOn does not accept a parent key"))
		}
		if slices.ContainsFunc(compiled.entering, func(e fromDefinition) bool { return e.event == entering.event.Ref() }) {
			return locate(invalid("duplicate entering event"))
		}
		compiled.entering = append(compiled.entering, fromDefinition{event: entering.event.Ref(), key: entering.key})
	}
	if len(c.usedNodes) != len(d.nodes) {
		return locate(invalid("WithNodes contains an unreachable node declaration"))
	}
	// Model-bound globals deliberately share bare target names across all nodes,
	// matching C#'s one root All dictionary, not derivative FromEvery groups.
	for _, g := range c.globals {
		if err = c.addGlobal(&compiled.nodeDefinition, g.globalDeclaration, g.fields, true); err != nil {
			return locate(err)
		}
	}
	if d.globalFor != nil {
		// C# merges only explicit root From properties. AutoMap is kernel-owned,
		// and globals are never sent to the kernel, so a bare From has no effect.
		for _, from := range compiled.from {
			if len(from.writes) == 0 {
				return Definition{}, declarationFailure(d.id, Provenance{Directive: "FromEvent", Offset: -1, Event: from.event}, &GlobalFromHasNoProperties{Global: d.model.GoType(), Event: from.event})
			}
		}
	}
	if d.globalFor == nil && d.variant == nil && !compiled.subscribesAll && !hasSubscriptions(&compiled.nodeDefinition) {
		return locate(invalid("projection must subscribe to at least one registered event"))
	}
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
	if err := validateExpression(w.expression, target, modelFields, eventFields, w.sourceType); err != nil {
		return declarationFailure(d.id, w.provenance, err)
	}
	return mergeWrite(d, &from.writes, w, overwrite)
}

func mergeWrite(d *definition, writes *[]write, w write, overwrite bool) error {
	for i, previous := range *writes {
		if previous.path != w.path {
			continue
		}
		if !overwrite {
			return declarationFailure(d.id, w.provenance, invalid("duplicate property write for event"))
		}
		d.diagnostics = append(d.diagnostics, Diagnostic{Message: "model-bound mapping shadows an earlier mapping (C# attribute-family precedence or shared bare-name global)", Previous: previous.provenance, Replacement: w.provenance})
		(*writes)[i] = w
		d.provenance = append(d.provenance, w.provenance)
		return nil
	}
	*writes = append(*writes, w)
	d.provenance = append(d.provenance, w.provenance)
	return nil
}

func priority(name string) int {
	switch name {
	case "set":
		return 1
	case "add":
		return 2
	case "subtract":
		return 3
	case "increment":
		return 4
	case "decrement":
		return 5
	case "count":
		return 6
	case "context":
		return 7
	case "value":
		return 8
	case "clear":
		return 9
	case "join":
		return 10
	case "every":
		return 11
	case "all":
		return 12
	default:
		return 0
	}
}
func declarationFailure(id string, p Provenance, err error) *DeclarationError {
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
	if !ok || registered.GoType() != event.GoType() || registered.Schema() != event.Schema() || registered.SourceStore() != event.SourceStore() {
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
func compareEvent(a, b events.TypeRef) int {
	if n := strings.Compare(string(a.ID), string(b.ID)); n != 0 {
		return n
	}
	return cmp.Compare(a.Generation, b.Generation)
}
