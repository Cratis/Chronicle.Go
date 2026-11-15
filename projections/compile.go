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
	// A derivative's fields are discovered by the same rule as ordinary fields,
	// so a fluent derivative's key tag alone never creates a projection.
	for _, f := range model.Fields() {
		if derivativeHasDirective(f, subscriptionDirective) {
			return true
		}
		directives, _ := declarations.Parse(declarations.V1, f.Tag)
		if slices.ContainsFunc(directives, func(d declarations.Directive) bool { return subscriptionDirective(d.Name) }) {
			return true
		}
	}
	return false
}

// subscriptionDirective reports whether a model tag directive produces a
// projection subscription, as opposed to type metadata such as key or no-auto.
func subscriptionDirective(name string) bool {
	switch name {
	case "set", "context", "value", "add", "subtract", "increment", "decrement", "count", "clear", "children", "join", "remove", "remove-join", "all":
		return true
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
	compiled, err := prepareOrdinaryDefinition(d, catalog)
	if err != nil {
		return locate(err)
	}
	c := compiler{result: compiled, catalog: catalog, declaration: d, usedNodes: map[reflect.Type]bool{}, active: map[nodeIdentity]bool{{typ: d.model.GoType()}: true}, ancestorCreators: map[events.TypeRef]int{}}
	if err := c.compileRoot(); err != nil {
		return locate(err)
	}
	if err := c.compileVariantKey(); err != nil {
		return Definition{}, err
	}
	if err := c.compileEntering(); err != nil {
		return locate(err)
	}
	if err := c.finalizeOrdinary(); err != nil {
		return locate(err)
	}
	return Definition{data: compiled}, nil
}

func prepareOrdinaryDefinition(d *declaration, catalog *events.Catalog) (*definition, error) {
	if err := validateOrdinaryInputs(d, catalog); err != nil {
		return nil, err
	}
	bound, err := readmodels.BindProjection(d.model, d.id, d.sequence, d.passive)
	if err != nil {
		return nil, err
	}
	initialState, err := prepareInitialState(d.model, d.initialState)
	if err != nil {
		return nil, err
	}
	compiled := &definition{id: d.id, model: bound, sequence: d.sequence, passive: d.passive, notRewindable: d.notRewindable, variant: d.variant, sequenceExplicit: d.sequenceExplicit, initialState: initialState, labels: slices.Clone(d.labels)}
	for _, event := range d.aliases {
		if err := validateEvent(catalog, event); err != nil {
			return nil, err
		}
	}
	return compiled, nil
}

func validateOrdinaryInputs(d *declaration, catalog *events.Catalog) error {
	if d.err != nil {
		return d.err
	}
	if d.globalFor != nil {
		if d.initialState != "" || len(d.labels) != 0 {
			return invalid("global handlers cannot author initial state or artifact labels")
		}
	}
	if !validOrdinaryIdentity(d, catalog) {
		return invalid("model, catalog, projection identity and sequence required")
	}
	return nil
}

func validOrdinaryIdentity(d *declaration, catalog *events.Catalog) bool {
	if d.model.GoType() == nil || catalog == nil {
		return false
	}
	return !blank(d.id) && !blank(string(d.sequence))
}

func (c *compiler) compileRoot() error {
	d := c.declaration
	node, err := c.compileNode(d, d.model.Fields(), nil, false, false, "", nil, false, false)
	if err != nil {
		return err
	}
	c.result.nodeDefinition = *node
	return nil
}

func (c *compiler) compileVariantKey() error {
	d, compiled := c.declaration, c.result
	if d.variantKey == "" {
		return nil
	}
	field, ok := binaryMappingField(d.model.Fields(), d.variantKey)
	if ok && binaryField(field) {
		return binaryMappingFailure(compiled, field, events.TypeRef{}, "VariantKey", "binary identities are not supported")
	}
	if !ok || !validVariantKeyField(field, d.variantKeyType) {
		return declarationFailure(d.id, Provenance{GoField: field.GoField, Path: d.variantKey, Directive: "VariantKey", Offset: -1}, invalid("variant key requires a non-nullable non-enum scalar model field"))
	}
	if compiled.keyField != "" && compiled.keyField != d.variantKey {
		return declarationFailure(d.id, Provenance{Offset: -1}, invalid("conflicting variant keys"))
	}
	compiled.keyField = d.variantKey
	return nil
}

func validVariantKeyField(field serialization.Field, typ reflect.Type) bool {
	if field.IsEnum() || field.Type != typ {
		return false
	}
	if field.Nullable || field.Collection {
		return false
	}
	return field.Scalar.IsPrimitive()
}

func (c *compiler) compileEntering() error {
	for _, entering := range c.declaration.entering {
		if err := c.addEntering(entering); err != nil {
			return err
		}
	}
	return nil
}

func (c *compiler) addEntering(entering subscription) error {
	compiled := c.result
	if err := c.validateSubscription(entering); err != nil {
		return err
	}
	if compiled.passive && entering.key.kind != sourceExpression {
		return invalid("passive key redirection is not supported by immediate instance reads")
	}
	if entering.parent.kind != emptyExpression {
		return invalid("EntersOn does not accept a parent key")
	}
	if slices.ContainsFunc(compiled.entering, func(e fromDefinition) bool { return e.event == entering.event.Ref() }) {
		return invalid("duplicate entering event")
	}
	compiled.entering = append(compiled.entering, fromDefinition{event: entering.event.Ref(), key: entering.key})
	return nil
}

func (c *compiler) finalizeOrdinary() error {
	d, compiled := c.declaration, c.result
	if len(c.usedNodes) != len(d.nodes) {
		return invalid("WithNodes contains an unreachable node declaration")
	}
	// Model-bound globals deliberately share bare target names across all nodes,
	// matching C#'s one root All dictionary, not derivative FromEvery groups.
	for _, g := range c.globals {
		if err := c.addGlobal(&compiled.nodeDefinition, g.globalDeclaration, g.fields, true); err != nil {
			return err
		}
	}
	if err := validateDerivedChildGlobals(&compiled.nodeDefinition, nil); err != nil {
		return err
	}
	if err := c.validateGlobalSources(); err != nil {
		return err
	}
	if c.requiresSubscription() && !hasSubscriptions(&compiled.nodeDefinition) {
		return invalid("projection must subscribe to at least one registered event")
	}
	return nil
}

func (c *compiler) validateGlobalSources() error {
	d := c.declaration
	if d.globalFor == nil {
		return nil
	}
	// C# merges only explicit root From properties. AutoMap is kernel-owned,
	// and globals are never sent to the kernel, so a bare From has no effect.
	for _, from := range c.result.from {
		if len(from.writes) == 0 {
			return declarationFailure(d.id, Provenance{Directive: "FromEvent", Offset: -1, Event: from.event}, &GlobalFromHasNoProperties{Global: d.model.GoType(), Event: from.event})
		}
	}
	return nil
}

func (c *compiler) requiresSubscription() bool {
	if c.declaration.globalFor != nil || c.declaration.variant != nil {
		return false
	}
	return !c.result.subscribesAll
}

func addWrite(d *definition, from *fromDefinition, w write, modelFields, eventFields []serialization.Field, overwrite bool) error {
	target, err := resolveWriteTarget(modelFields, w, eventFields, false)
	if err != nil {
		return declarationFailure(d.id, w.provenance, err)
	}
	w.provenance.GoField = target.GoField
	if err := validateTarget(target, w.targetType); err != nil {
		return declarationFailure(d.id, w.provenance, err)
	}
	if w.expression.kind == removeExpression {
		return mergeWrite(d, &from.writes, w, overwrite)
	}
	if err := validateExpression(w.expression, target, modelFields, eventFields, w.sourceType); err != nil {
		return declarationFailure(d.id, w.provenance, err)
	}
	return mergeWrite(d, &from.writes, w, overwrite)
}

func mergeWrite(d *definition, writes *[]write, w write, overwrite bool) error {
	for i, previous := range *writes {
		if previous.path != w.path || previous.entry != w.entry {
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

var mappingPriorities = map[string]int{
	"set": 1, "add": 2, "subtract": 3, "increment": 4,
	"decrement": 5, "count": 6, "context": 7, "value": 8,
	"clear": 9, "join": 10, "every": 11, "all": 12,
}

func priority(name string) int { return mappingPriorities[name] }
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
	if !ok {
		return invalid("event handle does not belong to the frozen store catalog")
	}
	if registered.GoType() != event.GoType() || registered.Schema() != event.Schema() {
		return invalid("event handle does not belong to the frozen store catalog")
	}
	if registered.SourceStore() != event.SourceStore() || !sameEventEnumProfiles(registered, event) {
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
		return resolvePersistedEvent(catalog, reference)
	}
	candidates := eventReferenceCandidates(catalog, reference)
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	if len(candidates) > 1 {
		return events.Descriptor{}, ambiguousEventReference(candidates)
	}
	return events.Descriptor{}, invalid("event reference is not registered")
}

func resolvePersistedEvent(catalog *events.Catalog, reference declarations.Value) (events.Descriptor, error) {
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

func eventReferenceCandidates(catalog *events.Catalog, reference declarations.Value) []events.Descriptor {
	var candidates []events.Descriptor
	for _, event := range catalog.Descriptors() {
		if matchesEventReference(event.GoType(), reference) {
			candidates = append(candidates, event)
		}
	}
	return candidates
}

func matchesEventReference(typ reflect.Type, reference declarations.Value) bool {
	switch reference.Kind {
	case declarations.Name:
		return typ.Name() == reference.Text
	case declarations.Call:
		return reference.Text == "go" && typ.PkgPath()+"."+typ.Name() == reference.Args[0].Value.Text
	}
	return false
}

func ambiguousEventReference(candidates []events.Descriptor) error {
	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		typ := candidate.GoType()
		names = append(names, typ.PkgPath()+"."+typ.Name())
	}
	slices.Sort(names)
	return invalid(fmt.Sprintf("ambiguous event name; qualified candidates: %s", strings.Join(names, ", ")))
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
