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

// noOpModelDirectives are model directives handled elsewhere that need no mapping here.
var noOpModelDirectives = map[string]bool{
	"children": true, "nested": true, "index": true, "subject": true,
	"pii": true, "compliance-details": true, "encrypted": true,
}

// eventDirective is a model directive resolved against the event it maps.
type eventDirective struct {
	field     serialization.Field
	directive declarations.Directive
	event     events.Descriptor
	p         Provenance
}

func (d eventDirective) write() write {
	return write{path: d.field.Path, expression: mappingExpression(d.directive, d.field.Name), provenance: d.p}
}

func (b *nodeBuilder) addModelFields() error {
	for _, field := range serialization.RootFields(b.fields) {
		if err := b.addModelField(field); err != nil {
			return err
		}
	}
	return nil
}

func parseModelDirectives(field serialization.Field) ([]declarations.Directive, error) {
	directives, err := declarations.Parse(declarations.V1, field.Tag)
	if err != nil {
		return nil, err
	}
	if err = declarations.Validate(declarations.Model, directives); err != nil {
		return nil, err
	}
	slices.SortStableFunc(directives, func(a, b declarations.Directive) int { return cmp.Compare(priority(a.Name), priority(b.Name)) })
	return directives, nil
}

func (b *nodeBuilder) addModelField(field serialization.Field) error {
	directives, err := parseModelDirectives(field)
	if err != nil {
		return err
	}
	structural := hasDirective(directives, "children") || hasDirective(directives, "nested")
	if err = b.expandIfStructural(field, directives, structural); err != nil {
		return err
	}
	for _, directive := range directives {
		if err = b.applyDirective(field, directive, structural); err != nil {
			return err
		}
	}
	return b.checkEmbeddedDeclarations(field, structural)
}

func (b *nodeBuilder) expandIfStructural(field serialization.Field, directives []declarations.Directive, structural bool) error {
	if !structural {
		return nil
	}
	return b.expandStructural(field, directives)
}

func (b *nodeBuilder) expandStructural(field serialization.Field, directives []declarations.Directive) error {
	if !b.d.modelBound {
		return invalid("model mapping tags and fluent mappings cannot be mixed")
	}
	if b.stopExpanding {
		return nil
	}
	return b.c.boundChild(b.n, field, b.fields, directives)
}

func (b *nodeBuilder) applyDirective(field serialization.Field, directive declarations.Directive, structural bool) error {
	p := Provenance{FrontEnd: "model-bound", GoField: field.GoField, Path: field.Path, Directive: directive.Name, Offset: directive.Offset}
	switch {
	case noOpModelDirectives[directive.Name]:
		return nil
	case directive.Name == "key":
		return b.applyKey(field, p)
	case directive.Name == "no-auto" || directive.Name == "not-projected":
		b.excludeField(field, p)
		return nil
	}
	return b.applyMappingDirective(field, directive, structural, p)
}

func (b *nodeBuilder) excludeField(field serialization.Field, p Provenance) {
	if !slices.Contains(b.n.exclusions, field.Path) {
		b.n.exclusions = append(b.n.exclusions, field.Path)
	}
	b.c.result.provenance = append(b.c.result.provenance, p)
}

func (b *nodeBuilder) applyMappingDirective(field serialization.Field, directive declarations.Directive, structural bool, p Provenance) error {
	if !b.d.modelBound {
		return b.failure(p, invalid("model mapping tags and fluent mappings cannot be mixed"))
	}
	if structural && isStructuralRemoval(directive.Name) {
		return nil
	}
	if isGlobalDirective(directive.Name) {
		b.addGlobalDirective(field, directive, p)
		return nil
	}
	return b.applyEventDirective(field, directive, p)
}

func isStructuralRemoval(name string) bool {
	return isRemovalDirective(name) || name == "clear"
}

func isGlobalDirective(name string) bool {
	return name == "every" || name == "all"
}

func isRemovalDirective(name string) bool {
	return name == "remove" || name == "remove-join"
}

func canBeKeyField(field serialization.Field) bool {
	return !field.IsEnum() && field.Scalar.IsPrimitive() && !field.Nullable
}

func (b *nodeBuilder) applyKey(field serialization.Field, p Provenance) error {
	if b.n.keyField != "" || !canBeKeyField(field) {
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

// isParentOwnedDirective reports whether a directive writes event state that a parent node already owns.
func isParentOwnedDirective(name string) bool {
	rank := priority(name)
	return rank >= 1 && rank <= 6 || name == "clear"
}

func (b *nodeBuilder) isChildOfSharedEvent(event events.Descriptor) bool {
	return b.parentFields != nil && !b.nested && !b.recursive && b.childEvents[event.Ref()]
}

// leavesDirectiveToChild reports whether a child node ignores the directive because the parent owns its event.
func (b *nodeBuilder) leavesDirectiveToChild(directive declarations.Directive, event events.Descriptor) bool {
	if !b.isChildOfSharedEvent(event) {
		return false
	}
	return isParentOwnedDirective(directive.Name)
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
	d := eventDirective{field: field, directive: directive, event: event, p: p}
	switch {
	case isRemovalDirective(directive.Name):
		return b.applyRemovalDirective(d)
	case directive.Name == "join":
		return b.applyJoinDirective(d)
	}
	return b.applyFromDirective(d)
}

func (b *nodeBuilder) applyRemovalDirective(d eventDirective) error {
	if err := b.c.addRemoval(b.n, removalFromTag(d.directive, d.event), true); err != nil {
		return b.failure(d.p, err)
	}
	return nil
}

func (b *nodeBuilder) applyFromDirective(d eventDirective) error {
	from := b.ensure(d.event)
	if a, ok := argument(d.directive, "key"); ok {
		if err := b.overrideKey(from, d.event, parsedKey(a), d.p); err != nil {
			return err
		}
	}
	return addWrite(b.c.result, from, d.write(), b.fields, d.event.Fields(), true)
}

func (b *nodeBuilder) applyJoinDirective(d eventDirective) error {
	on := d.field.Path
	if a, ok := argument(d.directive, "on"); ok {
		on = a.Text
	}
	s := newSubscription(d.event, nil)
	s.writes = []write{d.write()}
	if err := b.c.addJoin(b.n, joinDeclaration{subscription: s, on: on}, b.fields, true); err != nil {
		return b.failure(d.p, err)
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
	if err := b.checkNestedDeclarations(field); err != nil {
		return err
	}
	return b.checkDerivedDeclarations(field)
}

func (b *nodeBuilder) checkNestedDeclarations(field serialization.Field) error {
	if b.n.children[field.Path] != nil || b.n.nested[field.Path] != nil {
		return nil
	}
	for _, f := range b.fields {
		if strings.HasPrefix(f.Path, field.Path+".") && hasProjectionDirective(f.Tag) {
			return declarationFailure(b.c.result.id, Provenance{GoField: f.GoField, Path: f.Path, Offset: 0}, invalid("nested declarations require children or nested"))
		}
	}
	return nil
}

func (b *nodeBuilder) checkDerivedDeclarations(field serialization.Field) error {
	if b.n.children[field.Path] != nil {
		return nil
	}
	for _, f := range b.fields {
		if isSelfOrDescendant(f, field) && derivativeHasDirective(f, derivedDeclarationDirective) {
			return declarationFailure(b.c.result.id, Provenance{GoField: f.GoField, Path: f.Path, Offset: -1}, invalid("declarations on a derived type require a children collection of its family"))
		}
	}
	return nil
}

func isSelfOrDescendant(f, field serialization.Field) bool {
	return f.Path == field.Path || strings.HasPrefix(f.Path, field.Path+".")
}
