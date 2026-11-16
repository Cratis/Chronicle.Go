// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// Binary capability metadata comes from the compiled codec, never Go kinds or
// the JSON string shape. Objects containing binary need the same qualification.
func binaryField(field serialization.Field) bool { return field.ContainsBinary() }

func binaryUnsupported(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrUnsupported, message)
}

func binaryMappingFailure(d *definition, target serialization.Field, event events.TypeRef, directive, message string) error {
	return declarationFailure(d.id, Provenance{FrontEnd: "compiled", GoField: target.GoField, Path: target.Path, Directive: directive, Offset: -1, Event: event}, binaryUnsupported(message))
}

func binaryMappingField(fields []serialization.Field, path string) (serialization.Field, bool) {
	return serialization.FieldAtWithCapability(fields, path, binaryField)
}

func validateBinaryGraph(d *definition, catalog *events.Catalog) error {
	fields := d.model.Fields()
	for _, f := range serialization.EmittedRootFields(fields) {
		if binaryIdentity(f) {
			return binaryMappingFailure(d, f, events.TypeRef{}, "key", "binary identities are not supported")
		}
	}
	if err := validateBinaryAllEvents(d, fields); err != nil {
		return err
	}
	noAuto := d.noAuto && !d.inheritAuto
	return validateBinaryNode(d, &d.nodeDefinition, fields, catalog, noAuto, noAuto)
}

func binaryIdentity(f serialization.Field) bool {
	identity := strings.EqualFold(f.Name, "id") || f.Name == "_id" || strings.EqualFold(lastGoName(f.GoField), "Id")
	return identity && binaryField(f)
}

func validateBinaryAllEvents(d *definition, fields []serialization.Field) error {
	if !d.subscribesAll {
		return nil
	}
	for _, w := range d.all {
		if target, ok := binaryMappingField(fields, w.path); ok && binaryField(target) {
			return binaryMappingFailure(d, target, events.TypeRef{}, "all", "all-event binary mappings cannot validate unknown representations")
		}
	}
	return nil
}

type binaryNodeValidator struct {
	d                        *definition
	n                        *nodeDefinition
	fields                   []serialization.Field
	catalog                  *events.Catalog
	noAuto, projectionNoAuto bool
}

func validateBinaryNode(d *definition, n *nodeDefinition, fields []serialization.Field, catalog *events.Catalog, noAuto, projectionNoAuto bool) error {
	v := binaryNodeValidator{d: d, n: n, fields: fields, catalog: catalog, noAuto: noAuto, projectionNoAuto: projectionNoAuto}
	return v.validate()
}

func (v *binaryNodeValidator) validate() error {
	checks := []func() error{v.identities, v.removals, v.from, v.joins, v.children, v.nested}
	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (v *binaryNodeValidator) identities() error {
	for _, path := range []string{v.n.keyField, v.n.identifiedBy} {
		if path == "" || path == "$eventSourceId" || path == "*NotSet*" {
			continue
		}
		if field, ok := binaryMappingField(v.fields, path); ok && binaryField(field) {
			return binaryMappingFailure(v.d, field, events.TypeRef{}, "key", "binary identities are not supported")
		}
	}
	return nil
}

func (v *binaryNodeValidator) keys(ref events.TypeRef, key, parent expression) error {
	event, ok := v.catalog.LookupRef(ref)
	if !ok {
		return invalid("binary mapping event is not registered")
	}
	for _, e := range []expression{key, parent} {
		if field, ok := binaryKeyField(event.Fields(), e); ok {
			return binaryMappingFailure(v.d, field, ref, "key", "binary correlation keys are not supported")
		}
	}
	return nil
}

func (v *binaryNodeValidator) removals() error {
	for _, removal := range v.n.removals {
		if err := v.keys(removal.event, removal.key, removal.parent); err != nil {
			return err
		}
	}
	return nil
}

func (v *binaryNodeValidator) from() error {
	for _, from := range v.n.from {
		if err := v.mapping(from, false); err != nil {
			return err
		}
	}
	return nil
}

func (v *binaryNodeValidator) joins() error {
	for _, join := range v.n.joins {
		if target, ok := binaryMappingField(v.fields, join.on); ok && binaryField(target) {
			return binaryMappingFailure(v.d, target, join.event, "join", "binary correlation keys are not supported")
		}
		if err := v.mapping(join.fromDefinition, true); err != nil {
			return err
		}
	}
	return nil
}

func (v *binaryNodeValidator) children() error {
	for _, path := range sortedKeys(v.n.children) {
		child := v.n.children[path]
		childNoAuto := child.noAuto && !child.inheritAuto
		validator := binaryNodeValidator{d: v.d, n: child, fields: scopedFields(v.fields, path), catalog: v.catalog, noAuto: childNoAuto, projectionNoAuto: childNoAuto}
		if err := validator.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (v *binaryNodeValidator) nested() error {
	for _, path := range sortedKeys(v.n.nested) {
		nested := v.n.nested[path]
		nestedNoAuto := nested.noAuto
		if nested.inheritAuto {
			nestedNoAuto = v.projectionNoAuto
		}
		validator := binaryNodeValidator{d: v.d, n: nested, fields: scopedFields(v.fields, path), catalog: v.catalog, noAuto: nestedNoAuto, projectionNoAuto: v.projectionNoAuto}
		if err := validator.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (v *binaryNodeValidator) mapping(from fromDefinition, join bool) error {
	if err := v.keys(from.event, from.key, from.parent); err != nil {
		return err
	}
	event, ok := v.catalog.LookupRef(from.event)
	if !ok {
		return invalid("binary mapping event is not registered")
	}
	mapping := binaryMappingValidator{node: v, from: from, join: join, eventFields: event.Fields(), sources: enumRootFields(event.Fields()), targets: enumRootFields(v.fields)}
	if err := mapping.writes(); err != nil {
		return err
	}
	if v.noAuto || !join && aggregateOnly(from.writes) {
		return nil
	}
	return mapping.autoMap()
}

type binaryMappingValidator struct {
	node                          *binaryNodeValidator
	from                          fromDefinition
	join                          bool
	eventFields, sources, targets []serialization.Field
}

func (v *binaryMappingValidator) failure(target serialization.Field, directive, message string) error {
	return binaryMappingFailure(v.node.d, target, v.from.event, directive, message)
}

func (v *binaryMappingValidator) writes() error {
	for _, w := range append(slices.Clone(v.node.n.all), v.from.writes...) {
		target, _ := binaryMappingField(v.node.fields, w.path)
		var source serialization.Field
		switch w.expression.kind {
		case pathExpression, addExpression, subtractExpression:
			source, _ = binaryMappingField(v.eventFields, w.expression.text)
		}
		if binaryField(target) || binaryField(source) {
			return v.failure(target, "set", "binary is only qualified for same-representation AutoMap")
		}
	}
	return nil
}

func (v *binaryMappingValidator) asciiNames() error {
	if !slices.ContainsFunc(v.targets, binaryField) && !slices.ContainsFunc(v.sources, binaryField) {
		return nil
	}
	// Kernel AutoMap uses CLR OrdinalIgnoreCase, whose Unicode pairs
	// are not the ASCII-only matcher's pairs. Qualify before matching.
	if asciiFieldNames(v.targets) && asciiFieldNames(v.sources) && asciiWrites(v.from.writes) {
		return nil
	}
	candidates := append(slices.Clone(v.targets), v.sources...)
	field := candidates[slices.IndexFunc(candidates, binaryField)]
	return v.failure(field, "AutoMap", "binary auto-map requires ASCII property names; CLR Unicode comparison is not qualified")
}

func (v *binaryMappingValidator) autoMap() error {
	if err := v.asciiNames(); err != nil {
		return err
	}
	for _, source := range v.sources {
		if err := v.autoMapSource(source); err != nil {
			return err
		}
	}
	return nil
}

func (v *binaryMappingValidator) binaryCandidates(source serialization.Field, matches []serialization.Field) bool {
	// AutoMap emits property paths from raw root names. Inspect every
	// candidate for those paths, not just the matched emitted fields.
	sourceCandidate, _ := binaryMappingField(v.eventFields, source.Path)
	binaryTarget := slices.ContainsFunc(matches, func(target serialization.Field) bool {
		candidate, _ := binaryMappingField(v.node.fields, target.Path)
		return binaryField(candidate)
	})
	return binaryField(sourceCandidate) || binaryTarget
}

func (v *binaryMappingValidator) autoMapSource(source serialization.Field) error {
	matches := matchingASCIIFields(v.targets, source.Name)
	if len(matches) == 0 {
		return nil
	}
	if !v.binaryCandidates(source, matches) {
		return nil
	}
	target := matches[0]
	if slices.Contains(v.node.n.exclusions, target.Path) || autoMapWritten(v.from.writes, source.Name, v.join) {
		return nil
	}
	if v.join || v.node.d.variant != nil {
		directive := "VariantOf"
		if v.join {
			directive = "join"
		}
		return v.failure(target, directive, "binary copies through joins or variants are not qualified")
	}
	return v.leafCopy(source, target, matches)
}

func (v *binaryMappingValidator) leafCopy(source, target serialization.Field, matches []serialization.Field) error {
	if source.Scalar != serialization.Binary || target.Scalar != serialization.Binary {
		return v.failure(target, "AutoMap", "binary auto-map only supports leaf-to-leaf copies, not objects containing binary")
	}
	if !v.unambiguousPaths(source, target) || !v.sameRepresentation(source, target, matches) {
		return v.failure(target, "AutoMap", "auto-map binary representations must match unambiguously")
	}
	return nil
}

func (v *binaryMappingValidator) unambiguousPaths(source, target serialization.Field) bool {
	return eventPropertyPath(source.Name) && enumPropertySegments(source, v.eventFields) && enumPropertySegments(target, v.node.fields)
}

func (v *binaryMappingValidator) sameRepresentation(source, target serialization.Field, matches []serialization.Field) bool {
	return len(matches) == 1 && len(matchingASCIIFields(v.sources, source.Name)) == 1 && target.SameRepresentation(source)
}
