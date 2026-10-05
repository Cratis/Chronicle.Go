// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints

import (
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type declaredConstraint struct {
	name         string
	kind         Type
	fields       []EventFields
	types        []events.Descriptor
	sequences    []events.SequenceID
	unrestricted bool
	messages     map[events.TypeID]string
	message      string
}

// CompileDeclarations aggregates model-bound event declarations using the same
// builders as explicit constraints. Catalog order, then field order, determines
// composite parts and first-message precedence. It performs no I/O and returns
// detached definitions. NewClient calls it on each store's frozen, named catalog.
// Same-event same-name fields form one composite (a deliberate C# improvement).
func CompileDeclarations(catalog *events.Catalog) ([]Definition, error) {
	if catalog == nil {
		return nil, declarationError(events.Descriptor{}, serialization.Field{}, "unique", -1, "catalog required")
	}
	var groups []*declaredConstraint
	byName := map[string]*declaredConstraint{}
	add := func(event events.Descriptor, field serialization.Field, directive declarations.Directive, unique events.Unique, kind Type) error {
		name := unique.Name
		if name == "" {
			name = event.GoType().Name()
			if kind == Unique {
				name = field.GoField
			}
		}
		group := byName[name]
		if group == nil {
			group = &declaredConstraint{name: name, kind: kind, messages: map[events.TypeID]string{}}
			byName[name] = group
			groups = append(groups, group)
		} else if group.kind != kind {
			return declarationError(event, field, directive.Name, directive.Offset, "property and event-type constraints cannot share a name")
		}
		if unique.Message != "" {
			if group.message == "" {
				group.message = unique.Message
			}
			if group.messages[event.Ref().ID] == "" {
				group.messages[event.Ref().ID] = unique.Message
			}
		}
		if len(unique.EventSequences) == 0 {
			group.unrestricted = true
		}
		for _, sequence := range unique.EventSequences {
			if !slices.Contains(group.sequences, sequence) {
				group.sequences = append(group.sequences, sequence)
			}
		}
		index := slices.IndexFunc(group.types, func(d events.Descriptor) bool { return d.Ref() == event.Ref() })
		if index < 0 {
			index = len(group.types)
			group.types = append(group.types, event)
			group.fields = append(group.fields, EventFields{Event: event})
		}
		if kind == Unique {
			group.fields[index].Properties = append(group.fields[index].Properties, field.Path)
		}
		return nil
	}
	for _, event := range catalog.Descriptors() {
		if err := event.ValidateUnique(); err != nil {
			return nil, err
		}
		for _, unique := range event.UniqueDeclarations() {
			if err := add(event, serialization.Field{}, declarations.Directive{Name: "unique", Offset: -1}, unique, UniqueEventType); err != nil {
				return nil, err
			}
		}
		for _, field := range event.Fields() {
			directives, err := declarations.Parse(declarations.V1, field.Tag)
			if err != nil {
				return nil, err
			}
			for _, directive := range directives {
				if directive.Name != "unique" {
					continue
				}
				unique := events.Unique{}
				for _, argument := range directive.Args {
					switch argument.Name {
					case "name":
						unique.Name = argument.Value.Text
					case "message":
						unique.Message = argument.Value.Text
					case "sequences":
						for _, item := range argument.Value.Args {
							unique.EventSequences = append(unique.EventSequences, events.SequenceID(item.Value.Text))
						}
					}
				}
				if err := add(event, field, directive, unique, Unique); err != nil {
					return nil, err
				}
			}
		}
	}
	removers := map[string][]events.Descriptor{}
	for _, event := range catalog.Descriptors() {
		for _, name := range event.RemovedConstraints() {
			if strings.TrimSpace(name) == "" || byName[name] == nil {
				return nil, declarationError(event, serialization.Field{}, "remove-constraint", -1, "removal requires a declared constraint; compose explicit definitions with the builder")
			}
			removers[name] = append(removers[name], event)
		}
	}
	var result []Definition
	for _, group := range groups {
		builder := UniqueValues(group.name).WithMessage(group.message)
		if group.kind == UniqueEventType {
			builder = UniqueEventTypes(group.types...).WithName(group.name).WithMessageProvider(func(violation Violation) string {
				if own := group.messages[violation.EventTypeID]; own != "" {
					return own
				}
				return group.message
			})
		} else {
			for _, field := range group.fields {
				builder.On(field.Event, field.Properties...)
			}
		}
		if !group.unrestricted {
			builder.ForEventSequences(group.sequences...)
		}
		definition, err := builder.RemovedWith(removers[group.name]...).Build()
		if err != nil {
			return nil, &declarations.DeclarationError{Artifact: group.name, Directive: "unique", Offset: -1, Message: "invalid compiled constraint", Cause: err}
		}
		result = append(result, definition)
	}
	return result, nil
}

func declarationError(event events.Descriptor, field serialization.Field, directive string, offset int, message string) error {
	artifact := "events"
	if event.GoType() != nil {
		artifact = event.GoType().String()
	}
	return &declarations.DeclarationError{Artifact: artifact, GoField: field.GoField, Path: field.Path, Directive: directive, Offset: offset, Message: message, Cause: faults.ErrInvalidConfiguration}
}

// ToBuilder returns a detached builder for explicit composition with a compiled
// model-bound definition. Changes cannot mutate the original definition. Retained
// message providers have the same concurrency contract as WithMessageProvider.
func (d Definition) ToBuilder() *Builder {
	d.fields, d.types, d.removers, d.sequences = d.Fields(), d.EventTypes(), d.RemovalTypes(), d.EventSequences()
	return &Builder{definition: d}
}
