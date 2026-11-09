// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"errors"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

// validate checks what does not depend on the naming policy or on the event
// catalog: the shape of the elements, their uniqueness and their endpoints.
func (d *declaration) validate() error {
	seen := make(map[string]bool)
	claim := func(name string) error {
		if name == "" {
			return refuse(d.name, "", ErrInvalidGraph, "a node label or edge type cannot be empty")
		}
		if seen[name] {
			return refuse(d.name, name, ErrDuplicateElement, "%s is declared more than once", name)
		}
		seen[name] = true
		return nil
	}
	for _, node := range d.nodes {
		if err := claim(node.label); err != nil {
			return err
		}
		if err := node.validate(d.name); err != nil {
			return err
		}
	}
	for _, edge := range d.edges {
		if err := claim(edge.name); err != nil {
			return err
		}
		if err := edge.validate(d.name); err != nil {
			return err
		}
	}
	return nil
}

func problemsOf(graph, element string, problems []error) error {
	if len(problems) == 0 {
		return nil
	}
	return refuse(graph, element, ErrInvalidGraph, "%v", errors.Join(problems...))
}

func (n *Node) validate(graph string) error {
	if err := problemsOf(graph, n.label, n.problems); err != nil {
		return err
	}
	shape, err := analyze(graph, n.label, n.typ, serialization.PreservePropertyNames)
	if err != nil {
		return err
	}
	if _, count := shape.keyCandidate(n.key); count != 1 {
		return refuse(graph, n.label, ErrNodeKey, "%s has %d key properties", n.typ, count)
	}
	subscribed := make(map[events.TypeRef]string)
	for _, from := range n.from {
		if err := claimEvent(graph, n.label, subscribed, from.event, "from"); err != nil {
			return err
		}
		if err := checkMappings(graph, n.label, from.mappings); err != nil {
			return err
		}
	}
	removed := make(map[events.TypeRef]string)
	for _, removal := range n.removedWith {
		if err := claimEvent(graph, n.label, removed, removal.event, "removed with"); err != nil {
			return err
		}
	}
	return nil
}

func (e *Edge) validate(graph string) error {
	if err := problemsOf(graph, e.name, e.problems); err != nil {
		return err
	}
	if e.source == nil || e.target == nil {
		return refuse(graph, e.name, ErrEdgeEndpoints, "the source and the target of %s must be nodes of graph %s", e.typ, graph)
	}
	shape, err := analyze(graph, e.name, e.typ, serialization.PreservePropertyNames)
	if err != nil {
		return err
	}
	if _, count := shape.keyCandidate(e.identity); count > 1 {
		return refuse(graph, e.name, ErrEdgeIdentity, "%s has %d key properties", e.typ, count)
	}
	subscribed := make(map[events.TypeRef]string)
	for _, from := range e.from {
		if err := claimEvent(graph, e.name, subscribed, from.event, "from"); err != nil {
			return err
		}
		if err := checkMappings(graph, e.name, from.mappings); err != nil {
			return err
		}
	}
	removed := make(map[events.TypeRef]string)
	for _, removal := range e.removedWith {
		if err := claimEvent(graph, e.name, removed, removal.event, "removed with"); err != nil {
			return err
		}
	}
	return nil
}

func claimEvent(graph, element string, seen map[events.TypeRef]string, event Event, what string) error {
	if isNil(event) {
		return refuse(graph, element, ErrInvalidGraph, "%s needs an event type", what)
	}
	ref := event.Descriptor().Ref()
	if ref.ID == "" {
		return refuse(graph, element, ErrInvalidGraph, "%s needs a registered event type, not a zero value", what)
	}
	if _, duplicate := seen[ref]; duplicate {
		return refuse(graph, element, ErrInvalidMapping, "event %s is declared more than once %s", ref.ID, what)
	}
	seen[ref] = what
	return nil
}

func checkMappings(graph, element string, mappings []mapping) error {
	properties := make(map[string]bool, len(mappings))
	for _, m := range mappings {
		if m.property == "" {
			return refuse(graph, element, ErrInvalidMapping, "a mapping needs the name of the property it assigns")
		}
		if m.literalErr != nil {
			return refuse(graph, element, ErrInvalidMapping, "property %s: %v", m.property, m.literalErr)
		}
		if properties[m.property] {
			return refuse(graph, element, ErrInvalidMapping, "property %s is mapped more than once for the same event", m.property)
		}
		properties[m.property] = true
	}
	return nil
}

func isNil(event Event) bool {
	if event == nil {
		return true
	}
	return event.Descriptor().GoType() == nil
}
