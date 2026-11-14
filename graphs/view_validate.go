// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/serialization"
)

// ViewDefinition validates the views of the declaration against the naming
// policy of the client and returns their canonical definition, with the views
// sorted by name. A declaration
// without views yields a definition without views. Build has already refused
// every violation that does not depend on the policy.
func (d Declaration) ViewDefinition(policy serialization.NamingPolicy) (GraphViewDefinition, error) {
	if d.data == nil {
		return GraphViewDefinition{}, refuse("", "", ErrInvalidGraph, "a graph declaration is required")
	}
	if err := policy.Validate(); err != nil {
		return GraphViewDefinition{}, err
	}
	return d.data.checkViews(policy)
}

// match finds the property a view member reads: the exact name first, then the
// same name ignoring case. It returns the name the graph gives the property.
func (s *shape) match(name string) (string, bool) {
	if _, ok := s.byName[name]; ok {
		return name, true
	}
	for _, property := range s.properties {
		if strings.EqualFold(property.name, name) {
			return property.name, true
		}
	}
	return "", false
}

// resolveDirection derives the direction of a relation and the label at the far
// end of the edge. A non-empty reason refuses the relation.
func resolveDirection(root string, edge *Edge, explicit ViewDirection) (direction ViewDirection, far string, reason ViewReason, detail string) {
	source, target := edge.source.label, edge.target.label
	if root != source && root != target {
		return "", "", EdgeNotIncidentOnRoot, "edge " + edge.name + " runs from " + source + " to " + target + ", which does not touch " + root
	}
	if source == target {
		if explicit == "" {
			return "", "", AmbiguousDirection, "edge " + edge.name + " is a self edge on " + source + "; say Outgoing or Incoming"
		}
		return explicit, root, "", ""
	}
	derived, far := ViewOutgoing, target
	if root == target {
		derived, far = ViewIncoming, source
	}
	if explicit != "" && explicit != derived {
		return "", "", EdgeNotIncidentOnRoot, "edge " + edge.name + " cannot be followed " + string(explicit) + " from " + root
	}
	return derived, far, "", ""
}

// checkViews validates every view and builds the canonical definition in one
// pass, so a definition is never produced from a declaration that was not checked.
func (d *declaration) checkViews(policy serialization.NamingPolicy) (GraphViewDefinition, error) {
	result := GraphViewDefinition{Graph: d.name}
	if len(d.views) == 0 {
		return result, nil
	}
	schemaTypes := make(map[reflect.Type]bool)
	nodeShapes := make(map[*Node]*shape)
	edgeShapes := make(map[*Edge]*shape)
	for _, node := range d.nodes {
		schemaTypes[node.typ] = true
	}
	for _, edge := range d.edges {
		schemaTypes[edge.typ] = true
	}
	nodeShape := func(node *Node) (*shape, error) {
		if known, ok := nodeShapes[node]; ok {
			return known, nil
		}
		analyzed, err := analyze(d.name, node.label, node.typ, policy)
		if err == nil {
			nodeShapes[node] = analyzed
		}
		return analyzed, err
	}
	edgeShape := func(edge *Edge) (*shape, error) {
		if known, ok := edgeShapes[edge]; ok {
			return known, nil
		}
		analyzed, err := analyze(d.name, edge.name, edge.typ, policy)
		if err == nil {
			edgeShapes[edge] = analyzed
		}
		return analyzed, err
	}

	definitions := make(map[*viewSnapshot]*ViewDefinition, len(d.views))
	followed := make(map[*viewSnapshot][]*Edge)
	names := make(map[string]bool, len(d.views))
	result.Views = make([]ViewDefinition, len(d.views))
	for i, view := range d.views {
		if view.name == "" {
			return GraphViewDefinition{}, refuse(d.name, "", ErrInvalidGraph, "a view needs a name")
		}
		if len(view.problems) > 0 {
			return GraphViewDefinition{}, refuse(d.name, view.name, ErrInvalidGraph, "%v", errors.Join(view.problems...))
		}
		if schemaTypes[view.typ] {
			return GraphViewDefinition{}, refuseView(d.name, view.name, "", ViewIsAlsoSchema, "%s is a node or an edge of the graph and cannot also be a view", view.typ)
		}
		if names[view.name] {
			return GraphViewDefinition{}, refuseView(d.name, view.name, "", DuplicateViewName, "view %s is declared more than once", view.name)
		}
		names[view.name] = true
		if view.root == nil {
			return GraphViewDefinition{}, refuseView(d.name, view.name, "", ViewRootNotANode, "the root of the view is not a node of graph %s", d.name)
		}
		rootShape, err := nodeShape(view.root)
		if err != nil {
			return GraphViewDefinition{}, err
		}
		entry := &result.Views[i]
		*entry = ViewDefinition{Name: view.name, Root: view.root.label, Members: make([]ViewMember, len(view.members))}
		definitions[view] = entry
		members := make(map[string]bool, len(view.members))
		for j, member := range view.members {
			if member.name == "" {
				return GraphViewDefinition{}, refuse(d.name, view.name, ErrInvalidGraph, "a view member needs a name")
			}
			if members[member.name] {
				return GraphViewDefinition{}, refuse(d.name, view.name, ErrInvalidGraph, "member %s is declared more than once", member.name)
			}
			members[member.name] = true
			out := &entry.Members[j]
			*out = ViewMember{Name: member.name, Kind: member.kind}
			switch member.kind {
			case ViewProperty:
				source, ok := rootShape.match(member.source)
				if !ok {
					return GraphViewDefinition{}, refuseView(d.name, view.name, member.name, UnknownNodeProperty, "node %s has no property %q", view.root.label, member.source)
				}
				out.Source = source
			case ViewEdgeProperty:
				out.Source = member.source // resolved against the followed edge below
			case ViewRelation:
				if member.edge == nil {
					return GraphViewDefinition{}, refuseView(d.name, view.name, member.name, UnknownEdge, "the edge is not declared in graph %s", d.name)
				}
				direction, far, reason, detail := resolveDirection(view.root.label, member.edge, member.direction)
				if reason != "" {
					return GraphViewDefinition{}, refuseView(d.name, view.name, member.name, reason, "%s", detail)
				}
				if member.target == nil {
					return GraphViewDefinition{}, refuseView(d.name, view.name, member.name, RelationTargetNotAView, "the target is not a view declared in graph %s", d.name)
				}
				if member.target.root == nil || member.target.root.label != far {
					return GraphViewDefinition{}, refuseView(d.name, view.name, member.name, TargetLabelMismatch, "following %s %s from %s reaches %s, not the root of view %s", member.edge.name, direction, view.root.label, far, member.target.name)
				}
				followed[member.target] = append(followed[member.target], member.edge)
				out.Edge, out.Direction, out.Cardinality, out.View = member.edge.name, direction, member.cardinality, member.target.name
			}
		}
	}
	if err := d.rejectRecursiveViews(); err != nil {
		return GraphViewDefinition{}, err
	}
	for _, view := range d.views {
		if err := d.resolveEdgeProperties(view, definitions[view], followed[view], edgeShape); err != nil {
			return GraphViewDefinition{}, err
		}
	}
	slices.SortStableFunc(result.Views, func(a, b ViewDefinition) int { return strings.Compare(a.Name, b.Name) })
	for _, node := range d.nodes {
		analyzed, err := nodeShape(node)
		if err != nil {
			return GraphViewDefinition{}, err
		}
		key, _ := analyzed.keyCandidate(node.key)
		result.Schema.Nodes = append(result.Schema.Nodes, ViewSchemaNode{Label: node.label, Key: key, Properties: analyzed.names()})
	}
	for _, edge := range d.edges {
		analyzed, err := edgeShape(edge)
		if err != nil {
			return GraphViewDefinition{}, err
		}
		identity, _ := analyzed.keyCandidate(edge.identity)
		result.Schema.Edges = append(result.Schema.Edges, ViewSchemaEdge{Type: edge.name, Source: edge.source.label, Target: edge.target.label, IdentifiedBy: identity, Properties: analyzed.names()})
	}
	return result, nil
}

// names returns the property names in declaration order.
func (s *shape) names() []string {
	names := make([]string, len(s.properties))
	for i, property := range s.properties {
		names[i] = property.name
	}
	return names
}

// resolveEdgeProperties checks the edge property members of a view against the
// one edge type its relations follow.
func (d *declaration) resolveEdgeProperties(view *viewSnapshot, out *ViewDefinition, followed []*Edge, edgeShape func(*Edge) (*shape, error)) error {
	hasEdgeProperties := false
	for _, member := range view.members {
		hasEdgeProperties = hasEdgeProperties || member.kind == ViewEdgeProperty
	}
	if !hasEdgeProperties {
		return nil
	}
	if len(followed) == 0 {
		return refuseView(d.name, view.name, "", EdgePropertyViewNotUsedAsTarget, "no relation targets view %s, so there is no edge to read properties from", view.name)
	}
	edge := followed[0]
	for _, other := range followed[1:] {
		if other != edge {
			return refuseView(d.name, view.name, "", EdgePropertyViewSharedAcrossEdges, "view %s is the target of relations along %s and %s", view.name, edge.name, other.name)
		}
	}
	properties, err := edgeShape(edge)
	if err != nil {
		return err
	}
	for i, member := range view.members {
		if member.kind != ViewEdgeProperty {
			continue
		}
		source, ok := properties.match(member.source)
		if !ok {
			return refuseView(d.name, view.name, member.name, UnknownEdgeProperty, "edge %s has no property %q", edge.name, member.source)
		}
		out.Members[i].Source = source
	}
	return nil
}

// rejectRecursiveViews refuses a view that contains itself, directly or through
// the views it relates to.
func (d *declaration) rejectRecursiveViews() error {
	const (
		visiting = 1
		done     = 2
	)
	state := make(map[*viewSnapshot]int, len(d.views))
	var visit func(view *viewSnapshot) error
	visit = func(view *viewSnapshot) error {
		state[view] = visiting
		for _, member := range view.members {
			if member.kind != ViewRelation || member.target == nil {
				continue
			}
			switch state[member.target] {
			case visiting:
				return refuseView(d.name, view.name, member.name, RecursiveView, "view %s contains itself through %s", member.target.name, view.name)
			case 0:
				if err := visit(member.target); err != nil {
					return err
				}
			}
		}
		state[view] = done
		return nil
	}
	for _, view := range d.views {
		if state[view] == 0 {
			if err := visit(view); err != nil {
				return err
			}
		}
	}
	return nil
}
