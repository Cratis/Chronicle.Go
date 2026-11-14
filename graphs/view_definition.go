// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
)

// ViewMemberKind is the kind of a member of a graph view.
type ViewMemberKind string

// The kinds of view member.
const (
	// ViewProperty is a property of the root node.
	ViewProperty ViewMemberKind = "property"
	// ViewRelation follows one edge type in one direction to a target view.
	ViewRelation ViewMemberKind = "relation"
	// ViewEdgeProperty is a property of the edge that was followed to reach the view.
	ViewEdgeProperty ViewMemberKind = "edgeProperty"
)

// ViewDirection is the direction in which a relation follows an edge.
type ViewDirection string

// The directions of a relation.
const (
	// ViewOutgoing follows the edge from its source node to its target node.
	ViewOutgoing ViewDirection = "outgoing"
	// ViewIncoming follows the edge from its target node to its source node.
	ViewIncoming ViewDirection = "incoming"
)

// ViewCardinality says whether a relation yields a list or a single value.
type ViewCardinality string

// The cardinalities of a relation.
const (
	// ViewMany yields a list. A relation without related nodes yields an empty list.
	ViewMany ViewCardinality = "many"
	// ViewOne yields a single value. A relation without a related node yields null.
	ViewOne ViewCardinality = "one"
)

// ViewMember is one member of a view definition. Source is set for properties and
// edge properties; Edge, Direction, Cardinality and View are set for relations.
type ViewMember struct {
	Name        string
	Kind        ViewMemberKind
	Source      string
	Edge        string
	Direction   ViewDirection
	Cardinality ViewCardinality
	View        string
}

// ViewDefinition is the canonical description of one view: its name, the label
// of its root node and its members in declaration order.
type ViewDefinition struct {
	Name    string
	Root    string
	Members []ViewMember
}

// GraphViewDefinition is the canonical description of the views declared on a
// graph. It is a conformance and transport format shared by every Chronicle
// client, never authored by hand, and it does not change the graph projection.
type GraphViewDefinition struct {
	Graph string
	Views []ViewDefinition
}

// Canonical serializes the definition as UTF-8 JSON with LF line endings,
// two-space indentation, the property order shared by every client, views sorted
// by name ordinal and one trailing newline.
func (d GraphViewDefinition) Canonical() ([]byte, error) {
	document := canonicalDocument{Graph: d.Graph, Views: make([]canonicalView, 0, len(d.Views))}
	for _, view := range d.Views {
		entry := canonicalView{Name: view.Name, Root: view.Root, Members: make([]canonicalMember, 0, len(view.Members))}
		for _, member := range view.Members {
			entry.Members = append(entry.Members, canonicalMember(member))
		}
		document.Views = append(document.Views, entry)
	}
	slices.SortStableFunc(document.Views, func(a, b canonicalView) int { return strings.Compare(a.Name, b.Name) })
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(document); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// The canonical types fix the order of the JSON properties: encoding/json writes
// struct fields in declaration order, so the order is part of the type.
type canonicalDocument struct {
	Graph string          `json:"graph"`
	Views []canonicalView `json:"views"`
}

type canonicalView struct {
	Name    string            `json:"name"`
	Root    string            `json:"root"`
	Members []canonicalMember `json:"members"`
}

type canonicalMember struct {
	Name        string          `json:"name"`
	Kind        ViewMemberKind  `json:"kind"`
	Source      string          `json:"source,omitempty"`
	Edge        string          `json:"edge,omitempty"`
	Direction   ViewDirection   `json:"direction,omitempty"`
	Cardinality ViewCardinality `json:"cardinality,omitempty"`
	View        string          `json:"view,omitempty"`
}
