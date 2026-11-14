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
	// Schema lists the nodes and edges the views were validated against. Only
	// the schema-included form of the serialization writes it.
	Schema ViewSchema
	Views  []ViewDefinition
}

// ViewSchemaNode describes a node the views were validated against.
type ViewSchemaNode struct {
	Label      string
	Key        string
	Properties []string
}

// ViewSchemaEdge describes an edge the views were validated against.
// IdentifiedBy is empty for an edge without an identifying property.
type ViewSchemaEdge struct {
	Type         string
	Source       string
	Target       string
	IdentifiedBy string
	Properties   []string
}

// ViewSchema lists the nodes and edges of the graph, in declaration order, that
// the views were validated against.
type ViewSchema struct {
	Nodes []ViewSchemaNode
	Edges []ViewSchemaEdge
}

// Canonical serializes the definition in its default form, {graph, views}: UTF-8
// JSON with LF line endings, two-space indentation, the layout and property order
// shared by every client, views sorted by name ordinal, members in declaration
// order and one trailing newline.
func (d GraphViewDefinition) Canonical() ([]byte, error) { return d.canonical(false) }

// CanonicalWithSchema serializes the definition in its schema-included form,
// {graph, schema, views}, which also lists the nodes and edges the views were
// validated against. Both forms are identical in everything else.
func (d GraphViewDefinition) CanonicalWithSchema() ([]byte, error) { return d.canonical(true) }

func (d GraphViewDefinition) canonical(withSchema bool) ([]byte, error) {
	var w canonicalWriter
	w.line(0, "{")
	w.line(1, `"graph": `+w.quote(d.Graph)+",")
	if withSchema {
		w.line(1, `"schema": {`)
		w.list(2, `"nodes": [`, len(d.Schema.Nodes), true, func(i int) {
			node := d.Schema.Nodes[i]
			w.line(3, `{ "label": `+w.quote(node.Label)+`, "key": `+w.quote(node.Key)+`, "properties": `+w.array(node.Properties)+` }`+comma(i, len(d.Schema.Nodes)))
		})
		w.list(2, `"edges": [`, len(d.Schema.Edges), false, func(i int) {
			edge := d.Schema.Edges[i]
			identity := ""
			if edge.IdentifiedBy != "" {
				identity = `"identifiedBy": ` + w.quote(edge.IdentifiedBy) + ", "
			}
			w.line(3, `{ "type": `+w.quote(edge.Type)+`, "source": `+w.quote(edge.Source)+`, "target": `+w.quote(edge.Target)+`, `+identity+`"properties": `+w.array(edge.Properties)+` }`+comma(i, len(d.Schema.Edges)))
		})
		w.line(1, "},")
	}
	views := slices.Clone(d.Views)
	slices.SortStableFunc(views, func(a, b ViewDefinition) int { return strings.Compare(a.Name, b.Name) })
	w.list(1, `"views": [`, len(views), false, func(i int) {
		view := views[i]
		w.line(2, "{")
		w.line(3, `"name": `+w.quote(view.Name)+",")
		w.line(3, `"root": `+w.quote(view.Root)+",")
		w.list(3, `"members": [`, len(view.Members), false, func(j int) {
			w.line(4, view.Members[j].inline()+comma(j, len(view.Members)))
		})
		w.line(2, "}"+comma(i, len(views)))
	})
	w.line(0, "}")
	return w.bytes()
}

func (m ViewMember) inline() string {
	var w canonicalWriter
	text := `{ "name": ` + w.quote(m.Name) + `, "kind": ` + w.quote(string(m.Kind))
	if m.Source != "" {
		text += `, "source": ` + w.quote(m.Source)
	}
	if m.Edge != "" {
		text += `, "edge": ` + w.quote(m.Edge)
	}
	if m.Direction != "" {
		text += `, "direction": ` + w.quote(string(m.Direction))
	}
	if m.Cardinality != "" {
		text += `, "cardinality": ` + w.quote(string(m.Cardinality))
	}
	if m.View != "" {
		text += `, "view": ` + w.quote(m.View)
	}
	return text + " }"
}

func comma(index, count int) string {
	if index < count-1 {
		return ","
	}
	return ""
}

// canonicalWriter writes the fixed layout by hand: encoding/json cannot lay out
// inline objects and arrays, and the layout is part of the format.
type canonicalWriter struct {
	buffer bytes.Buffer
	err    error
}

func (w *canonicalWriter) line(depth int, text string) {
	w.buffer.WriteString(strings.Repeat("  ", depth))
	w.buffer.WriteString(text)
	w.buffer.WriteByte('\n')
}

func (w *canonicalWriter) quote(text string) string {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(text); err != nil && w.err == nil {
		w.err = err
	}
	return strings.TrimSuffix(out.String(), "\n")
}

// array writes an inline array with a space inside the brackets, or [] when empty.
func (w *canonicalWriter) array(items []string) string {
	if len(items) == 0 {
		return "[]"
	}
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = w.quote(item)
	}
	return "[ " + strings.Join(quoted, ", ") + " ]"
}

// list writes a list that is empty as [] on one line, and otherwise one line per item.
func (w *canonicalWriter) list(depth int, open string, count int, more bool, item func(int)) {
	separator := ""
	if more {
		separator = ","
	}
	if count == 0 {
		w.line(depth, open+"]"+separator)
		return
	}
	w.line(depth, open)
	for i := 0; i < count; i++ {
		item(i)
	}
	w.line(depth, "]"+separator)
}

func (w *canonicalWriter) bytes() ([]byte, error) { return w.buffer.Bytes(), w.err }
