// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"errors"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/serialization"
)

var errNilOption = errors.New("nil option")

// SinkType identifies the kernel sink provider that stores a graph.
type SinkType string

const (
	// Neo4j stores the graph in Neo4j. It is the default graph sink.
	Neo4j SinkType = "Neo4j"
	// InMemoryGraph keeps the graph in memory, for development and specifications.
	InMemoryGraph SinkType = "InMemoryGraph"
)

// Graph declares one property graph: its nodes and edges, stored by one sink.
// The name of the graph is also the identifier of its projection and of the read
// model the graph is registered as. It is not safe for concurrent mutation.
type Graph struct {
	name     string
	sink     SinkType
	elements []Element
	views    []*View
}

// New declares a graph with its nodes and edges.
func New(name string, elements ...Element) *Graph {
	return &Graph{name: name, elements: slices.Clone(elements)}
}

// UsingSink stores the graph in the sink. Without it the client's default graph
// sink applies, Neo4j unless chronicle.WithDefaultGraphSinkType says otherwise.
func (g *Graph) UsingSink(kind SinkType) *Graph {
	g.sink = kind
	return g
}

// Declaration is an immutable, validated snapshot of a graph. Its zero value is
// invalid. Pass it to chronicle.Registry.AddGraph. Event types, property names
// and the naming policy are resolved when the client is prepared.
type Declaration struct{ data *declaration }

type declaration struct {
	name  string
	sink  SinkType
	nodes []*Node
	edges []*Edge
	views []*viewSnapshot
}

// Name returns the name of the graph.
func (d Declaration) Name() string {
	if d.data == nil {
		return ""
	}
	return d.data.name
}

// Build validates everything that does not depend on the client's naming policy
// and the registered events, and freezes the result. Property names, event types
// and mappings are validated against them when the client is prepared.
func (g *Graph) Build() (Declaration, error) {
	if g == nil {
		return Declaration{}, refuse("", "", ErrInvalidGraph, "a graph is required")
	}
	if strings.TrimSpace(g.name) == "" {
		return Declaration{}, refuse(g.name, "", ErrInvalidGraph, "a graph needs a name")
	}
	switch g.sink {
	case "", Neo4j, InMemoryGraph:
	default:
		return Declaration{}, refuse(g.name, "", ErrInvalidGraph, "the graph sink must be Neo4j or InMemoryGraph")
	}
	data := &declaration{name: g.name, sink: g.sink}
	index := make(map[*Node]*Node)
	edgeIndex := make(map[*Edge]*Edge)
	var edges []*Edge
	for _, element := range g.elements {
		switch element := element.(type) {
		case *Node:
			if element == nil {
				return Declaration{}, refuse(g.name, "", ErrInvalidGraph, "a nil node")
			}
			if index[element] == nil {
				index[element] = element.snapshot()
				data.nodes = append(data.nodes, index[element])
			}
		case *Edge:
			if element == nil {
				return Declaration{}, refuse(g.name, "", ErrInvalidGraph, "a nil edge")
			}
			edges = append(edges, element)
		default:
			return Declaration{}, refuse(g.name, "", ErrInvalidGraph, "a graph holds nodes and edges only")
		}
	}
	// Endpoints are resolved to the frozen nodes; one that is not part of the
	// graph stays nil and is refused by validation.
	for _, edge := range edges {
		frozen := edge.snapshot()
		frozen.source, frozen.target = index[edge.source], index[edge.target]
		data.edges = append(data.edges, frozen)
		edgeIndex[edge] = frozen
	}
	if len(data.nodes) == 0 {
		return Declaration{}, refuse(g.name, "", ErrInvalidGraph, "a graph needs at least one node")
	}
	if err := data.validate(); err != nil {
		return Declaration{}, err
	}
	for _, view := range g.views {
		if view == nil {
			return Declaration{}, refuse(g.name, "", ErrInvalidGraph, "a nil view")
		}
	}
	data.views = freezeViews(g.views, index, edgeIndex)
	if _, err := data.checkViews(serialization.PreservePropertyNames); err != nil {
		return Declaration{}, err
	}
	return Declaration{data: data}, nil
}

func (n *Node) snapshot() *Node {
	copy := *n
	copy.from = make([]*nodeFrom, len(n.from))
	for i, from := range n.from {
		item := *from
		item.mappings = slices.Clone(from.mappings)
		copy.from[i] = &item
	}
	copy.removedWith = make([]*nodeRemoval, len(n.removedWith))
	for i, removal := range n.removedWith {
		item := *removal
		copy.removedWith[i] = &item
	}
	copy.problems = slices.Clone(n.problems)
	return &copy
}

func (e *Edge) snapshot() *Edge {
	copy := *e
	copy.from = make([]*edgeFrom, len(e.from))
	for i, from := range e.from {
		item := *from
		item.mappings = slices.Clone(from.mappings)
		copy.from[i] = &item
	}
	copy.removedWith = make([]*edgeRemoval, len(e.removedWith))
	for i, removal := range e.removedWith {
		item := *removal
		copy.removedWith[i] = &item
	}
	copy.problems = slices.Clone(e.problems)
	return &copy
}
