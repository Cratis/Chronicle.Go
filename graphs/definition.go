// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"encoding/json"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/cratis/chronicle.go/contracts/bcl"
	projections "github.com/cratis/chronicle.go/contracts/projections"
	readmodels "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
)

// Definition is the immutable result of compiling a graph: the read model the
// graph is registered as, and the projection that fills it. Its zero value is
// invalid. Copies are safe for concurrent use.
type Definition struct{ data *definition }

type definition struct {
	name       string
	sink       SinkType
	schema     string
	nodes      []string
	edges      []string
	events     []events.TypeRef
	readModel  *readmodels.ReadModelDefinition
	projection *projections.ProjectionDefinition
	views      GraphViewDefinition
}

// Name returns the name of the graph, which is also the identifier of its
// projection and of its read model.
func (d Definition) Name() string {
	if d.data == nil {
		return ""
	}
	return d.data.name
}

// Sink returns the sink that stores the graph.
func (d Definition) Sink() SinkType {
	if d.data == nil {
		return ""
	}
	return d.data.sink
}

// Schema returns the read model schema the graph is registered with. The graph
// is described by x-graph-* annotations inside an ordinary JSON schema: the root
// is marked x-graph, and every node and edge is a property of the root.
func (d Definition) Schema() string {
	if d.data == nil {
		return ""
	}
	return d.data.schema
}

// Nodes returns the node labels in declaration order.
func (d Definition) Nodes() []string {
	if d.data == nil {
		return nil
	}
	return slices.Clone(d.data.nodes)
}

// Edges returns the edge types in declaration order.
func (d Definition) Edges() []string {
	if d.data == nil {
		return nil
	}
	return slices.Clone(d.data.edges)
}

// Events returns the event types the graph observes, once each.
func (d Definition) Events() []events.TypeRef {
	if d.data == nil {
		return nil
	}
	return slices.Clone(d.data.events)
}

// ReadModel returns a new wire definition of the read model the graph is stored as.
func (d Definition) ReadModel() *readmodels.ReadModelDefinition {
	if d.data == nil {
		return nil
	}
	return proto.Clone(d.data.readModel).(*readmodels.ReadModelDefinition)
}

// Projection returns a new wire definition of the projection that fills the graph.
func (d Definition) Projection() *projections.ProjectionDefinition {
	if d.data == nil {
		return nil
	}
	return proto.Clone(d.data.projection).(*projections.ProjectionDefinition)
}

// Views returns the canonical definition of the views declared on the graph. It
// has no views for a graph that declares none. Views describe how the graph is
// read; they are not part of the read model schema or the projection. Reading
// through views needs a host that executes them.
func (d Definition) Views() GraphViewDefinition {
	if d.data == nil {
		return GraphViewDefinition{}
	}
	views := make([]ViewDefinition, len(d.data.views.Views))
	for i, view := range d.data.views.Views {
		views[i] = ViewDefinition{Name: view.Name, Root: view.Root, Members: slices.Clone(view.Members)}
	}
	if len(views) == 0 {
		views = nil
	}
	schema := ViewSchema{Nodes: slices.Clone(d.data.views.Schema.Nodes), Edges: slices.Clone(d.data.views.Schema.Edges)}
	return GraphViewDefinition{Graph: d.data.views.Graph, Schema: schema, Views: views}
}

func (c *compiler) definition(nodes []*compiledNode, edges []*compiledEdge) (Definition, error) {
	data := &definition{name: c.graph, sink: c.sink, events: slices.Clone(c.used)}
	graph := &projections.GraphProjectionDefinition{
		Nodes: make(map[string]*projections.GraphNodeProjectionDefinition),
		Edges: make(map[string]*projections.GraphEdgeProjectionDefinition),
	}
	root := map[string]any{}
	for _, node := range nodes {
		data.nodes = append(data.nodes, node.label)
		graph.Nodes[node.label] = node.projection
		root[node.label] = elementSchema(node.shape, map[string]any{"x-graph-kind": "Node", "x-graph-key": node.key})
	}
	for _, edge := range edges {
		data.edges = append(data.edges, edge.name)
		graph.Edges[edge.name] = edge.projection
		annotations := map[string]any{"x-graph-kind": "Edge", "x-graph-source": edge.source, "x-graph-target": edge.target}
		if edge.identity != "" {
			annotations["x-graph-identity"] = edge.identity
		}
		root[edge.name] = elementSchema(edge.shape, annotations)
	}
	schema, err := json.Marshal(map[string]any{"type": "object", "title": c.graph, "x-graph": true, "properties": root})
	if err != nil {
		return Definition{}, refuse(c.graph, "", ErrInvalidGraph, "the schema cannot be written")
	}
	data.schema = string(schema)
	data.readModel = &readmodels.ReadModelDefinition{
		Type:               &readmodels.ReadModelType{Identifier: c.graph, Generation: 1},
		ContainerName:      c.graph,
		DisplayName:        c.graph,
		Sink:               &readmodels.SinkDefinition{ConfigurationId: &bcl.Guid{}, TypeId: string(c.sink)},
		Schema:             data.schema,
		ObserverType:       readmodels.ReadModelObserverType_Projection,
		ObserverIdentifier: c.graph,
	}
	data.projection = &projections.ProjectionDefinition{
		EventSequenceId:   string(events.EventLog),
		Identifier:        c.graph,
		ReadModel:         c.graph,
		IsActive:          true,
		IsRewindable:      true,
		InitialModelState: "{}",
		All:               &projections.FromEveryDefinition{},
		Graph:             graph,
	}
	return Definition{data: data}, nil
}

// elementSchema describes the properties of a node or an edge with the schemas the
// client uses for every other struct-backed model, so a property has the same type
// here as it has anywhere, and adds the graph annotations.
func elementSchema(shape *shape, annotations map[string]any) map[string]any {
	properties := make(map[string]json.RawMessage, len(shape.properties))
	for _, property := range shape.properties {
		properties[property.name] = property.schema
	}
	result := map[string]any{"type": "object", "properties": properties}
	for key, value := range annotations {
		result[key] = value
	}
	return result
}
