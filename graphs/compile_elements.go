// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	projections "github.com/cratis/chronicle.go/contracts/projections"
)

type compiledNode struct {
	label      string
	key        string
	shape      *shape
	projection *projections.GraphNodeProjectionDefinition
}

type compiledEdge struct {
	name       string
	source     string
	target     string
	identity   string
	shape      *shape
	projection *projections.GraphEdgeProjectionDefinition
}

func (c *compiler) node(node *Node) (*compiledNode, error) {
	shape, err := analyze(c.graph, node.label, node.typ, c.policy)
	if err != nil {
		return nil, err
	}
	key, count := shape.keyCandidate(node.key)
	if count != 1 {
		return nil, refuse(c.graph, node.label, ErrNodeKey, "%s has %d key properties", node.typ, count)
	}
	if _, ok := shape.byName[key]; !ok {
		return nil, refuse(c.graph, node.label, ErrUnknownProperty, "the key %q is not a property of %s", key, node.typ)
	}
	compiled := &compiledNode{label: node.label, key: key, shape: shape, projection: &projections.GraphNodeProjectionDefinition{
		From:        make(map[string]*projections.GraphNodeFromDefinition),
		RemovedWith: make(map[string]*projections.GraphNodeRemovedWithDefinition),
	}}
	for _, from := range node.from {
		event, err := c.event(node.label, from.event)
		if err != nil {
			return nil, err
		}
		keyExpression, err := c.keyExpression(node.label, "key", from.key, from.keySet, event)
		if err != nil {
			return nil, err
		}
		properties, err := c.mappings(node.label, shape, key, event, from.mappings)
		if err != nil {
			return nil, err
		}
		compiled.projection.From[eventKey(event.ref)] = &projections.GraphNodeFromDefinition{Key: keyExpression, Properties: properties, UpdateOnly: from.updateOnly}
	}
	for _, removal := range node.removedWith {
		event, err := c.event(node.label, removal.event)
		if err != nil {
			return nil, err
		}
		keyExpression, err := c.keyExpression(node.label, "key", removal.key, removal.keySet, event)
		if err != nil {
			return nil, err
		}
		compiled.projection.RemovedWith[eventKey(event.ref)] = &projections.GraphNodeRemovedWithDefinition{Key: keyExpression, Detach: removal.detach}
	}
	return compiled, nil
}

func (c *compiler) edge(edge *Edge, labels map[*Node]string) (*compiledEdge, error) {
	shape, err := analyze(c.graph, edge.name, edge.typ, c.policy)
	if err != nil {
		return nil, err
	}
	identity := ""
	if candidate, count := shape.keyCandidate(edge.identity); count > 1 {
		return nil, refuse(c.graph, edge.name, ErrEdgeIdentity, "%s has %d key properties", edge.typ, count)
	} else if count == 1 {
		identity = candidate
		if _, ok := shape.byName[identity]; !ok {
			return nil, refuse(c.graph, edge.name, ErrUnknownProperty, "the identifying property %q is not a property of %s", identity, edge.typ)
		}
	}
	compiled := &compiledEdge{name: edge.name, source: labels[edge.source], target: labels[edge.target], identity: identity, shape: shape, projection: &projections.GraphEdgeProjectionDefinition{
		From:        make(map[string]*projections.GraphEdgeFromDefinition),
		RemovedWith: make(map[string]*projections.GraphEdgeRemovedWithDefinition),
	}}
	for _, from := range edge.from {
		event, err := c.event(edge.name, from.event)
		if err != nil {
			return nil, err
		}
		source, target, id, err := c.edgeKeys(edge.name, identity, event, from.source, from.sourceSet, from.target, from.targetSet, from.identity, from.idSet)
		if err != nil {
			return nil, err
		}
		properties, err := c.mappings(edge.name, shape, identity, event, from.mappings)
		if err != nil {
			return nil, err
		}
		compiled.projection.From[eventKey(event.ref)] = &projections.GraphEdgeFromDefinition{SourceKey: source, TargetKey: target, Identity: id, Properties: properties}
	}
	for _, removal := range edge.removedWith {
		event, err := c.event(edge.name, removal.event)
		if err != nil {
			return nil, err
		}
		source, target, id, err := c.edgeKeys(edge.name, identity, event, removal.source, removal.sourceSet, removal.target, removal.targetSet, removal.identity, removal.idSet)
		if err != nil {
			return nil, err
		}
		compiled.projection.RemovedWith[eventKey(event.ref)] = &projections.GraphEdgeRemovedWithDefinition{SourceKey: source, TargetKey: target, Identity: id}
	}
	return compiled, nil
}

// edgeKeys resolves the end node keys of an edge operation and, when the edge has
// an identifying property, its identity: the named event property, or the one
// with the same name as the identifying property.
func (c *compiler) edgeKeys(element, identity string, event *eventShape, source string, sourceSet bool, target string, targetSet bool, id string, idSet bool) (string, string, string, error) {
	sourceKey, err := c.keyExpression(element, "source key", source, sourceSet, event)
	if err != nil {
		return "", "", "", err
	}
	targetKey, err := c.keyExpression(element, "target key", target, targetSet, event)
	if err != nil {
		return "", "", "", err
	}
	if identity == "" {
		if idSet {
			return "", "", "", refuse(c.graph, element, ErrEdgeIdentity, "event type %s names an identity, but the edge has no identifying property", event.ref.ID)
		}
		return sourceKey, targetKey, "", nil
	}
	path := id
	if !idSet {
		path = identity
	}
	if _, err := c.resolvePath(element, "identity", path, event); err != nil {
		return "", "", "", refuse(c.graph, element, ErrEdgeIdentity, "event type %s cannot supply the identity %q: name an event property with UsingIdentity", event.ref.ID, identity)
	}
	return sourceKey, targetKey, path, nil
}
