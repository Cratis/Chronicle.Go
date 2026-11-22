// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import "github.com/cratis/chronicle.go/readmodels"

// ImmediateProjection replays by event-source identity, not a custom key or
// cross-source join. Validate the final graph, including synthesized variants.
func validateNoSinkGraph(d *definition) error {
	if d.passive || d.model.Sink().Type != readmodels.NoSink || sourceKeyedNode(&d.nodeDefinition) {
		return nil
	}
	return invalid("None projections require event-source keys without joins")
}

func sourceKeyedNode(node *nodeDefinition) bool {
	if len(node.joins) != 0 {
		return false
	}
	for _, from := range node.from {
		if !sourceKeyPair(from.key, from.parent) {
			return false
		}
	}
	for _, removal := range node.removals {
		if removal.join || !sourceKeyPair(removal.key, removal.parent) {
			return false
		}
	}
	return sourceKeyedNodes(node.children) && sourceKeyedNodes(node.nested)
}

func sourceKeyedNodes(nodes map[string]*nodeDefinition) bool {
	for _, node := range nodes {
		if !sourceKeyedNode(node) {
			return false
		}
	}
	return true
}

func sourceKeyPair(key, parent expression) bool {
	return sourceIdentity(key) && sourceIdentity(parent)
}

func sourceIdentity(value expression) bool {
	return value.kind == emptyExpression || value.kind == sourceExpression
}
