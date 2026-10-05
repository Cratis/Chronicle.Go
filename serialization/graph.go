// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "strings"

func reachableDefinitions(root map[string]any, definitions map[string]any) map[string]any {
	result := map[string]any{}
	var visit func(any)
	visit = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			if ref, ok := value["$ref"].(string); ok {
				name := strings.TrimPrefix(ref, "#/definitions/")
				if _, exists := result[name]; !exists {
					if target, exists := definitions[name]; exists {
						result[name] = target
						visit(target)
					}
				}
			}
			for key, child := range value {
				if key != "definitions" {
					visit(child)
				}
			}
		case []any:
			for _, child := range value {
				visit(child)
			}
		}
	}
	visit(root)
	return result
}

func visitNodes(n *node, seen map[*node]bool, visit func(*node) error) error {
	if n.reference != nil {
		n = n.reference
	}
	if seen[n] {
		return nil
	}
	seen[n] = true
	if err := visit(n); err != nil {
		return err
	}
	if n.item != nil {
		if err := visitNodes(n.item, seen, visit); err != nil {
			return err
		}
	}
	for _, f := range n.fields {
		if err := visitNodes(f.value, seen, visit); err != nil {
			return err
		}
	}
	for _, d := range n.derivatives {
		if err := visitNodes(d.node, seen, visit); err != nil {
			return err
		}
	}
	return nil
}

func (p *Plan) visitAll(visit func(*node) error) error {
	seen := map[*node]bool{}
	if err := visitNodes(p.root, seen, visit); err != nil {
		return err
	}
	for _, family := range p.families {
		if err := visitNodes(family, seen, visit); err != nil {
			return err
		}
	}
	return nil
}

func hasFamily(n *node, seen map[*node]bool) bool {
	if n.reference != nil {
		n = n.reference
	}
	if seen[n] {
		return false
	}
	seen[n] = true
	if n.family {
		return true
	}
	if n.item != nil && hasFamily(n.item, seen) {
		return true
	}
	for _, f := range n.fields {
		if hasFamily(f.value, seen) {
			return true
		}
	}
	return false
}
