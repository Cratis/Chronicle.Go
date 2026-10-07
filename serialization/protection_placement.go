// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
)

// The kernel traverses declared object properties, dictionary values
// (additionalProperties) and array items; since 19.32.2 it applies metadata
// beneath unprotected maps (Chronicle#4551) and protects classified
// collection-valued array elements as a whole (Chronicle#4552). A protected
// container is handled as a blob before that traversal. Validate the compiled
// graph, including references, so providers, tags and inherited
// classifications obey the same fail-closed binary placement rule.
type protectionPlacement struct {
	covered bool
}

type placementReference struct {
	ref string
	protectionPlacement
}

func validateProtectionPlacement(node map[string]any, definitions map[string]any, placement protectionPlacement, active map[placementReference]bool) error {
	if ref, ok := node["$ref"].(string); ok {
		key := placementReference{ref, placement}
		if active[key] {
			return nil
		}
		active[key] = true
		defer delete(active, key)
		target, ok := definitions[strings.TrimPrefix(ref, "#/definitions/")].(map[string]any)
		if !ok {
			return protectionError("unresolved protected schema reference")
		}
		return validateProtectionPlacement(target, definitions, placement, active)
	}
	protected := node["compliance"] != nil || node["security"] != nil
	if format, _ := node["format"].(string); strings.TrimSuffix(format, "?") == "byte-array" && (protected || placement.covered) {
		return &declarations.DeclarationError{Directive: "protection", Offset: -1, Message: "binary protection is not supported", Cause: faults.ErrUnsupported}
	}
	placement.covered = placement.covered || protected
	properties, _ := node["properties"].(map[string]any)
	for _, property := range properties {
		if err := validateProtectionPlacement(property.(map[string]any), definitions, placement, active); err != nil {
			return err
		}
	}
	for _, key := range []string{"items", "additionalProperties"} {
		if child, ok := node[key].(map[string]any); ok {
			if err := validateProtectionPlacement(child, definitions, placement, active); err != nil {
				return err
			}
		}
	}
	return nil
}

// NestedCollectionProtection reports whether a compiled schema declares
// protection beneath an unprotected map (Chronicle#4551) or on a
// collection-valued array element (Chronicle#4552). Kernels before 19.32.2 skip
// that metadata and store those values unprotected. Malformed schemas and
// unresolved references report true so callers fail closed.
func NestedCollectionProtection(schema string) bool {
	var root map[string]any
	if json.Unmarshal([]byte(schema), &root) != nil || root == nil {
		return true
	}
	definitions, _ := root["definitions"].(map[string]any)
	found, err := nestedCollectionProtection(root, definitions, nestedPlacement{}, map[nestedReference]bool{})
	return found || err
}

type nestedPlacement struct {
	covered, mapValue, arrayItem bool
}

type nestedReference struct {
	ref string
	nestedPlacement
}

func nestedCollectionProtection(node map[string]any, definitions map[string]any, placement nestedPlacement, active map[nestedReference]bool) (bool, bool) {
	if ref, ok := node["$ref"].(string); ok {
		key := nestedReference{ref, placement}
		if active[key] {
			return false, false
		}
		active[key] = true
		defer delete(active, key)
		target, ok := definitions[strings.TrimPrefix(ref, "#/definitions/")].(map[string]any)
		if !ok {
			return false, true
		}
		return nestedCollectionProtection(target, definitions, placement, active)
	}
	protected := node["compliance"] != nil || node["security"] != nil
	if protected && !placement.covered {
		if placement.mapValue || (placement.arrayItem && (node["items"] != nil || node["additionalProperties"] != nil)) {
			return true, false
		}
	}
	placement.covered = placement.covered || protected
	properties, _ := node["properties"].(map[string]any)
	for _, property := range properties {
		child, ok := property.(map[string]any)
		if !ok {
			continue
		}
		next := placement
		next.arrayItem = false
		if found, failed := nestedCollectionProtection(child, definitions, next, active); found || failed {
			return found, failed
		}
	}
	if items, ok := node["items"].(map[string]any); ok {
		next := placement
		next.arrayItem = true
		if found, failed := nestedCollectionProtection(items, definitions, next, active); found || failed {
			return found, failed
		}
	}
	if values, ok := node["additionalProperties"].(map[string]any); ok {
		next := placement
		next.mapValue, next.arrayItem = true, false
		if found, failed := nestedCollectionProtection(values, definitions, next, active); found || failed {
			return found, failed
		}
	}
	return false, false
}
