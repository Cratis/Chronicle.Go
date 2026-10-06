// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
)

// The kernel only traverses declared object properties and array items. A
// protected property container is handled as a blob before that traversal.
// Validate the compiled graph, including references, so providers, tags and
// inherited classifications obey the same fail-closed placement rules.
type protectionPlacement struct {
	covered, mapValue, arrayItem bool
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
	if protected && !placement.covered {
		if placement.mapValue {
			return protectionError("protection beneath an unprotected map is not supported by the kernel; protect the entire map property")
		}
		if placement.arrayItem && (node["items"] != nil || node["additionalProperties"] != nil) {
			return protectionError("protection on collection-valued array elements is not supported by the kernel")
		}
	}
	placement.covered = placement.covered || protected
	properties, _ := node["properties"].(map[string]any)
	for _, property := range properties {
		child := placement
		child.arrayItem = false
		if err := validateProtectionPlacement(property.(map[string]any), definitions, child, active); err != nil {
			return err
		}
	}
	if items, ok := node["items"].(map[string]any); ok {
		child := placement
		child.arrayItem = true
		if err := validateProtectionPlacement(items, definitions, child, active); err != nil {
			return err
		}
	}
	if values, ok := node["additionalProperties"].(map[string]any); ok {
		child := placement
		child.mapValue, child.arrayItem = true, false
		if err := validateProtectionPlacement(values, definitions, child, active); err != nil {
			return err
		}
	}
	return nil
}
