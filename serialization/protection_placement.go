// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
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
