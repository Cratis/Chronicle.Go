// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
)

// A well-formed response can still be incomplete. Never replace an object with
// an empty reply that silently drops classified leaves. Coarse collection
// protection may legitimately release to an empty collection after erasure.
func validateReleased(schema string, input, output []byte) error {
	var root map[string]any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return faults.ErrProtocol
	}
	definitions, _ := root["definitions"].(map[string]any)
	return validateReleasedNode(root, definitions, input, output, 0)
}

func validateReleasedNode(schema map[string]any, definitions map[string]any, input, output []byte, depth int) error {
	if depth > 256 {
		return faults.ErrProtocol
	}
	if len(input) == 0 || bytes.Equal(bytes.TrimSpace(input), []byte("null")) {
		return nil
	}
	if ref, ok := schema["$ref"].(string); ok {
		target, ok := definitions[strings.TrimPrefix(ref, "#/definitions/")].(map[string]any)
		if !ok {
			return faults.ErrProtocol
		}
		return validateReleasedNode(target, definitions, input, output, depth+1)
	}
	if schema["compliance"] != nil || schema["security"] != nil {
		if len(output) == 0 {
			return faults.ErrProtocol
		}
		return nil
	}
	properties, _ := schema["properties"].(map[string]any)
	if len(properties) > 0 {
		var before, after map[string]json.RawMessage
		if json.Unmarshal(input, &before) != nil {
			return faults.ErrProtocol
		}
		if len(output) != 0 && json.Unmarshal(output, &after) != nil {
			return faults.ErrProtocol
		}
		for name, value := range properties {
			if err := validateReleasedNode(value.(map[string]any), definitions, before[name], after[name], depth+1); err != nil {
				return err
			}
		}
	}
	if items, ok := schema["items"].(map[string]any); ok {
		var before, after []json.RawMessage
		if json.Unmarshal(input, &before) != nil {
			return faults.ErrProtocol
		}
		if len(output) != 0 && json.Unmarshal(output, &after) != nil {
			return faults.ErrProtocol
		}
		for i, value := range before {
			var released json.RawMessage
			if i < len(after) {
				released = after[i]
			}
			if err := validateReleasedNode(items, definitions, value, released, depth+1); err != nil {
				return err
			}
		}
	}
	if values, ok := schema["additionalProperties"].(map[string]any); ok {
		var before, after map[string]json.RawMessage
		if json.Unmarshal(input, &before) != nil {
			return faults.ErrProtocol
		}
		if len(output) != 0 && json.Unmarshal(output, &after) != nil {
			return faults.ErrProtocol
		}
		for name, value := range before {
			if err := validateReleasedNode(values, definitions, value, after[name], depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
