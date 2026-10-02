// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
)

func modelSchema(schema string, config modelConfig) (string, error) {
	var root map[string]any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return "", err
	}
	if err := reservedProperties(root); err != nil {
		return "", err
	}
	for _, paths := range [][]string{config.indexes, config.pii} {
		seen := make(map[string]bool)
		for _, path := range paths {
			if seen[path] {
				return "", invalid("duplicate property path: " + path)
			}
			seen[path] = true
			if schemaProperty(root, path) == nil {
				return "", invalid("unknown serialized property path: " + path)
			}
		}
	}
	for _, path := range config.pii {
		property := schemaProperty(root, path)
		if property["type"] != "string" || property["format"] != nil {
			return "", fmt.Errorf("%w: PII currently requires a scalar string property", faults.ErrUnsupported)
		}
		if path == config.subject || strings.EqualFold(path, "id") {
			return "", invalid("PII cannot protect the model key or subject")
		}
		property["compliance"] = []any{map[string]any{"metadataType": "PII", "details": ""}}
	}
	if config.subject != "" {
		property := schemaProperty(root, config.subject)
		if strings.Contains(config.subject, ".") || property == nil || !stringProperty(property) {
			return "", invalid("subject must name a top-level string property")
		}
	}
	data, err := json.Marshal(root)
	return string(data), err
}

func stringProperty(property map[string]any) bool {
	if property["type"] == "string" {
		return true
	}
	kinds, ok := property["type"].([]any)
	return ok && len(kinds) == 2 && kinds[0] == "string" && kinds[1] == "null"
}

func schemaProperty(root map[string]any, path string) map[string]any {
	for _, part := range strings.Split(path, ".") {
		if items, ok := root["items"].(map[string]any); ok {
			root = items
		}
		properties, _ := root["properties"].(map[string]any)
		root, _ = properties[part].(map[string]any)
		if root == nil {
			return nil
		}
	}
	return root
}

func reservedProperties(node map[string]any) error {
	properties, _ := node["properties"].(map[string]any)
	for name, value := range properties {
		switch strings.ToLower(name) {
		case "_subject", "__subject", "__subjects":
			return invalid("reserved compliance property: " + name)
		}
		if err := reservedProperties(value.(map[string]any)); err != nil {
			return err
		}
	}
	for _, key := range []string{"items", "additionalProperties"} {
		if child, ok := node[key].(map[string]any); ok {
			if err := reservedProperties(child); err != nil {
				return err
			}
		}
	}
	return nil
}
