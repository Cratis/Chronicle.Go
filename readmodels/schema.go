// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func modelSchema(plan *serialization.Plan, config modelConfig) (string, error) {
	options := append([]compliance.Declaration(nil), config.protection...)
	for _, path := range config.pii {
		options = append(options, compliance.Property(path, compliance.Classification{PII: true}))
	}
	schema, err := plan.ProtectedSchema(options...)
	if err != nil {
		return "", err
	}
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
	for _, path := range config.indexes {
		field, ok := serialization.FieldAtWithCapability(plan.Fields(), path, serialization.Field.ContainsBinary)
		if ok && field.ContainsBinary() {
			return "", fmt.Errorf("%w: binary index is not supported: %s", faults.ErrUnsupported, path)
		}
	}
	for _, field := range serialization.EmittedRootFields(plan.Fields()) {
		if field.ContainsBinary() && (materializedIdentityAlias(field.Name) || strings.EqualFold(field.GoField, "id")) {
			return "", fmt.Errorf("%w: binary identity is not supported: %s", faults.ErrUnsupported, field.Path)
		}
	}
	if config.subject != "" {
		field, ok := serialization.FieldAtWithCapability(plan.Fields(), config.subject, serialization.Field.ContainsBinary)
		if ok && field.ContainsBinary() {
			return "", fmt.Errorf("%w: binary subject is not supported: %s", faults.ErrUnsupported, config.subject)
		}
		if strings.Contains(config.subject, ".") || !ok || !field.Scalar.IsPrimitive() {
			return "", invalid("subject must name a top-level scalar property")
		}
	}
	data, err := json.Marshal(root)
	return string(data), err
}

func schemaProperty(root map[string]any, path string) map[string]any {
	definitions, _ := root["definitions"].(map[string]any)
	resolve := func(node map[string]any) map[string]any {
		if ref, ok := node["$ref"].(string); ok && strings.HasPrefix(ref, "#/definitions/") {
			node, _ = definitions[strings.TrimPrefix(ref, "#/definitions/")].(map[string]any)
		}
		return node
	}
	for _, part := range strings.Split(path, ".") {
		root = resolve(root)
		if items, ok := root["items"].(map[string]any); ok {
			root = resolve(items)
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
