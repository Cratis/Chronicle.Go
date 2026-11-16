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
	schema, err := plan.ProtectedSchema(modelProtection(config)...)
	if err != nil {
		return "", err
	}
	var root map[string]any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return "", err
	}
	if err := validateModelSchema(root, plan, config); err != nil {
		return "", err
	}
	data, err := json.Marshal(root)
	return string(data), err
}

func modelProtection(config modelConfig) []compliance.Declaration {
	options := append([]compliance.Declaration(nil), config.protection...)
	for _, path := range config.pii {
		options = append(options, compliance.Property(path, compliance.Classification{PII: true}))
	}
	return options
}

func validateModelSchema(root map[string]any, plan *serialization.Plan, config modelConfig) error {
	if err := reservedProperties(root); err != nil {
		return err
	}
	for _, paths := range [][]string{config.indexes, config.pii} {
		if err := validateSchemaPaths(root, paths); err != nil {
			return err
		}
	}
	if err := validateBinaryIndexes(plan, config.indexes); err != nil {
		return err
	}
	if err := validateBinaryIdentity(plan); err != nil {
		return err
	}
	return validateSchemaSubject(plan, config.subject)
}

func validateSchemaPaths(root map[string]any, paths []string) error {
	seen := make(map[string]bool)
	for _, path := range paths {
		if seen[path] {
			return invalid("duplicate property path: " + path)
		}
		seen[path] = true
		if schemaProperty(root, path) == nil {
			return invalid("unknown serialized property path: " + path)
		}
	}
	return nil
}

func validateBinaryIndexes(plan *serialization.Plan, paths []string) error {
	for _, path := range paths {
		field, ok := serialization.FieldAtWithCapability(plan.Fields(), path, serialization.Field.ContainsBinary)
		if ok && field.ContainsBinary() {
			return fmt.Errorf("%w: binary index is not supported: %s", faults.ErrUnsupported, path)
		}
	}
	return nil
}

func validateBinaryIdentity(plan *serialization.Plan) error {
	for _, field := range serialization.EmittedRootFields(plan.Fields()) {
		if field.ContainsBinary() && isIdentityField(field) {
			return fmt.Errorf("%w: binary identity is not supported: %s", faults.ErrUnsupported, field.Path)
		}
	}
	return nil
}

func isIdentityField(field serialization.Field) bool {
	return materializedIdentityAlias(field.Name) || strings.EqualFold(field.GoField, "id")
}

func validateSchemaSubject(plan *serialization.Plan, subject string) error {
	if subject == "" {
		return nil
	}
	field, ok := serialization.FieldAtWithCapability(plan.Fields(), subject, serialization.Field.ContainsBinary)
	if ok && field.ContainsBinary() {
		return fmt.Errorf("%w: binary subject is not supported: %s", faults.ErrUnsupported, subject)
	}
	if !isTopLevelScalar(field, ok, subject) {
		return invalid("subject must name a top-level scalar property")
	}
	return nil
}

func isTopLevelScalar(field serialization.Field, found bool, path string) bool {
	return !strings.Contains(path, ".") && found && field.Scalar.IsPrimitive()
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
	return reservedCollectionProperties(node)
}

func reservedCollectionProperties(node map[string]any) error {
	for _, key := range []string{"items", "additionalProperties"} {
		if child, ok := node[key].(map[string]any); ok {
			if err := reservedProperties(child); err != nil {
				return err
			}
		}
	}
	return nil
}
