// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"net/url"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/jsonstructure"
)

// ProtectionRoots returns protected root properties and whether each requires a
// subject. It follows local references and collection/composition schemas without
// looping. Unknown metadata types still count as protection. Malformed metadata,
// unresolved references and ambiguous JSON fail with a payload-free error.
func ProtectionRoots(schema string) (map[string]bool, error) {
	if jsonstructure.Validate([]byte(schema)) != nil {
		return nil, faults.ErrInvalidConfiguration
	}
	var root map[string]any
	if json.Unmarshal([]byte(schema), &root) != nil || root == nil {
		return nil, faults.ErrInvalidConfiguration
	}
	inspect := protectionInspector{root: root}
	if _, _, err := inspect.walk(root, map[string]bool{}, 0); err != nil {
		return nil, err
	}
	result := map[string]bool{}
	properties, _ := root["properties"].(map[string]any) // walk validated shape.
	for name, property := range properties {
		protected, subject, err := inspect.walk(property, map[string]bool{}, 0)
		if err != nil {
			return nil, err
		}
		if protected {
			result[name] = subject
		}
	}
	// Protection outside named root properties cannot be represented by this
	// API's release groups. Never reinterpret it as an unprotected document.
	outside := make(map[string]any, len(root))
	for key, value := range root {
		if key != "properties" {
			outside[key] = value
		}
	}
	if protected, _, err := inspect.walk(outside, map[string]bool{}, 0); err != nil || protected {
		return nil, faults.ErrInvalidConfiguration
	}
	return result, nil
}

type protectionInspector struct {
	root   map[string]any
	visits int
}

func (i *protectionInspector) walk(value any, active map[string]bool, depth int) (protected, subject bool, err error) {
	// Reference graphs can expand exponentially despite bounded JSON depth.
	// Refuse excessive work rather than treating an unfinished walk as plain.
	i.visits++
	if depth > jsonstructure.MaxDepth || i.visits > 100_000 {
		return false, false, faults.ErrInvalidConfiguration
	}
	node, ok := value.(map[string]any)
	if !ok {
		return false, false, faults.ErrInvalidConfiguration
	}
	protected, subject, err = schemaClassification(node)
	if err != nil {
		return false, false, err
	}
	visit := func(child any) error {
		p, s, err := i.walk(child, active, depth+1)
		protected, subject = protected || p, subject || s
		return err
	}
	if value, exists := node["$ref"]; exists {
		ref, ok := value.(string)
		if !ok {
			return false, false, faults.ErrInvalidConfiguration
		}
		target, err := i.reference(ref)
		if err != nil {
			return false, false, err
		}
		if !active[ref] {
			active[ref] = true
			err = visit(target)
			delete(active, ref)
			if err != nil {
				return false, false, err
			}
		}
	}
	for _, keyword := range []string{"properties", "patternProperties"} {
		if value, exists := node[keyword]; exists {
			children, ok := value.(map[string]any)
			if !ok {
				return false, false, faults.ErrInvalidConfiguration
			}
			for _, child := range children {
				if err := visit(child); err != nil {
					return false, false, err
				}
			}
		}
	}
	for _, keyword := range []string{"items", "additionalProperties", "additionalItems", "contains", "not", "if", "then", "else"} {
		if child, exists := node[keyword]; exists {
			if _, boolean := child.(bool); boolean {
				continue
			}
			if err := visit(child); err != nil {
				return false, false, err
			}
		}
	}
	for _, keyword := range []string{"allOf", "anyOf", "oneOf"} {
		if value, exists := node[keyword]; exists {
			children, ok := value.([]any)
			if !ok || len(children) == 0 {
				return false, false, faults.ErrInvalidConfiguration
			}
			for _, child := range children {
				if err := visit(child); err != nil {
					return false, false, err
				}
			}
		}
	}
	return protected, subject, nil
}

func schemaClassification(node map[string]any) (protected, subject bool, err error) {
	for _, category := range []string{"compliance", "security"} {
		value, exists := node[category]
		if !exists {
			continue
		}
		metadata, ok := value.([]any)
		if !ok {
			return false, false, faults.ErrInvalidConfiguration
		}
		for _, entry := range metadata {
			object, ok := entry.(map[string]any)
			if !ok {
				return false, false, faults.ErrInvalidConfiguration
			}
			kind, ok := object["metadataType"].(string)
			if !ok || strings.TrimSpace(kind) == "" {
				return false, false, faults.ErrInvalidConfiguration
			}
			protected = true
			subject = subject || kind == "PII" || kind == "EncryptedSubject"
		}
	}
	return protected, subject, nil
}

func (i protectionInspector) reference(ref string) (any, error) {
	if !strings.HasPrefix(ref, "#/") {
		return nil, faults.ErrInvalidConfiguration
	}
	pointer, err := url.PathUnescape(ref[2:])
	if err != nil {
		return nil, faults.ErrInvalidConfiguration
	}
	var value any = i.root
	for _, token := range strings.Split(pointer, "/") {
		// Resolve JSON Pointer escapes in order, including C# definition names.
		if strings.Contains(strings.ReplaceAll(strings.ReplaceAll(token, "~1", ""), "~0", ""), "~") {
			return nil, faults.ErrInvalidConfiguration
		}
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		object, ok := value.(map[string]any)
		if !ok {
			return nil, faults.ErrInvalidConfiguration
		}
		value, ok = object[token]
		if !ok {
			return nil, faults.ErrInvalidConfiguration
		}
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, faults.ErrInvalidConfiguration
	}
	return value, nil
}
