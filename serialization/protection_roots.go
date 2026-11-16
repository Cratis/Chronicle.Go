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
// subject. It follows local references and collection/composition/dependency
// schemas without looping. Boolean subschemas are unclassified; instance-valued
// default, enum, const and examples are not inspected as schemas. Unknown metadata
// types still count as protection. Malformed metadata, unresolved references,
// ambiguous JSON and protection-bearing unknown extensions fail with a payload-free
// error. Definitions are validated; protection applies through references.
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
	result, err := inspect.rootProperties()
	if err != nil {
		return nil, err
	}
	// Protection outside named root properties cannot be represented by this
	// API's release groups. Never reinterpret it as an unprotected document.
	outside := withoutRootProperties(root)
	if protected, _, err := inspect.walk(outside, map[string]bool{}, 0); err != nil || protected {
		return nil, faults.ErrInvalidConfiguration
	}
	return result, nil
}

type protectionInspector struct {
	root   map[string]any
	visits int
}

func (i *protectionInspector) rootProperties() (map[string]bool, error) {
	result := map[string]bool{}
	properties, _ := i.root["properties"].(map[string]any) // walk validated shape.
	for name, property := range properties {
		protected, subject, err := i.walk(property, map[string]bool{}, 0)
		if err != nil {
			return nil, err
		}
		if protected {
			result[name] = subject
		}
	}
	return result, nil
}

func withoutRootProperties(root map[string]any) map[string]any {
	outside := make(map[string]any, len(root))
	for key, value := range root {
		if key != "properties" {
			outside[key] = value
		}
	}
	return outside
}

type protectionWalk struct {
	inspector *protectionInspector
	active    map[string]bool
	depth     int
	protected bool
	subject   bool
}

func (i *protectionInspector) walk(value any, active map[string]bool, depth int) (bool, bool, error) {
	// Reference graphs can expand exponentially despite bounded JSON depth.
	// Refuse excessive work rather than treating an unfinished walk as plain.
	i.visits++
	if depth > jsonstructure.MaxDepth || i.visits > 100_000 {
		return false, false, faults.ErrInvalidConfiguration
	}
	if _, boolean := value.(bool); boolean {
		return false, false, nil
	}
	node, ok := value.(map[string]any)
	if !ok {
		return false, false, faults.ErrInvalidConfiguration
	}
	protected, subject, err := schemaClassification(node)
	if err != nil {
		return false, false, err
	}
	walk := protectionWalk{inspector: i, active: active, depth: depth, protected: protected, subject: subject}
	for _, step := range []func(map[string]any) error{
		walk.reference, walk.properties, walk.definitions, walk.dependencies,
		walk.singleSchemas, walk.compositions, walk.extensions,
	} {
		if err := step(node); err != nil {
			return false, false, err
		}
	}
	return walk.protected, walk.subject, nil
}

func (w *protectionWalk) visit(child any) error {
	p, s, err := w.inspector.walk(child, w.active, w.depth+1)
	w.protected, w.subject = w.protected || p, w.subject || s
	return err
}

func (w *protectionWalk) reference(node map[string]any) error {
	value, exists := node["$ref"]
	if !exists {
		return nil
	}
	ref, ok := value.(string)
	if !ok {
		return faults.ErrInvalidConfiguration
	}
	target, err := w.inspector.reference(ref)
	if err != nil {
		return err
	}
	if w.active[ref] {
		return nil
	}
	w.active[ref] = true
	err = w.visit(target)
	delete(w.active, ref)
	return err
}

func (w *protectionWalk) properties(node map[string]any) error {
	for _, keyword := range []string{"properties", "patternProperties"} {
		if value, exists := node[keyword]; exists {
			if err := visitSchemaMap(value, w.visit); err != nil {
				return err
			}
		}
	}
	return nil
}

func visitSchemaMap(value any, visit func(any) error) error {
	children, ok := value.(map[string]any)
	if !ok {
		return faults.ErrInvalidConfiguration
	}
	for _, child := range children {
		if err := visit(child); err != nil {
			return err
		}
	}
	return nil
}

func (w *protectionWalk) definitions(node map[string]any) error {
	// Definitions store schemas, but do not apply them to the current instance.
	// Validate even unused definitions; only a reference propagates protection.
	value, exists := node["definitions"]
	if !exists {
		return nil
	}
	return visitSchemaMap(value, func(definition any) error {
		_, _, err := w.inspector.walk(definition, w.active, w.depth+1)
		return err
	})
}

func (w *protectionWalk) dependencies(node map[string]any) error {
	value, exists := node["dependencies"]
	if !exists {
		return nil
	}
	dependencies, ok := value.(map[string]any)
	if !ok {
		return faults.ErrInvalidConfiguration
	}
	for name, dependency := range dependencies {
		if err := w.dependency(name, dependency); err != nil {
			return err
		}
	}
	return nil
}

func (w *protectionWalk) dependency(name string, dependency any) error {
	if strings.TrimSpace(name) == "" {
		return faults.ErrInvalidConfiguration
	}
	if names, propertyDependency := dependency.([]any); propertyDependency {
		return validatePropertyDependencies(names)
	}
	return w.visit(dependency)
}

func validatePropertyDependencies(names []any) error {
	seen := make(map[string]bool, len(names))
	for _, value := range names {
		name, ok := value.(string)
		if !ok || seen[name] {
			return faults.ErrInvalidConfiguration
		}
		seen[name] = true
	}
	return nil
}

func (w *protectionWalk) singleSchemas(node map[string]any) error {
	for _, keyword := range []string{"items", "additionalProperties", "additionalItems", "propertyNames", "contains", "not", "if", "then", "else"} {
		child, exists := node[keyword]
		if !exists {
			continue
		}
		if _, boolean := child.(bool); boolean {
			continue
		}
		if err := w.visit(child); err != nil {
			return err
		}
	}
	return nil
}

func (w *protectionWalk) compositions(node map[string]any) error {
	for _, keyword := range []string{"allOf", "anyOf", "oneOf"} {
		if value, exists := node[keyword]; exists {
			if err := w.composition(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *protectionWalk) composition(value any) error {
	children, ok := value.([]any)
	if !ok || len(children) == 0 {
		return faults.ErrInvalidConfiguration
	}
	for _, child := range children {
		if err := w.visit(child); err != nil {
			return err
		}
	}
	return nil
}

func (w *protectionWalk) extensions(node map[string]any) error {
	// Unknown extensions are not schema edges we can interpret. Refuse plausible
	// protection in them rather than silently declaring the document plain.
	// Instance-valued keywords are deliberately excluded: a default/enum value
	// may legitimately have fields named security, compliance or $ref.
	for keyword, value := range node {
		switch keyword {
		case "$ref", "properties", "patternProperties", "definitions", "dependencies",
			"items", "additionalProperties", "additionalItems", "propertyNames", "contains",
			"not", "if", "then", "else", "allOf", "anyOf", "oneOf", "security", "compliance",
			"default", "enum", "const", "examples":
			continue
		}
		if err := w.inspector.checkUnknown(value, w.depth+1); err != nil {
			return err
		}
	}
	return nil
}

// checkUnknown does not classify instance data as a schema. It rejects ambiguous
// extension containers containing metadata or references, with the same budget
// as the recognized schema walk and without exposing their contents.
func (i *protectionInspector) checkUnknown(value any, depth int) error {
	i.visits++
	if depth > jsonstructure.MaxDepth || i.visits > 100_000 {
		return faults.ErrInvalidConfiguration
	}
	switch value := value.(type) {
	case map[string]any:
		return i.checkUnknownObject(value, depth)
	case []any:
		for _, child := range value {
			if err := i.checkUnknown(child, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *protectionInspector) checkUnknownObject(value map[string]any, depth int) error {
	for name, child := range value {
		switch name {
		case "security", "compliance", "$ref":
			return faults.ErrInvalidConfiguration
		}
		if err := i.checkUnknown(child, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func schemaClassification(node map[string]any) (protected, subject bool, err error) {
	for _, category := range []string{"compliance", "security"} {
		value, exists := node[category]
		if !exists {
			continue
		}
		p, s, err := metadataClassification(value)
		if err != nil {
			return false, false, err
		}
		protected, subject = protected || p, subject || s
	}
	return protected, subject, nil
}

func metadataClassification(value any) (protected, subject bool, err error) {
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
	switch value.(type) {
	case map[string]any, bool:
		return value, nil
	default:
		return nil, faults.ErrInvalidConfiguration
	}
}
