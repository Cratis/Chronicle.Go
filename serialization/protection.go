// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
)

// ProtectedSchema compiles tags and explicit classifications through one schema
// pipeline. It never modifies the plan. Composite classification descends to
// leaves; an explicitly classified collection remains a protected container.
// Metadata providers are evaluated only during compilation. PII and confidentiality
// cannot classify the same value, and event-source identities cannot be protected.
func (p *Plan) ProtectedSchema(options ...compliance.Declaration) (string, error) {
	if len(options) == 0 {
		classified := false
		if err := p.visitAll(func(n *node) error {
			for _, f := range n.fields {
				directives, err := declarations.Parse(declarations.V1, f.tag)
				if err != nil {
					return err
				}
				for _, directive := range directives {
					if directive.Name == "pii" || directive.Name == "encrypted" || directive.Name == "compliance-details" {
						classified = true
					}
				}
			}
			return nil
		}); err != nil {
			return "", err
		}
		if !classified && len(p.families) == 0 {
			return p.Schema(), nil
		}
	}
	c := protectionCompiler{types: map[reflect.Type]compliance.Classification{}, paths: map[string]compliance.Classification{}, active: map[protectionKey]map[string]any{}, definitions: map[string]any{}, usedTypes: map[reflect.Type]bool{}}
	for _, option := range options {
		switch {
		case option.TargetType() != nil:
			typ := dereference(option.TargetType())
			if _, exists := c.types[typ]; exists {
				return "", protectionError("duplicate type classification")
			}
			c.types[typ] = option.Metadata()
		case option.Path() != "":
			if _, exists := c.paths[option.Path()]; exists {
				return "", protectionError("duplicate property classification")
			}
			if _, ok := FieldAt(p.Fields(), option.Path()); !ok {
				return "", protectionError("unknown classification property")
			}
			c.paths[option.Path()] = option.Metadata()
		case option.Provider() != nil:
			c.providers = append(c.providers, option.Provider())
		default:
			return "", protectionError("empty classification declaration")
		}
		if err := validateClassification(option.Metadata()); err != nil {
			return "", err
		}
	}
	// Unused families have no document-property address. Audit their type, tag
	// and provider classifications without applying unrelated root path overrides.
	paths := c.paths
	c.paths = nil
	for _, family := range p.families {
		if _, err := c.walk(family, compliance.Classification{}, compliance.Classification{}, ""); err != nil {
			return "", err
		}
	}
	c.paths = paths
	schema, err := c.walk(p.root, compliance.Classification{}, compliance.Classification{}, "")
	if err != nil {
		var declaration *declarations.DeclarationError
		if errors.As(err, &declaration) {
			declaration.Artifact = p.typ.String()
		}
		return "", err
	}
	if c.classified && hasFamily(p.root, map[*node]bool{}) {
		return "", protectionError("active protection cannot traverse an open derived family")
	}
	for typ := range c.types {
		if !c.usedTypes[typ] {
			return "", protectionError("classified type is not reachable from the declaration")
		}
	}
	c.definitions = reachableDefinitions(schema, c.definitions)
	if len(c.definitions) > 0 {
		schema = maps.Clone(schema)
		schema["definitions"] = c.definitions
	}
	if err := validateProtectionCategories(schema, c.definitions, false, false, map[categoryReference]bool{}); err != nil {
		return "", err
	}
	if err := validateProtectionPlacement(schema, c.definitions, protectionPlacement{}, map[placementReference]bool{}); err != nil {
		return "", err
	}
	data, err := json.Marshal(schema)
	return string(data), err
}

type protectionKey struct {
	node     *node
	metadata compliance.Classification
	path     string
}
type protectionCompiler struct {
	types       map[reflect.Type]compliance.Classification
	usedTypes   map[reflect.Type]bool
	paths       map[string]compliance.Classification
	providers   []compliance.Provider
	active      map[protectionKey]map[string]any
	definitions map[string]any
	classified  bool
}

func (c *protectionCompiler) typeMetadata(typ reflect.Type) (compliance.Classification, error) {
	typ = dereference(typ)
	c.usedTypes[typ] = true
	result := c.types[typ]
	for _, provider := range c.providers {
		provided, err := invoke(func() (compliance.Classification, error) { return provider(compliance.Target{Type: typ}) })
		if err != nil {
			return result, providerFailure(err)
		}
		result, err = combineClassification(result, provided)
		if err != nil {
			return result, err
		}
	}
	return result, validateClassification(result)
}

func (c *protectionCompiler) walk(n *node, inherited, member compliance.Classification, path string) (map[string]any, error) {
	if n.reference != nil {
		n = n.reference
	}
	typeMetadata, err := c.typeMetadata(n.typ)
	if err != nil {
		return nil, err
	}
	local, err := combineClassification(member, typeMetadata)
	if err != nil {
		return nil, err
	}
	metadata, err := combineClassification(local, inherited)
	if err != nil {
		return nil, err
	}
	if metadata != (compliance.Classification{}) {
		if hasBinary(n) {
			return nil, &declarations.DeclarationError{Directive: "protection", Offset: -1, Message: "protected binary placements are not supported", Cause: faults.ErrUnsupported}
		}
		if hasEnum(n) {
			return nil, protectionError("protected enum placements are not supported")
		}
		c.classified = true
	}
	if (metadata.PII || metadata.Encrypted) && containsSourceIdentity(n, map[*node]bool{}) {
		return nil, protectionError("event-source identity cannot be protected")
	}
	contextPath := ""
	for target := range c.paths {
		if path == "" || strings.HasPrefix(target, path+".") {
			// Distinguish even the root's pending overrides from a recursive
			// occurrence with no overrides; otherwise they repeat every cycle.
			contextPath = "." + path
			break
		}
	}
	key := protectionKey{n, metadata, contextPath}
	if previous := c.active[key]; previous != nil {
		name := fmt.Sprintf("protected_%x", sha256.Sum256([]byte(n.typ.PkgPath()+n.typ.String()+fmt.Sprint(metadata)+contextPath)))
		c.definitions[name] = previous
		return map[string]any{"$ref": "#/definitions/" + name}, nil
	}
	result := maps.Clone(n.schema)
	delete(result, "definitions")
	c.active[key] = result
	defer delete(c.active, key)
	if n.family {
		if metadata != (compliance.Classification{}) {
			return nil, protectionError("classified derived families are not supported")
		}
		for _, derivative := range n.derivatives {
			classifiedBefore := c.classified
			c.classified = false
			_, err := c.walk(derivative.node, compliance.Classification{}, compliance.Classification{}, path)
			if err != nil {
				return nil, err
			}
			if c.classified {
				return nil, protectionError("classified derived variants are not supported")
			}
			c.classified = classifiedBefore
		}
		return result, nil
	}
	if n.typ.Kind() == reflect.Pointer {
		item, err := c.walk(n.item, inherited, member, path)
		if err != nil {
			return nil, err
		}
		// Rebuild from the protected child, not the original pointer schema:
		// a property override can replace a recursive reference with inline fields.
		clear(result)
		maps.Copy(result, item)
		if format, ok := result["format"].(string); ok {
			result["format"] = strings.TrimSuffix(format, "?") + "?"
			if n.item.binary {
				result["type"] = []string{"string", "null"}
			}
		} else if kind, ok := result["type"].(string); ok {
			result["type"] = []string{kind, "null"}
		}
		return result, nil
	}
	if n.item != nil && n.concept == nil {
		item, err := c.walk(n.item, compliance.Classification{}, compliance.Classification{}, path)
		if err != nil {
			return nil, err
		}
		if n.typ.Kind() == reflect.Map {
			result["additionalProperties"] = item
		} else {
			result["items"] = item
		}
	}
	if len(n.fields) > 0 {
		properties := make(map[string]any, len(n.fields))
		for _, f := range n.fields {
			fieldPath := f.name
			if path != "" {
				fieldPath = path + "." + f.name
			}
			fieldMetadata, err := classificationTag(f.tag)
			if err != nil {
				return nil, err
			}
			if explicit, exists := c.paths[fieldPath]; exists {
				if fieldMetadata.PII || fieldMetadata.Encrypted {
					return nil, protectionError("duplicate field classification")
				}
				fieldMetadata, err = combineClassification(explicit, fieldMetadata)
				if err != nil {
					return nil, err
				}
			}
			declaringType, declaringMetadata := n.typ, typeMetadata
			for _, index := range f.index[:len(f.index)-1] {
				declaringType = dereference(declaringType.Field(index).Type)
				embeddedMetadata, err := c.typeMetadata(declaringType)
				if err != nil {
					return nil, err
				}
				declaringMetadata, err = combineClassification(embeddedMetadata, declaringMetadata)
				if err != nil {
					return nil, err
				}
			}
			for _, provider := range c.providers {
				provided, err := invoke(func() (compliance.Classification, error) {
					return provider(compliance.Target{Type: f.value.typ, DeclaringType: declaringType, Field: n.typ.FieldByIndex(f.index).Name})
				})
				if err != nil {
					return nil, providerFailure(err)
				}
				fieldMetadata, err = combineClassification(fieldMetadata, provided)
				if err != nil {
					return nil, err
				}
			}
			// Member, declaring type, then member type: C# attribute precedence.
			fieldMetadata, err = combineClassification(fieldMetadata, declaringMetadata)
			if err != nil {
				return nil, err
			}
			properties[f.name], err = c.walk(f.value, metadata, fieldMetadata, fieldPath)
			if err != nil {
				var declaration *declarations.DeclarationError
				if errors.As(err, &declaration) && declaration.Path == "" {
					declaration.Path, declaration.GoField = fieldPath, f.goName
				}
				return nil, err
			}
		}
		result["properties"] = properties
		return result, nil
	}
	if metadata.PII {
		result["compliance"] = []any{map[string]any{"metadataType": "PII", "details": metadata.Details}}
	}
	if metadata.Encrypted {
		kind := map[compliance.Scope]string{compliance.Subject: "EncryptedSubject", compliance.Namespace: "EncryptedNamespace", compliance.Global: "EncryptedGlobal"}[metadata.Scope]
		result["security"] = []any{map[string]any{"metadataType": kind, "details": metadata.Details}}
	}
	return result, nil
}

func classificationTag(tag string) (compliance.Classification, error) {
	result := compliance.Classification{}
	directives, err := declarations.Parse(declarations.V1, tag)
	if err != nil {
		return result, err
	}
	seen := map[string]bool{}
	for _, directive := range directives {
		if directive.Name != "pii" && directive.Name != "encrypted" && directive.Name != "compliance-details" {
			continue
		}
		if seen[directive.Name] {
			return result, protectionError("duplicate classification directive")
		}
		seen[directive.Name] = true
		switch directive.Name {
		case "pii":
			result.PII = true
		case "encrypted":
			result.Encrypted, result.Scope, result.DetailsSet = true, compliance.Subject, true
			for _, arg := range directive.Args {
				if arg.Name == "scope" {
					result.Scope = compliance.Scope(arg.Value.Text)
				}
				if arg.Name == "details" {
					result.Details = arg.Value.Text
				}
			}
		case "compliance-details":
			if len(directive.Args) == 1 {
				result.Details, result.DetailsSet = directive.Args[0].Value.Text, true
			}
		}
	}
	if seen["encrypted"] && seen["compliance-details"] {
		return result, protectionError("compliance details do not describe confidentiality")
	}
	return result, validateClassification(result)
}

func combineClassification(first, fallback compliance.Classification) (compliance.Classification, error) {
	if err := validateClassification(first); err != nil {
		return first, err
	}
	if err := validateClassification(fallback); err != nil {
		return first, err
	}
	detailsPresent := first.DetailsSet || first.Details != "" || first.Encrypted
	first.PII = first.PII || fallback.PII
	if !first.Encrypted && fallback.Encrypted {
		first.Encrypted, first.Scope = true, fallback.Scope
	}
	if !detailsPresent {
		first.Details, first.DetailsSet = fallback.Details, fallback.DetailsSet || fallback.Encrypted || fallback.Details != ""
	}
	if first.Encrypted && first.Scope == "" {
		first.Scope = compliance.Subject
	}
	return first, validateClassification(first)
}
func validateClassification(value compliance.Classification) error {
	if value.PII && value.Encrypted {
		return protectionError("PII and confidentiality cannot protect the same value")
	}
	if value.Scope != "" && (!value.Encrypted || (value.Scope != compliance.Subject && value.Scope != compliance.Namespace && value.Scope != compliance.Global)) {
		return protectionError("invalid confidentiality scope")
	}
	return nil
}
func providerFailure(cause error) error {
	return &declarations.DeclarationError{Directive: "protection", Offset: -1, Message: "classification provider failed", Cause: errors.Join(faults.ErrInvalidConfiguration, cause)}
}

func protectionError(message string) error {
	return &declarations.DeclarationError{Directive: "protection", Offset: -1, Message: message, Cause: faults.ErrInvalidConfiguration}
}
func dereference(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}
func containsSourceIdentity(n *node, active map[*node]bool) bool {
	if n.reference != nil {
		n = n.reference
	}
	if active[n] {
		return false
	}
	active[n] = true
	defer delete(active, n)
	if n.typ.PkgPath() == "github.com/cratis/chronicle.go/events" && n.typ.Name() == "SourceID" {
		return true
	}
	if n.concept != nil && n.concept.Type.PkgPath() == "github.com/cratis/chronicle.go/events" && n.concept.Type.Name() == "SourceID" {
		return true
	}
	if n.item != nil && containsSourceIdentity(n.item, active) {
		return true
	}
	for _, derivative := range n.derivatives {
		if containsSourceIdentity(derivative.node, active) {
			return true
		}
	}
	for _, f := range n.fields {
		if containsSourceIdentity(f.value, active) {
			return true
		}
	}
	return false
}

type categoryReference struct {
	ref            string
	pii, encrypted bool
}

func validateProtectionCategories(node map[string]any, definitions map[string]any, pii, encrypted bool, active map[categoryReference]bool) error {
	pii = pii || node["compliance"] != nil
	encrypted = encrypted || node["security"] != nil
	if pii && encrypted {
		return protectionError("PII and confidentiality cannot protect the same value")
	}
	if ref, ok := node["$ref"].(string); ok {
		key := categoryReference{ref, pii, encrypted}
		if active[key] {
			return nil
		}
		active[key] = true
		defer delete(active, key)
		target, ok := definitions[strings.TrimPrefix(ref, "#/definitions/")].(map[string]any)
		if !ok || !strings.HasPrefix(ref, "#/definitions/") {
			return protectionError("unresolved protected schema reference")
		}
		if err := validateProtectionCategories(target, definitions, pii, encrypted, active); err != nil {
			return err
		}
	}
	properties, _ := node["properties"].(map[string]any)
	for _, property := range properties {
		if err := validateProtectionCategories(property.(map[string]any), definitions, pii, encrypted, active); err != nil {
			return err
		}
	}
	for _, key := range []string{"items", "additionalProperties"} {
		if child, ok := node[key].(map[string]any); ok {
			if err := validateProtectionCategories(child, definitions, pii, encrypted, active); err != nil {
				return err
			}
		}
	}
	return nil
}
