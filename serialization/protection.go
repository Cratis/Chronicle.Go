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
		classified, err := p.hasProtectionTags()
		if err != nil {
			return "", err
		}
		if !classified && len(p.families) == 0 {
			return p.Schema(), nil
		}
	}
	c := protectionCompiler{types: map[reflect.Type]compliance.Classification{}, paths: map[string]compliance.Classification{}, active: map[protectionKey]map[string]any{}, definitions: map[string]any{}, usedTypes: map[reflect.Type]bool{}}
	return c.compile(p, options)
}

func (c *protectionCompiler) compile(plan *Plan, options []compliance.Declaration) (string, error) {
	for _, option := range options {
		if err := c.applyDeclaration(plan, option); err != nil {
			return "", err
		}
	}
	if err := c.auditFamilies(plan.families); err != nil {
		return "", err
	}
	schema, err := c.walk(plan.root, compliance.Classification{}, compliance.Classification{}, "")
	if err != nil {
		return "", protectionArtifactError(err, plan.typ)
	}
	return c.encodeSchema(plan, schema)
}

func protectionArtifactError(err error, typ reflect.Type) error {
	var declaration *declarations.DeclarationError
	if errors.As(err, &declaration) {
		declaration.Artifact = typ.String()
	}
	return err
}

func (c *protectionCompiler) encodeSchema(plan *Plan, schema map[string]any) (string, error) {
	if c.classified && hasFamily(plan.root, map[*node]bool{}) {
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
	categories := protectionCategories{definitions: c.definitions, active: map[categoryReference]bool{}}
	if err := categories.validate(schema, false, false); err != nil {
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

func (p *Plan) hasProtectionTags() (bool, error) {
	classified := false
	err := p.visitAll(func(n *node) error {
		for _, f := range n.fields {
			present, err := hasFieldProtection(f.tag)
			if err != nil {
				return err
			}
			classified = classified || present
		}
		return nil
	})
	return classified, err
}

func hasFieldProtection(tag string) (bool, error) {
	directives, err := declarations.Parse(declarations.V1, tag)
	if err != nil {
		return false, err
	}
	for _, directive := range directives {
		if protectionDirective(directive.Name) {
			return true, nil
		}
	}
	return false, nil
}

func (c *protectionCompiler) applyDeclaration(plan *Plan, option compliance.Declaration) error {
	switch {
	case option.TargetType() != nil:
		typ := dereference(option.TargetType())
		if _, exists := c.types[typ]; exists {
			return protectionError("duplicate type classification")
		}
		c.types[typ] = option.Metadata()
	case option.Path() != "":
		if err := c.applyProperty(plan, option); err != nil {
			return err
		}
	case option.Provider() != nil:
		c.providers = append(c.providers, option.Provider())
	default:
		return protectionError("empty classification declaration")
	}
	return validateClassification(option.Metadata())
}

func (c *protectionCompiler) applyProperty(plan *Plan, option compliance.Declaration) error {
	if _, exists := c.paths[option.Path()]; exists {
		return protectionError("duplicate property classification")
	}
	if _, ok := FieldAt(plan.Fields(), option.Path()); !ok {
		return protectionError("unknown classification property")
	}
	c.paths[option.Path()] = option.Metadata()
	return nil
}

func (c *protectionCompiler) auditFamilies(families []*node) error {
	// Unused families have no document-property address. Audit their type, tag
	// and provider classifications without applying unrelated root path overrides.
	paths := c.paths
	c.paths = nil
	for _, family := range families {
		if _, err := c.walk(family, compliance.Classification{}, compliance.Classification{}, ""); err != nil {
			return err
		}
	}
	c.paths = paths
	return nil
}

func (c *protectionCompiler) typeMetadata(typ reflect.Type) (compliance.Classification, error) {
	typ = dereference(typ)
	c.usedTypes[typ] = true
	result, err := c.providedMetadata(compliance.Target{Type: typ}, c.types[typ])
	if err != nil {
		return result, err
	}
	return result, validateClassification(result)
}

func (c *protectionCompiler) providedMetadata(target compliance.Target, result compliance.Classification) (compliance.Classification, error) {
	for _, provider := range c.providers {
		provided, err := invoke(func() (compliance.Classification, error) { return provider(target) })
		if err != nil {
			return result, providerFailure(err)
		}
		result, err = combineClassification(result, provided)
		if err != nil {
			return result, err
		}
	}
	return result, nil
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
	if err := c.validateNodeProtection(n, metadata); err != nil {
		return nil, err
	}
	contextPath := c.overrideContext(path)
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
	value := protectedValue{node: n, metadata: metadata, typeMetadata: typeMetadata, path: path, schema: result}
	return c.walkValue(value, inherited, member)
}

func (c *protectionCompiler) validateNodeProtection(n *node, metadata compliance.Classification) error {
	if metadata != (compliance.Classification{}) {
		if hasBinary(n) {
			return &declarations.DeclarationError{Directive: "protection", Offset: -1, Message: "protected binary placements are not supported", Cause: faults.ErrUnsupported}
		}
		if hasEnum(n) {
			return protectionError("protected enum placements are not supported")
		}
		c.classified = true
	}
	if protectedCategory(metadata) && containsSourceIdentity(n, map[*node]bool{}) {
		return protectionError("event-source identity cannot be protected")
	}
	return nil
}

func (c *protectionCompiler) overrideContext(path string) string {
	for target := range c.paths {
		if path == "" || strings.HasPrefix(target, path+".") {
			// Distinguish even the root's pending overrides from a recursive
			// occurrence with no overrides; otherwise they repeat every cycle.
			return "." + path
		}
	}
	return ""
}

// protectedValue carries one traversal occurrence, including its active schema.
// Recursive references must observe that same map while children are rebuilt.
type protectedValue struct {
	node         *node
	metadata     compliance.Classification
	typeMetadata compliance.Classification
	path         string
	schema       map[string]any
}

func (c *protectionCompiler) walkValue(value protectedValue, inherited, member compliance.Classification) (map[string]any, error) {
	if value.node.family {
		return c.walkFamily(value)
	}
	if value.node.typ.Kind() == reflect.Pointer {
		return c.walkPointer(value, inherited, member)
	}
	if value.node.item != nil && value.node.concept == nil {
		if err := c.walkCollection(value); err != nil {
			return nil, err
		}
	}
	if len(value.node.fields) > 0 {
		return c.walkProperties(value)
	}
	applyLeafClassification(value)
	return value.schema, nil
}

func applyLeafClassification(value protectedValue) {
	if value.metadata.PII {
		value.schema["compliance"] = []any{map[string]any{"metadataType": "PII", "details": value.metadata.Details}}
	}
	if value.metadata.Encrypted {
		kind := map[compliance.Scope]string{compliance.Subject: "EncryptedSubject", compliance.Namespace: "EncryptedNamespace", compliance.Global: "EncryptedGlobal"}[value.metadata.Scope]
		value.schema["security"] = []any{map[string]any{"metadataType": kind, "details": value.metadata.Details}}
	}
}

func (c *protectionCompiler) walkFamily(value protectedValue) (map[string]any, error) {
	if value.metadata != (compliance.Classification{}) {
		return nil, protectionError("classified derived families are not supported")
	}
	for _, derivative := range value.node.derivatives {
		classifiedBefore := c.classified
		c.classified = false
		_, err := c.walk(derivative.node, compliance.Classification{}, compliance.Classification{}, value.path)
		if err != nil {
			return nil, err
		}
		if c.classified {
			return nil, protectionError("classified derived variants are not supported")
		}
		c.classified = classifiedBefore
	}
	return value.schema, nil
}

func (c *protectionCompiler) walkPointer(value protectedValue, inherited, member compliance.Classification) (map[string]any, error) {
	item, err := c.walk(value.node.item, inherited, member, value.path)
	if err != nil {
		return nil, err
	}
	// Rebuild from the protected child, not the original pointer schema:
	// a property override can replace a recursive reference with inline fields.
	result := value.schema
	clear(result)
	maps.Copy(result, item)
	if format, ok := result["format"].(string); ok {
		result["format"] = strings.TrimSuffix(format, "?") + "?"
		if value.node.item.binary {
			result["type"] = []string{"string", "null"}
		}
	} else if kind, ok := result["type"].(string); ok {
		result["type"] = []string{kind, "null"}
	}
	return result, nil
}

func (c *protectionCompiler) walkCollection(value protectedValue) error {
	item, err := c.walk(value.node.item, compliance.Classification{}, compliance.Classification{}, value.path)
	if err != nil {
		return err
	}
	if value.node.typ.Kind() == reflect.Map {
		value.schema["additionalProperties"] = item
	} else {
		value.schema["items"] = item
	}
	return nil
}

func (c *protectionCompiler) walkProperties(value protectedValue) (map[string]any, error) {
	properties := make(map[string]any, len(value.node.fields))
	for _, f := range value.node.fields {
		fieldPath := f.name
		if value.path != "" {
			fieldPath = value.path + "." + f.name
		}
		fieldMetadata, err := c.fieldMetadata(value, f, fieldPath)
		if err != nil {
			return nil, err
		}
		properties[f.name], err = c.walk(f.value, value.metadata, fieldMetadata, fieldPath)
		if err != nil {
			return nil, fieldProtectionError(err, f, fieldPath)
		}
	}
	value.schema["properties"] = properties
	return value.schema, nil
}

func fieldProtectionError(err error, f field, path string) error {
	var declaration *declarations.DeclarationError
	if errors.As(err, &declaration) && declaration.Path == "" {
		declaration.Path, declaration.GoField = path, f.goName
	}
	return err
}

func (c *protectionCompiler) fieldMetadata(value protectedValue, f field, path string) (compliance.Classification, error) {
	metadata, err := classificationTag(f.tag)
	if err != nil {
		return metadata, err
	}
	if explicit, exists := c.paths[path]; exists {
		if metadata.PII || metadata.Encrypted {
			return metadata, protectionError("duplicate field classification")
		}
		metadata, err = combineClassification(explicit, metadata)
		if err != nil {
			return metadata, err
		}
	}
	declaringType, declaringMetadata, err := c.declaringMetadata(value, f.index)
	if err != nil {
		return metadata, err
	}
	target := compliance.Target{Type: f.value.typ, DeclaringType: declaringType, Field: value.node.typ.FieldByIndex(f.index).Name}
	metadata, err = c.providedMetadata(target, metadata)
	if err != nil {
		return metadata, err
	}
	// Member, declaring type, then member type: C# attribute precedence.
	return combineClassification(metadata, declaringMetadata)
}

func (c *protectionCompiler) declaringMetadata(value protectedValue, indices []int) (reflect.Type, compliance.Classification, error) {
	typ, metadata := value.node.typ, value.typeMetadata
	for _, index := range indices[:len(indices)-1] {
		typ = dereference(typ.Field(index).Type)
		embeddedMetadata, err := c.typeMetadata(typ)
		if err != nil {
			return typ, metadata, err
		}
		metadata, err = combineClassification(embeddedMetadata, metadata)
		if err != nil {
			return typ, metadata, err
		}
	}
	return typ, metadata, nil
}

func classificationTag(tag string) (compliance.Classification, error) {
	result := compliance.Classification{}
	directives, err := declarations.Parse(declarations.V1, tag)
	if err != nil {
		return result, err
	}
	seen := map[string]bool{}
	for _, directive := range directives {
		if !protectionDirective(directive.Name) {
			continue
		}
		if seen[directive.Name] {
			return result, protectionError("duplicate classification directive")
		}
		seen[directive.Name] = true
		applyClassificationDirective(&result, directive)
	}
	if seen["encrypted"] && seen["compliance-details"] {
		return result, protectionError("compliance details do not describe confidentiality")
	}
	return result, validateClassification(result)
}

func applyClassificationDirective(result *compliance.Classification, directive declarations.Directive) {
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

func classificationDetailsPresent(value compliance.Classification) bool {
	return value.DetailsSet || value.Details != "" || value.Encrypted
}

func combineClassification(first, fallback compliance.Classification) (compliance.Classification, error) {
	if err := validateClassification(first); err != nil {
		return first, err
	}
	if err := validateClassification(fallback); err != nil {
		return first, err
	}
	detailsPresent := classificationDetailsPresent(first)
	first.PII = first.PII || fallback.PII
	if !first.Encrypted && fallback.Encrypted {
		first.Encrypted, first.Scope = true, fallback.Scope
	}
	if !detailsPresent {
		first.Details, first.DetailsSet = fallback.Details, classificationDetailsPresent(fallback)
	}
	first = defaultConfidentialityScope(first)
	return first, validateClassification(first)
}

func defaultConfidentialityScope(value compliance.Classification) compliance.Classification {
	if value.Encrypted && value.Scope == "" {
		value.Scope = compliance.Subject
	}
	return value
}

func protectedCategory(value compliance.Classification) bool {
	return value.PII || value.Encrypted
}

func validateClassification(value compliance.Classification) error {
	if value.PII && value.Encrypted {
		return protectionError("PII and confidentiality cannot protect the same value")
	}
	if invalidConfidentialityScope(value) {
		return protectionError("invalid confidentiality scope")
	}
	return nil
}
func invalidConfidentialityScope(value compliance.Classification) bool {
	if value.Scope == "" {
		return false
	}
	return !value.Encrypted || !validConfidentialityScope(value.Scope)
}

func validConfidentialityScope(scope compliance.Scope) bool {
	switch scope {
	case compliance.Subject, compliance.Namespace, compliance.Global:
		return true
	default:
		return false
	}
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
	if sourceOrItemIdentity(n, active) {
		return true
	}
	if derivativesContainSourceIdentity(n.derivatives, active) {
		return true
	}
	for _, f := range n.fields {
		if containsSourceIdentity(f.value, active) {
			return true
		}
	}
	return false
}

func sourceOrItemIdentity(n *node, active map[*node]bool) bool {
	if sourceIdentityType(n.typ) {
		return true
	}
	if n.concept != nil && sourceIdentityType(n.concept.Type) {
		return true
	}
	return n.item != nil && containsSourceIdentity(n.item, active)
}

func derivativesContainSourceIdentity(derivatives []derivative, active map[*node]bool) bool {
	for _, derivative := range derivatives {
		if containsSourceIdentity(derivative.node, active) {
			return true
		}
	}
	return false
}

func sourceIdentityType(typ reflect.Type) bool {
	return typ.PkgPath() == "github.com/cratis/chronicle.go/events" && typ.Name() == "SourceID"
}

type categoryReference struct {
	ref            string
	pii, encrypted bool
}

type protectionCategories struct {
	definitions map[string]any
	active      map[categoryReference]bool
}

func (v protectionCategories) validate(node map[string]any, pii, encrypted bool) error {
	key := categoryReference{pii: pii || node["compliance"] != nil, encrypted: encrypted || node["security"] != nil}
	return v.validateNode(node, key)
}

func (v protectionCategories) validateNode(node map[string]any, key categoryReference) error {
	if key.pii && key.encrypted {
		return protectionError("PII and confidentiality cannot protect the same value")
	}
	if ref, ok := node["$ref"].(string); ok {
		key.ref = ref
		if v.active[key] {
			return nil
		}
		v.active[key] = true
		defer delete(v.active, key)
		if err := v.validateReference(key); err != nil {
			return err
		}
	}
	if err := v.validateProperties(node, key); err != nil {
		return err
	}
	return v.validateItems(node, key)
}

func (v protectionCategories) validateProperties(node map[string]any, inherited categoryReference) error {
	properties, _ := node["properties"].(map[string]any)
	for _, property := range properties {
		if err := v.validate(property.(map[string]any), inherited.pii, inherited.encrypted); err != nil {
			return err
		}
	}
	return nil
}

func (v protectionCategories) validateItems(node map[string]any, inherited categoryReference) error {
	for _, key := range []string{"items", "additionalProperties"} {
		child, ok := node[key].(map[string]any)
		if !ok {
			continue
		}
		if err := v.validate(child, inherited.pii, inherited.encrypted); err != nil {
			return err
		}
	}
	return nil
}

func (v protectionCategories) validateReference(key categoryReference) error {
	target, ok := v.definitions[strings.TrimPrefix(key.ref, "#/definitions/")].(map[string]any)
	if !ok || !strings.HasPrefix(key.ref, "#/definitions/") {
		return protectionError("unresolved protected schema reference")
	}
	return v.validate(target, key.pii, key.encrypted)
}
