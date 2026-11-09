// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/serialization"
)

// property is a top-level property of a node or an edge, as the graph names it.
type property struct {
	name     string
	scalar   serialization.Scalar
	list     bool
	schema   json.RawMessage
	keyed    bool
	nullable bool
}

// shape is what the graph knows about a node or an edge type: its properties, in
// declaration order, and which of them carry the key tag.
type shape struct {
	properties []property
	byName     map[string]*property
	keys       []string
}

// analyze compiles typ with the naming policy and keeps its top-level
// properties. A graph property is a primitive or a list of primitives; an object
// or a list of objects has to become a node of its own.
func analyze(graph, element string, typ reflect.Type, policy serialization.NamingPolicy) (*shape, error) {
	if typ.Kind() != reflect.Struct {
		return nil, refuse(graph, element, ErrInvalidGraph, "%s is not a struct", typ)
	}
	plan, err := serialization.CompileReadModel(typ, policy)
	if err != nil {
		return nil, fmt.Errorf("graph %s element %s: %w", graph, element, err)
	}
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
		return nil, refuse(graph, element, ErrInvalidGraph, "the schema of %s cannot be read", typ)
	}
	result := &shape{byName: make(map[string]*property)}
	for _, field := range plan.Fields() {
		if strings.Contains(field.Path, ".") {
			continue
		}
		raw, ok := schema.Properties[field.Name]
		if !ok {
			continue
		}
		list, err := classify(raw)
		if err != nil {
			return nil, refuse(graph, element, ErrNestedObject, "property %s of %s: %v", field.Name, typ, err)
		}
		if field.Scalar == serialization.Binary {
			return nil, refuse(graph, element, ErrInvalidGraph, "property %s of %s is binary, which a graph cannot store", field.Name, typ)
		}
		entry := property{name: field.Name, scalar: field.Scalar, list: list, schema: raw, nullable: field.Nullable}
		if list {
			entry.scalar = serialization.NotScalar
		}
		for _, directive := range strings.Split(field.Tag, ";") {
			if strings.TrimSpace(directive) == "key" {
				entry.keyed = true
				result.keys = append(result.keys, field.Name)
			}
		}
		result.properties = append(result.properties, entry)
	}
	for i := range result.properties {
		result.byName[result.properties[i].name] = &result.properties[i]
	}
	return result, nil
}

// classify reads the JSON schema of one property. It reports whether the
// property is a list, and refuses objects and lists of objects.
func classify(raw json.RawMessage) (list bool, err error) {
	var schema struct {
		Type  string          `json:"type"`
		Ref   string          `json:"$ref"`
		Items json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		return false, err
	}
	if schema.Ref != "" || schema.Type == "object" {
		return false, fmt.Errorf("it is an object; model the object as a node of its own and connect it with an edge")
	}
	if schema.Type != "array" {
		return false, nil
	}
	var items struct {
		Type string `json:"type"`
		Ref  string `json:"$ref"`
	}
	if len(schema.Items) > 0 {
		if err := json.Unmarshal(schema.Items, &items); err != nil {
			return false, err
		}
	}
	if items.Ref != "" || items.Type == "object" || items.Type == "array" {
		return false, fmt.Errorf("it is a list of objects; model the object as a node of its own and connect it with an edge")
	}
	return true, nil
}

// keyCandidate returns the property that carries the key tag, or the explicit one.
func (s *shape) keyCandidate(explicit string) (string, int) {
	if explicit != "" {
		return explicit, 1
	}
	if len(s.keys) == 1 {
		return s.keys[0], 1
	}
	return "", len(s.keys)
}
