// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
)

// validatePropertyNames refuses names whose kernel lookup can select a function,
// literal or different sibling, even when the graph contains no binary values.
func validatePropertyNames(candidates []*node, binary bool) error {
	// Retain the existing binary diagnostics before checking the broader rule.
	if binary {
		if err := validateBinaryGraphs(candidates); err != nil {
			return err
		}
	}
	return validatePropertyNameCandidates(candidates)
}

func validatePropertyNameCandidates(candidates []*node) error {
	seen := map[*node]bool{}
	for _, candidate := range candidates {
		if err := walkPropertyNames(candidate, "", seen); err != nil {
			return err
		}
	}
	return nil
}

func walkPropertyNames(n *node, path string, seen map[*node]bool) error {
	if n == nil || seen[n] {
		return nil
	}
	seen[n] = true
	if err := validateSiblingPropertyNames(n.fields, path); err != nil {
		return err
	}
	for _, f := range n.fields {
		if err := walkPropertyNames(f.value, propertyNamePath(path, f.name), seen); err != nil {
			return err
		}
	}
	return walkPropertyNameDescendants(n, path, seen)
}

func walkPropertyNameDescendants(n *node, path string, seen map[*node]bool) error {
	children := []*node{n.item, n.reference}
	for _, derivative := range n.derivatives {
		children = append(children, derivative.node)
	}
	for _, child := range children {
		if err := walkPropertyNames(child, path, seen); err != nil {
			return err
		}
	}
	return nil
}

func validateSiblingPropertyNames(fields []field, path string) error {
	for i, f := range fields {
		if err := validatePropertyName(f.name, fields[:i], path); err != nil {
			return err
		}
	}
	return nil
}

func validatePropertyName(name string, previous []field, path string) error {
	fieldPath := propertyNamePath(path, name)
	if binaryReservedPropertyName(binaryPropertyNameKey(name)) {
		return fmt.Errorf("%w: property %q is a reserved kernel property-path token", faults.ErrUnsupported, fieldPath)
	}
	if sibling := caseInsensitiveSibling(name, previous); sibling != "" {
		return fmt.Errorf("%w: property %q collides case-insensitively with %q", faults.ErrUnsupported, fieldPath, propertyNamePath(path, sibling))
	}
	return nil
}

func caseInsensitiveSibling(name string, previous []field) string {
	for _, f := range previous {
		if strings.EqualFold(f.name, name) {
			return f.name
		}
	}
	return ""
}

func propertyNamePath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}
