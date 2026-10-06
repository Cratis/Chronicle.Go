// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Binary-containing graphs use only ordinary, non-reserved property segments.
// Checking all emitted siblings at every level avoids depending on which path
// partition, accessor or case-insensitive fallback the kernel happens to choose.
// Binary-free graphs retain their existing naming and lookup behavior.
func validateBinaryPropertyNames(root *node) error {
	type location struct {
		node *node
		root bool
	}
	seen := map[location]bool{}
	var walk func(*node, bool) error
	walk = func(n *node, rootObject bool) error {
		if n == nil || seen[location{n, rootObject}] {
			return nil
		}
		seen[location{n, rootObject}] = true
		names := make(map[string]bool, len(n.fields))
		for _, f := range n.fields {
			key := binaryPropertyNameKey(f.name)
			if !binaryASCIIPropertyName(f.name) || binaryReservedPropertyName(key) || names[key] || !rootObject && key == "ID" || n.containsBinary && key == "VALUE" {
				return binaryPropertyNamesError()
			}
			names[key] = true
			if err := walk(f.value, false); err != nil {
				return err
			}
		}
		for _, child := range []*node{n.item, n.reference} {
			if err := walk(child, rootObject); err != nil {
				return err
			}
		}
		for _, derivative := range n.derivatives {
			if err := walk(derivative.node, false); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root, true)
}

func binaryPropertyNamesError() error {
	return fmt.Errorf("%w: binary-containing types require ASCII, case-insensitively unique, non-reserved property names", faults.ErrUnsupported)
}

// v19.29.4 PropertyPath.ResolvePropertyPathSegment recognizes Week (the entire
// DerivedPropertyFunctions.All registry) and $this. *NotSet* is its sentinel.
// LiteralExpressionResolver also intercepts true/false before ordinary event
// content. Brackets, call syntax and all $-prefixed expression tokens are already
// excluded by the identifier grammar, but the path tokens are listed explicitly.
// All underscore-prefixed names are separately refused: MongoDB aliases _id and
// owns every name in Storage/WellKnownProperties.cs (__lastHandledEventSequenceNumber,
// __initialized, __subject, __subjects). Nested id aliases and binary-owning
// objects' value properties are refused by the location-aware walk above.
func binaryReservedPropertyName(key string) bool {
	switch key {
	case "WEEK", "$THIS", "*NOTSET*", "TRUE", "FALSE":
		return true
	default:
		return false
	}
}

// ASCII names give CLR ordinal casing and Go EqualFold identical sibling
// classes. In particular, Kelvin sign and long s cannot alias ASCII fields.
func binaryPropertyNameKey(name string) string { return strings.ToUpper(name) }

func binaryASCIIPropertyName(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		c := name[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || i > 0 && (c >= '0' && c <= '9' || c == '_') {
			continue
		}
		return false
	}
	return true
}
