// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
)

// Binary-containing graphs use only ordinary, non-reserved property segments.
// Checking all emitted siblings at every level avoids depending on which path
// partition, accessor or case-insensitive fallback the kernel happens to choose.
// Binary-free graphs retain their existing naming and lookup behavior.
func validateBinaryPropertyNames(root *node) error {
	return visitNodes(root, map[*node]bool{}, func(n *node) error {
		names := make(map[string]bool, len(n.fields))
		for _, f := range n.fields {
			key := binaryPropertyNameKey(f.name)
			if strings.Contains(f.name, ".") || !declarations.Path(f.name) || binaryReservedPropertyName(key) || names[key] {
				return binaryPropertyNamesError()
			}
			names[key] = true
		}
		return nil
	})
}

func binaryPropertyNamesError() error {
	return fmt.Errorf("%w: binary-containing types require simple, case-insensitively unique, non-reserved property names", faults.ErrUnsupported)
}

// v19.29.4 PropertyPath.ResolvePropertyPathSegment recognizes Week (the entire
// DerivedPropertyFunctions.All registry) and $this. *NotSet* is its sentinel.
// LiteralExpressionResolver also intercepts true/false before ordinary event
// content. Brackets, call syntax and all $-prefixed expression tokens are already
// excluded by the identifier grammar, but the path tokens are listed explicitly.
func binaryReservedPropertyName(key string) bool {
	switch key {
	case "WEEK", "$THIS", "*NOTSET*", "TRUE", "FALSE":
		return true
	default:
		return false
	}
}

// OrdinalIgnoreCase uses simple uppercase, not Unicode full case folding:
// no expansions or normalization; Kelvin sign stays distinct from ASCII K.
// CLR ordinal casing leaves dotless i and long s distinct from ASCII I and S.
func binaryPropertyNameKey(name string) string {
	return strings.Map(func(r rune) rune {
		if r == '\u0131' || r == '\u017f' {
			return r
		}
		return unicode.ToUpper(r)
	}, name)
}
