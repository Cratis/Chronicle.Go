// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/cratis/chronicle.go/internal/faults"
)

// NamingPolicy is an immutable property-name policy, independent of container names.
type NamingPolicy uint8

const (
	// PreservePropertyNames preserves Go field spelling, like C# DefaultNamingPolicy.
	// This is the default. Explicit json tags override every policy.
	PreservePropertyNames NamingPolicy = iota
	// CamelCase matches Fundamentals CamelCaseNamingPolicy: Person becomes person,
	// but leading acronyms such as URLValue and ID retain their spelling.
	CamelCase
	// LegacyGoCamelCase retains Chronicle.Go's original wire names (URLValue becomes
	// urlValue and ID becomes id). Use it for already-persisted Go schemas only.
	LegacyGoCamelCase
)

// Validate rejects unknown policies before any artifact or connection is created.
func (p NamingPolicy) Validate() error {
	if p > LegacyGoCamelCase {
		return fmt.Errorf("%w: unknown property naming policy", faults.ErrInvalidConfiguration)
	}
	return nil
}

func (p NamingPolicy) name(value string) string {
	if p == PreservePropertyNames {
		return value
	}
	runes := []rune(value)
	if p == CamelCase {
		// C# uses UTF-16 chars, so supplementary letters do not participate in casing.
		upper := func(r rune) bool { return r <= 0xffff && unicode.IsUpper(r) }
		if len(runes) == 0 || !upper(runes[0]) || len(runes) >= 2 && upper(runes[1]) {
			return value
		}
		for i := range runes {
			if i == 1 && !upper(runes[i]) {
				break
			}
			if i > 0 && i+1 < len(runes) && !upper(runes[i+1]) {
				if runes[i+1] == ' ' {
					runes[i] = unicode.ToLower(runes[i])
				}
				break
			}
			runes[i] = unicode.ToLower(runes[i])
		}
		return string(runes)
	}
	// Preserve the old Go algorithm exactly, including its Unicode behavior.
	for i := range runes {
		if !unicode.IsUpper(runes[i]) {
			break
		}
		if i > 0 && i+1 < len(runes) && !unicode.IsUpper(runes[i+1]) {
			break
		}
		runes[i] = unicode.ToLower(runes[i])
	}
	return string(runes)
}

// RebindPath resolves an existing serialized path into another plan by declared
// field identity. It never applies casing to path text. Empty paths stay empty;
// missing fields fail closed. Registry composition and future migrations can use it.
func RebindPath(path string, from, to []Field) (string, error) {
	if path == "" {
		return "", nil
	}
	original, ok := FieldAt(from, path)
	if ok {
		for _, field := range to {
			if field.GoField == original.GoField && field.Type == original.Type {
				return field.Path, nil
			}
		}
	}
	for _, parent := range RootFields(from) {
		if !strings.HasPrefix(path, parent.Path+".") {
			continue
		}
		for _, candidate := range RootFields(to) {
			if parent.GoField != candidate.GoField || parent.Type != candidate.Type {
				continue
			}
			suffix, err := RebindPath(strings.TrimPrefix(path, parent.Path+"."), parent.Fields(), candidate.Fields())
			if err != nil {
				return "", err
			}
			return candidate.Path + "." + suffix, nil
		}
	}
	return "", fmt.Errorf("%w: property path %q has no matching plan field", faults.ErrInvalidConfiguration, path)
}
