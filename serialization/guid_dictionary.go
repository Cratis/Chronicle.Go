// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"reflect"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/google/uuid"
)

// guidDictionaryKeyLength is the length of the canonical hyphenated (D) form,
// the only spelling System.Text.Json's Guid converter reads from a property name.
const guidDictionaryKeyLength = 36

var (
	guidKeyType        = reflect.TypeFor[uuid.UUID]()
	guidDictionaryType = reflect.TypeFor[map[uuid.UUID]string]()
)

// isGuidDictionary reports whether typ is a map keyed by google/uuid.UUID.
func isGuidDictionary(typ reflect.Type) bool {
	return typ.Kind() == reflect.Map && typ.Key() == guidKeyType
}

// checkGuidDictionary admits only event/document maps of exactly
// map[uuid.UUID]string (true aliases included); a defined map type, whose
// methods could intercept decoding, is refused.
func checkGuidDictionary(typ reflect.Type, state *compileState) error {
	if state.readModel {
		return unsupported(typ, "UUID map keys are not supported in read models")
	}
	if typ != guidDictionaryType {
		return unsupported(typ, "UUID maps must be exactly map[uuid.UUID]string")
	}
	return nil
}

// dictionaryKeyText returns the property name written for a map key. UUID keys
// use the lowercase RFC 4122 form; string keys are written unchanged.
func dictionaryKeyText(key reflect.Value) string {
	if key.Type() == guidKeyType {
		return key.Interface().(uuid.UUID).String()
	}
	return key.String()
}

// parseGuidDictionaryKeys parses every property name of a UUID dictionary of type
// typ, and returns nil for any other map type. A
// name that is not exactly 36 characters of canonical D form, or two names that
// resolve to the same UUID, is a payload-free protocol violation; no partial
// result is returned. Either hex case is accepted on read.
func parseGuidDictionaryKeys[V any](typ reflect.Type, items map[string]V) (map[string]uuid.UUID, error) {
	if !isGuidDictionary(typ) {
		return nil, nil
	}
	parsed := make(map[string]uuid.UUID, len(items))
	seen := make(map[uuid.UUID]struct{}, len(items))
	for name := range items {
		if len(name) != guidDictionaryKeyLength {
			return nil, faults.ErrProtocol
		}
		id, err := uuid.Parse(name)
		if err != nil {
			return nil, faults.ErrProtocol
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, faults.ErrProtocol
		}
		seen[id] = struct{}{}
		parsed[name] = id
	}
	return parsed, nil
}

// dictionaryKey builds the typed map key for a decoded property name; guids is
// the result of parseGuidDictionaryKeys, nil for string-keyed maps.
func dictionaryKey(keyType reflect.Type, name string, guids map[string]uuid.UUID) reflect.Value {
	key := reflect.New(keyType).Elem()
	if guids != nil {
		key.Set(reflect.ValueOf(guids[name]))
	} else {
		key.SetString(name)
	}
	return key
}
