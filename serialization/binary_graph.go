// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/fundamentals.go/concepts"
)

// binaryTypeCandidate controls only duplicate-error ordering, never admission.
// Inspect emitted fields before compilation can stop at another invalid field.
// Binary-free graphs retain main's immediate duplicate refusal.
func binaryTypeCandidate(typ reflect.Type, policy NamingPolicy, readModel bool, codecs *Codecs) bool {
	seen := map[compileKey]bool{}
	var walk func(reflect.Type, bool, bool) bool
	walk = func(typ reflect.Type, root, derived bool) bool {
		key := compileKey{typ, derived, root}
		if seen[key] {
			return false
		}
		seen[key] = true
		if typ.Kind() == reflect.Pointer {
			return walk(typ.Elem(), false, derived)
		}
		if representation, ok, err := concepts.Underlying(typ); err != nil {
			return false // The compiler retains the declaration error.
		} else if ok {
			return walk(representation.Type, false, false)
		}
		for _, contract := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
			if typ.Implements(contract) || reflect.PointerTo(typ).Implements(contract) {
				return false // Custom shapes never acquire the binary codec.
			}
		}
		switch typ.Kind() {
		case reflect.Slice:
			if typ.Elem() == reflect.TypeFor[byte]() {
				return true
			}
			return walk(typ.Elem(), false, false)
		case reflect.Array, reflect.Map:
			return walk(typ.Elem(), false, false)
		case reflect.Struct:
			fieldPolicy := policy
			if derived {
				fieldPolicy = CamelCase
			}
			fields, _ := serializedFieldsWithDuplicateHandler(typ, fieldPolicy, root, func(error) {})
			for _, f := range fields {
				if walk(f.field.Type, false, false) {
					return true
				}
			}
		}
		return false
	}
	if walk(typ, readModel, false) {
		return true
	}
	if codecs != nil {
		for _, registration := range codecs.ordered {
			if registration.kind == derivedCodec && walk(registration.concrete, false, true) {
				return true
			}
		}
	}
	return false
}

// No recursive/$ref binary graph has packaged or pinned-kernel qualification,
// even when the recursive branch itself has no binary descendants.
func validateBinaryRecursion(root *node) error {
	active, seen := map[*node]bool{}, map[*node]bool{}
	var walk func(*node) error
	walk = func(n *node) error {
		if n == nil {
			return nil
		}
		if active[n] {
			return fmt.Errorf("%w: recursive binary-containing types are not qualified", faults.ErrUnsupported)
		}
		if seen[n] {
			return nil
		}
		active[n], seen[n] = true, true
		defer delete(active, n)
		for _, child := range []*node{n.item, n.reference} {
			if err := walk(child); err != nil {
				return err
			}
		}
		for _, f := range n.fields {
			if err := walk(f.value); err != nil {
				return err
			}
		}
		for _, derivative := range n.derivatives {
			if err := walk(derivative.node); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root)
}
