// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
)

type fieldCandidate struct {
	field    reflect.StructField
	index    []int
	name     string
	goName   string
	tagged   bool
	optional bool // A promoted field below an embedded pointer is not required.
}

// serializedFields resolves promotion first, then retains index-path order, as
// encoding/json does. Direct duplicate names still fail closed, as before.
func serializedFields(typ reflect.Type, policy NamingPolicy, readModelRoot bool) ([]fieldCandidate, error) {
	return serializedFieldsWithDuplicateHandler(typ, policy, readModelRoot, nil)
}

// Plans can defer an exact duplicate until the full binary capability graph is
// known. All other callers retain immediate duplicate refusal. Deferred fields
// are never published: binary naming admission or the original error rejects them.
func serializedFieldsWithDuplicateHandler(typ reflect.Type, policy NamingPolicy, readModelRoot bool, duplicate func(error)) ([]fieldCandidate, error) {
	var candidates []fieldCandidate
	if err := collectCandidates(typ, policy, readModelRoot, nil, "", false, map[reflect.Type]bool{}, &candidates); err != nil {
		return nil, err
	}
	groups := map[string][]int{}
	for i, candidate := range candidates {
		groups[candidate.name] = append(groups[candidate.name], i)
	}
	selected := make([]bool, len(candidates))
	// Resolve in traversal order, so invalid declarations fail deterministically.
	for _, candidate := range candidates {
		group, exists := groups[candidate.name]
		if !exists {
			continue
		}
		delete(groups, candidate.name)
		depth := len(candidates[group[0]].index)
		for _, i := range group {
			if d := len(candidates[i].index); d < depth {
				depth = d
			}
		}
		var dominant []int
		for _, i := range group {
			if len(candidates[i].index) == depth {
				dominant = append(dominant, i)
			}
		}
		if depth == 1 && len(dominant) > 1 {
			err := fmt.Errorf("%w: %s: duplicate JSON property: %s", faults.ErrInvalidConfiguration, typ, candidate.name)
			if duplicate == nil {
				return nil, err
			}
			duplicate(err)
			for _, i := range dominant {
				selected[i] = true
			}
			continue
		}
		var tagged []int
		for _, i := range dominant {
			if candidates[i].tagged {
				tagged = append(tagged, i)
			}
		}
		if len(tagged) > 0 {
			dominant = tagged
		}
		if len(dominant) == 1 {
			selected[dominant[0]] = true
		}
	}
	var result []fieldCandidate
	for i, candidate := range candidates {
		if selected[i] {
			result = append(result, candidate)
		} else if candidate.field.Tag.Get("chronicle") != "" {
			return nil, ignoredDeclaration(typ, candidate.goName)
		}
	}
	return result, nil
}

func collectCandidates(typ reflect.Type, policy NamingPolicy, readModelRoot bool, index []int, prefix string, optional bool, active map[reflect.Type]bool, result *[]fieldCandidate) error {
	if active[typ] {
		return nil // Recursive anonymous promotion cannot introduce new fields.
	}
	active[typ] = true
	defer delete(active, typ)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("chronicle")
		tags := strings.Split(f.Tag.Get("json"), ",")
		underlying := f.Type
		if underlying.Kind() == reflect.Pointer {
			underlying = underlying.Elem()
		}
		goName := prefix + f.Name
		if tags[0] == "-" || !f.IsExported() && (!f.Anonymous || underlying.Kind() != reflect.Struct) {
			if tag != "" {
				return ignoredDeclaration(typ, goName)
			}
			continue
		}
		for _, option := range tags[1:] {
			if option != "omitempty" && option != "omitzero" {
				return unsupported(typ, "unsupported JSON tag option")
			}
		}
		indices := append(append([]int(nil), index...), i)
		if f.Anonymous && tags[0] == "" && underlying.Kind() == reflect.Struct {
			if tag != "" {
				return ignoredDeclaration(typ, goName)
			}
			if err := collectCandidates(underlying, policy, readModelRoot, indices, goName+".", optional || f.Type.Kind() == reflect.Pointer, active, result); err != nil {
				return err
			}
			continue
		}
		name := tags[0]
		if name == "" {
			propertyName := f.Name
			if _, tagged := f.Tag.Lookup("json"); readModelRoot && propertyName == "ID" && !tagged {
				propertyName = "Id"
			}
			name = policy.name(propertyName)
		}
		if err := validateTag(tag, declarations.Any, typ.String(), goName, name); err != nil {
			return err
		}
		*result = append(*result, fieldCandidate{field: f, index: indices, name: name, goName: goName, tagged: tags[0] != "", optional: optional})
	}
	return nil
}

func ignoredDeclaration(typ reflect.Type, goName string) error {
	return &declarations.DeclarationError{Artifact: typ.String(), GoField: goName, Offset: 0, Message: "declaration on an ignored field", Cause: faults.ErrInvalidConfiguration}
}

// fieldValue follows promoted paths. Encoding skips a nil embedded pointer;
// decoding allocates it only for a property actually present in the input.
func fieldValue(value reflect.Value, index []int, allocate bool) (reflect.Value, error) {
	for _, i := range index {
		if value.Kind() == reflect.Pointer {
			if value.IsNil() {
				if !allocate {
					return reflect.Value{}, nil
				}
				if !value.CanSet() {
					return reflect.Value{}, unsupported(value.Type(), "cannot allocate unexported embedded pointer")
				}
				value.Set(reflect.New(value.Type().Elem()))
			}
			value = value.Elem()
		}
		value = value.Field(i)
	}
	return value, nil
}
