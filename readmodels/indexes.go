// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func collectIndexes(plan *serialization.Plan, config *modelConfig, typ reflect.Type) error {
	// Fields uses path-local cycle guards, retaining repeated sibling types.
	var mapPaths []string
	for _, field := range plan.Fields() {
		if slices.ContainsFunc(mapPaths, func(path string) bool { return strings.HasPrefix(field.GoField, path+".") }) {
			continue // C# does not collect indexes from dictionary values.
		}
		fieldType := field.Type
		for fieldType.Kind() == reflect.Pointer || fieldType.Kind() == reflect.Slice || fieldType.Kind() == reflect.Array {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Map {
			mapPaths = append(mapPaths, field.GoField)
		}
		directives, err := declarations.Parse(declarations.V1, field.Tag)
		if err != nil {
			return err // The plan has already validated syntax and role.
		}
		for _, directive := range directives {
			if directive.Name != "index" {
				continue
			}
			if slices.Contains(config.indexes, field.Path) {
				return &declarations.DeclarationError{Artifact: typ.String(), GoField: field.GoField, Path: field.Path, Directive: "index", Offset: directive.Offset, Message: "duplicate index declaration", Cause: faults.ErrInvalidConfiguration}
			}
			config.indexes = append(config.indexes, field.Path)
		}
	}
	return nil
}
