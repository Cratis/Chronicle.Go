// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"reflect"
	"slices"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func collectIndexes(plan *serialization.Plan, config *modelConfig, typ reflect.Type) error {
	// Fields uses path-local cycle guards, retaining repeated sibling types.
	for _, field := range plan.Fields() {
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
