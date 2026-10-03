// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

func (c *compiler) childEvents(fields []serialization.Field) (map[events.TypeRef]bool, error) {
	result := map[events.TypeRef]bool{}
	for _, field := range serialization.RootFields(fields) {
		directives, err := declarations.Parse(declarations.V1, field.Tag)
		if err != nil {
			return nil, err
		}
		for _, directive := range directives {
			if directive.Name != "children" {
				continue
			}
			event, err := c.resolve(directive, Provenance{GoField: field.GoField, Path: field.Path, Directive: "children", Offset: directive.Offset})
			if err != nil {
				return nil, err
			}
			result[event.Ref()] = true
		}
	}
	return result, nil
}
