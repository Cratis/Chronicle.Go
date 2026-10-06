// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import "github.com/cratis/chronicle.go/serialization"

// Key expressions have event-side paths, including composite parts. Context,
// constants and source identity do not resolve their text as payload properties.
func binaryKeyField(fields []serialization.Field, e expression) (serialization.Field, bool) {
	switch e.kind {
	case pathExpression:
		if field, ok := binaryMappingField(fields, e.text); ok && binaryField(field) {
			return field, true
		}
	case compositeExpression:
		for _, part := range e.parts {
			if field, ok := binaryKeyField(fields, part.expression); ok {
				return field, true
			}
		}
	}
	return serialization.Field{}, false
}
