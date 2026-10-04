// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"encoding/json"
	"reflect"

	"github.com/cratis/chronicle.go/serialization"
)

// Parse only an integer token, then validate through the actual frozen field
// codec. Never decode into an application enum with JSON/text hooks.
func validateEnumLiteral(data []byte, target serialization.Field) error {
	var integer int32
	if json.Unmarshal(data, &integer) != nil {
		return invalid("enum literal requires an exact Int32 token")
	}
	value := reflect.New(target.Type).Elem()
	scalar := value
	if scalar.Kind() == reflect.Pointer {
		scalar.Set(reflect.New(scalar.Type().Elem()))
		scalar = scalar.Elem()
	}
	if scalar.Kind() != reflect.Int32 {
		return invalid("enum literal requires a scalar field")
	}
	scalar.SetInt(int64(integer))
	if _, err := target.Marshal(value.Interface()); err != nil {
		return invalid("enum literal is not a declared member")
	}
	return nil
}
