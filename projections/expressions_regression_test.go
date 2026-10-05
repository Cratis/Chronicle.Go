// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/serialization"
)

func TestLiteralValidatesWrappedConceptIntegerRange(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[conceptfixtures.Unsigned](), reflect.TypeFor[*conceptfixtures.Unsigned]()} {
		fields := representationFields(t, typ, serialization.PreservePropertyNames)
		for _, tc := range []struct {
			literal string
			valid   bool
		}{
			{"9223372036854775807", true}, {"9223372036854775808", false}, {"18446744073709551615", false}, {"18446744073709551616", false}, {"-1", false},
		} {
			err := validateLiteral(expression{kind: literalExpression, literalKind: declarations.Number, text: tc.literal}, fields[0])
			if (err == nil) != tc.valid {
				t.Errorf("%v literal %s: %v, valid=%v", typ, tc.literal, err, tc.valid)
			}
		}
	}
}

func TestStringFormatsMustBeCompatible(t *testing.T) {
	for _, tc := range []struct {
		target, source string
		valid          bool
	}{
		{"date", "uuid", false}, {"uuid", "time-span", false}, {"time", "date-time", false},
		{"uuid", "guid", true}, {"guid", "uuid", true}, {"date", "date", true}, {"", "date-time", true}, {"uuid", "", true},
	} {
		target := serialization.Field{Type: reflect.TypeFor[string](), Scalar: serialization.String, Format: tc.target}
		source := serialization.Field{Type: reflect.TypeFor[string](), Scalar: serialization.String, Format: tc.source}
		if got := scalarCompatible(target, source, nil, nil); got != tc.valid {
			t.Errorf("%q <- %q = %v", tc.target, tc.source, got)
		}
	}
}

func representationFields(t *testing.T, typ reflect.Type, policy serialization.NamingPolicy) []serialization.Field {
	t.Helper()
	plan, err := serialization.Compile(reflect.StructOf([]reflect.StructField{{Name: "Value", Type: typ}}), policy)
	if err != nil {
		t.Fatal(err)
	}
	return plan.Fields()
}

func TestObjectAndCollectionCompatibilityUsesSerializedProperties(t *testing.T) {
	type sourceObject struct {
		Text  string `json:"shared"`
		Extra bool
	}
	type targetObject struct {
		Number int `json:"shared"`
	}
	type renamedObject struct {
		Renamed string `json:"shared"`
		Missing int
	}
	type absentObject struct{ Other int }
	for _, tc := range []struct {
		name           string
		target, source reflect.Type
		valid          bool
	}{
		{"mismatched property", reflect.TypeFor[targetObject](), reflect.TypeFor[sourceObject](), false},
		{"serialized names and missing properties", reflect.TypeFor[renamedObject](), reflect.TypeFor[sourceObject](), true},
		{"no shared properties", reflect.TypeFor[absentObject](), reflect.TypeFor[sourceObject](), true},
		{"slice elements", reflect.TypeFor[[]int](), reflect.TypeFor[[]string](), false},
		{"widened slice elements", reflect.TypeFor[[]float64](), reflect.TypeFor[[]int32](), true},
		{"array to slice", reflect.TypeFor[[]int64](), reflect.TypeFor[[2]int32](), true},
		{"nested collections", reflect.TypeFor[[][]int](), reflect.TypeFor[[][]string](), false},
		{"nested objects", reflect.TypeFor[struct{ Child targetObject }](), reflect.TypeFor[struct{ Child sourceObject }](), false},
		{"collection objects", reflect.TypeFor[[]targetObject](), reflect.TypeFor[[]sourceObject](), false},
		{"compatible collection objects", reflect.TypeFor[[]renamedObject](), reflect.TypeFor[[]sourceObject](), true},
		{"dictionary values", reflect.TypeFor[map[string]int](), reflect.TypeFor[map[string]string](), false},
		{"dictionary objects", reflect.TypeFor[map[string]targetObject](), reflect.TypeFor[map[string]sourceObject](), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
				targetFields := representationFields(t, tc.target, policy)
				sourceFields := representationFields(t, tc.source, policy)
				if got := scalarCompatible(targetFields[0], sourceFields[0], targetFields, sourceFields); got != tc.valid {
					t.Errorf("policy %v: compatible=%v, want %v", policy, got, tc.valid)
				}
			}
		})
	}
}
