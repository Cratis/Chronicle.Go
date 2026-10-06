// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func TestBinaryNamesRefuseNonASCIIRegardlessOfCaseFolding(t *testing.T) {
	for _, tc := range []struct {
		left, right string
	}{
		{"é", "É"},
		{"σ", "ς"},
		{"𐐀", "𐐨"},
		{"k", "K"},
		{"i", "ı"},
		{"s", "ſ"},
		{"ß", "ẞ"},
		{"é", "é"},
	} {
		t.Run(tc.left+"/"+tc.right, func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{
				{Name: "Left", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"` + tc.left + `"`)},
				{Name: "Right", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"` + tc.right + `"`)},
				{Name: "Payload", Type: reflect.TypeFor[[]byte]()},
			})
			plan, err := serialization.Compile(typ)
			if plan != nil || !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("non-ASCII property admitted: %v", err)
			}
		})
	}
}

func TestBinaryNamesRefuseNonASCIIUnderEveryNamingPolicy(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"alias first": reflect.TypeFor[struct {
			Note string `json:"K"`
			K    []byte
		}](),
		"alias last": reflect.TypeFor[struct {
			K    []byte
			Note string `json:"K"`
		}](),
	} {
		t.Run(name, func(t *testing.T) {
			for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
				// Preserve refuses the Kelvin sign itself; both camel policies
				// turn it into k, which collides with the tagged ASCII sibling.
				if next, err := serialization.Compile(typ, policy); next != nil || !errors.Is(err, faults.ErrUnsupported) {
					t.Fatalf("non-ASCII name or recased collision admitted: %v, %v", next, err)
				}
			}
		})
	}
}

func TestBinaryFreeNamingRetainsExactDuplicateRefusal(t *testing.T) {
	type document struct {
		Note    string `json:"payload"`
		Payload string
	}
	plan, err := serialization.Compile(reflect.TypeFor[document]())
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.CamelCase, serialization.LegacyGoCamelCase} {
		if next, err := plan.WithNamingPolicy(policy); next != nil || !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("existing exact-name duplicate behavior changed: %v, %v", next, err)
		}
	}
}
