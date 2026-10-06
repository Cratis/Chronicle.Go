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

func TestBinaryNamesUseOrdinalUppercaseNotFullCaseFolding(t *testing.T) {
	for _, tc := range []struct {
		left, right string
		collision   bool
	}{
		{"é", "É", true},
		{"σ", "ς", true},
		{"𐐀", "𐐨", true},
		{"k", "K", false},
		{"i", "ı", false},
		{"s", "ſ", false},
		{"ß", "ẞ", false},
		{"é", "é", false}, // No normalization; the combining mark also fails the identifier grammar.
	} {
		t.Run(tc.left+"/"+tc.right, func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{
				{Name: "Left", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"` + tc.left + `"`)},
				{Name: "Right", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"` + tc.right + `"`)},
				{Name: "Payload", Type: reflect.TypeFor[[]byte]()},
			})
			plan, err := serialization.Compile(typ)
			if tc.collision || tc.right == "é" {
				if plan != nil || !errors.Is(err, faults.ErrUnsupported) {
					t.Fatalf("case/grammar collision admitted: %v", err)
				}
			} else if err != nil {
				t.Fatalf("distinct ordinal names refused: %v", err)
			}
		})
	}
}

func TestBinaryNamesRefuseNewOrdinalCollisionAfterNaming(t *testing.T) {
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
			plan, err := serialization.Compile(typ)
			if err != nil {
				t.Fatal(err)
			}
			for _, policy := range []serialization.NamingPolicy{serialization.CamelCase, serialization.LegacyGoCamelCase} {
				// CLR ordinal casing does not equate Kelvin sign with K, but
				// naming converts the untagged Kelvin sign into ASCII k.
				if next, err := plan.WithNamingPolicy(policy); next != nil || !errors.Is(err, faults.ErrUnsupported) {
					t.Fatalf("new ordinal collision admitted after naming: %v, %v", next, err)
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
