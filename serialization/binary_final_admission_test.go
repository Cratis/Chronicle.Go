// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func TestBinaryNamesRefuseGoFoldAliases(t *testing.T) {
	for _, pair := range [][2]string{{"k", "K"}, {"s", "ſ"}} {
		for _, reverse := range []bool{false, true} {
			if reverse {
				pair[0], pair[1] = pair[1], pair[0]
			}
			t.Run(pair[0]+"/"+pair[1], func(t *testing.T) {
				typ := reflect.StructOf([]reflect.StructField{
					{Name: "Left", Type: reflect.TypeFor[[]byte](), Tag: reflect.StructTag(`json:"` + pair[0] + `"`)},
					{Name: "Right", Type: reflect.TypeFor[[]byte](), Tag: reflect.StructTag(`json:"` + pair[1] + `"`)},
				})
				for _, compile := range []func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error){serialization.Compile, serialization.CompileReadModel} {
					if plan, err := compile(typ); plan != nil || !errors.Is(err, faults.ErrUnsupported) {
						t.Fatalf("Go EqualFold alias admitted: %v", err)
					}
				}
			})
		}
	}
}

func TestBinaryASCIIDecodeSelectsOnlyIntendedField(t *testing.T) {
	type document struct {
		K []byte `json:"k"`
		S []byte `json:"s"`
	}
	plan, err := serialization.Compile(reflect.TypeFor[document]())
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{`{"k":"AQ=="}`, `{"K":"AQ=="}`} {
		var got document
		if err := plan.Unmarshal([]byte(input), &got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.K, []byte{1}) || got.S == nil || len(got.S) != 0 {
			t.Fatalf("decode selected another field: %#v", got)
		}
	}
}

func TestBinaryGraphRefusesUnrelatedRecursiveBranch(t *testing.T) {
	type recursive struct{ Next *recursive }
	type document struct {
		Payload []byte
		Tree    *recursive
	}
	if plan, err := serialization.Compile(reflect.TypeFor[document]()); plan != nil || !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("binary graph with recursive sibling admitted: %v", err)
	}
	if _, err := serialization.Compile(reflect.TypeFor[recursive]()); err != nil {
		t.Fatalf("binary-free recursion changed: %v", err)
	}
}

type binaryFreeCustomShape struct{ Payload []byte }

func (binaryFreeCustomShape) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }

func TestBinaryFreeDuplicatePrecedesLaterUnsupportedType(t *testing.T) {
	base := []reflect.StructField{
		{Name: "A", Type: reflect.TypeFor[string](), Tag: `json:"x"`},
		{Name: "B", Type: reflect.TypeFor[string](), Tag: `json:"x"`},
		{Name: "C", Type: reflect.TypeFor[chan int]()},
	}
	for name, extra := range map[string][]reflect.StructField{
		"plain":          nil,
		"ignored binary": {{Name: "Payload", Type: reflect.TypeFor[[]byte](), Tag: `json:"-"`}},
		"shadowed binary": {
			{Name: "BinaryEmbedded", Type: reflect.TypeFor[BinaryEmbedded](), Anonymous: true},
			{Name: "Payload", Type: reflect.TypeFor[string]()},
		},
		"custom shape": {{Name: "Custom", Type: reflect.TypeFor[binaryFreeCustomShape]()}},
	} {
		t.Run(name, func(t *testing.T) {
			typ := reflect.StructOf(append(append([]reflect.StructField(nil), base...), extra...))
			for _, compile := range []func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error){serialization.Compile, serialization.CompileReadModel} {
				_, err := compile(typ)
				if !errors.Is(err, faults.ErrInvalidConfiguration) || errors.Is(err, faults.ErrUnsupported) || !strings.Contains(err.Error(), "duplicate JSON property: x") {
					t.Fatalf("origin/main duplicate precedence changed: %v", err)
				}
			}
		})
	}
}

func TestBinaryAfterCompileFailureStillDefersDuplicates(t *testing.T) {
	typ := reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeFor[string](), Tag: `json:"x"`},
		{Name: "B", Type: reflect.TypeFor[string](), Tag: `json:"x"`},
		{Name: "C", Type: reflect.TypeFor[chan int]()},
		{Name: "Payload", Type: reflect.TypeFor[[]byte]()},
	})
	if _, err := serialization.Compile(typ); !errors.Is(err, faults.ErrUnsupported) || !strings.Contains(err.Error(), "unsupported JSON shape") {
		t.Fatalf("later binary field did not retain compile-error precedence: %v", err)
	}
}
