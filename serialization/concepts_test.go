// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/fundamentals.go/concepts"
)

type encodedConcept[T any] struct{ JSON string }

func (encodedConcept[T]) ConceptValue() T                { panic("discovery must not execute") }
func (v encodedConcept[T]) MarshalJSON() ([]byte, error) { return []byte(v.JSON), nil }
func (*encodedConcept[T]) UnmarshalJSON([]byte) error    { return nil }
func (v encodedConcept[T]) MarshalText() ([]byte, error) { return []byte(v.JSON), nil }
func (*encodedConcept[T]) UnmarshalText([]byte) error    { return nil }

func TestConceptCodecsCannotBypassScalarShapeOrWidth(t *testing.T) {
	for _, value := range []any{
		encodedConcept[uint8]{"256"}, encodedConcept[int8]{"-129"}, encodedConcept[int64]{"1.5"},
		encodedConcept[concepts.UUID]{`"00112233-4455-6677-8899-AABBCCDDEEFF"`},
		encodedConcept[concepts.DateOnly]{`"2026-02-30"`}, encodedConcept[concepts.TimeOnly]{`"25:00:00"`},
		encodedConcept[concepts.TimeSpan]{`"PT1H"`},
	} {
		t.Run(reflect.TypeOf(value).String(), func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: reflect.TypeOf(value)}})
			plan, err := serialization.Compile(typ)
			if err != nil {
				t.Fatal(err)
			}
			event := reflect.New(typ).Elem()
			event.Field(0).Set(reflect.ValueOf(value))
			if _, err := plan.Marshal(event.Interface()); !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("invalid codec accepted: %v", err)
			}
		})
	}
}

func TestSharedScalarPointersAndCollectionsUseUnderlyingFormats(t *testing.T) {
	for _, tc := range []struct {
		typ    reflect.Type
		format string
	}{
		{reflect.TypeFor[concepts.UUID](), "uuid"}, {reflect.TypeFor[conceptfixtures.AuthorID](), "uuid"},
		{reflect.TypeFor[concepts.DateOnly](), "date"}, {reflect.TypeFor[concepts.TimeOnly](), "time"}, {reflect.TypeFor[concepts.TimeSpan](), "duration"},
	} {
		t.Run(tc.format+tc.typ.Name(), func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{
				{Name: "Optional", Type: reflect.PointerTo(reflect.PointerTo(tc.typ))},
				{Name: "Slice", Type: reflect.SliceOf(tc.typ)},
				{Name: "Map", Type: reflect.MapOf(reflect.TypeFor[string](), tc.typ)},
			})
			plan, err := serialization.Compile(typ)
			if err != nil {
				t.Fatal(err)
			}
			var schema struct {
				Properties map[string]struct {
					Format                      string
					Items, AdditionalProperties struct{ Format string }
				}
			}
			if err := json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
				t.Fatal(err)
			}
			if schema.Properties["Optional"].Format != tc.format+"?" || schema.Properties["Slice"].Items.Format != tc.format || schema.Properties["Map"].AdditionalProperties.Format != tc.format {
				t.Fatal(plan.Schema())
			}
			data, err := plan.Marshal(reflect.New(typ).Elem().Interface())
			if err != nil || string(data) != "{}" {
				t.Fatalf("nil: %s %v", data, err)
			}
			value := reflect.New(typ).Elem()
			pointer := reflect.New(tc.typ)
			outer := reflect.New(pointer.Type())
			outer.Elem().Set(pointer)
			value.Field(0).Set(outer)
			value.Field(1).Set(reflect.MakeSlice(reflect.SliceOf(tc.typ), 1, 1))
			m := reflect.MakeMap(value.Field(2).Type())
			m.SetMapIndex(reflect.ValueOf("zero"), reflect.Zero(tc.typ))
			value.Field(2).Set(m)
			if _, err := plan.Marshal(value.Interface()); err != nil {
				t.Fatalf("zero scalars: %v", err)
			}
		})
	}
}

type pointerConcept string

func (*pointerConcept) ConceptValue() string { panic("must not execute") }

type unforwardedDate concepts.DateOnly

func TestInvalidConceptDeclarationsFailWithInspectableReasons(t *testing.T) {
	for _, tc := range []struct {
		typ    reflect.Type
		reason concepts.InvalidReason
	}{
		{reflect.TypeFor[pointerConcept](), concepts.ReasonPointerMethod},
		{reflect.TypeFor[encodedConcept[conceptfixtures.Name]](), concepts.ReasonNestedConcept},
		{reflect.TypeFor[unforwardedDate](), concepts.ReasonMissingForwarding},
	} {
		typ := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: tc.typ}})
		_, err := serialization.Compile(typ)
		var detail *concepts.TypeError
		if !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &detail) || detail.Reason != tc.reason {
			t.Fatalf("%v: %v (%+v)", tc.typ, err, detail)
		}
	}
}

func TestConceptMapKeysFailRatherThanBypassTextCodecs(t *testing.T) {
	_, err := serialization.Compile(reflect.TypeFor[struct {
		Values map[conceptfixtures.Name]string
	}]())
	if !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("custom map keys accepted: %v", err)
	}
}
