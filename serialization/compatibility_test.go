// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

func TestNumericFormatsMatchKernel(t *testing.T) {
	for _, tc := range []struct {
		value  any
		format string
	}{
		{int(0), "int64"}, {int8(0), "int16"}, {int16(0), "int16"}, {int32(0), "int32"}, {int64(0), "int64"},
		{uint(0), "uint64"}, {uint8(0), "byte"}, {uint16(0), "uint32"}, {uint32(0), "uint32"}, {uint64(0), "uint64"},
		{Amount(0), "uint64"}, {float32(0), "float"}, {float64(0), "double"},
	} {
		t.Run(reflect.TypeOf(tc.value).String(), func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: reflect.TypeOf(tc.value)}})
			plan, err := serialization.Compile(typ)
			if err != nil {
				t.Fatal(err)
			}
			var schema struct {
				Properties map[string]struct{ Format string }
			}
			if err = json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
				t.Fatal(err)
			}
			if schema.Properties["value"].Format != tc.format {
				t.Fatalf("schema = %s", plan.Schema())
			}
		})
	}
}

func TestNullableScalars(t *testing.T) {
	type nullable struct {
		Flag  *bool
		Count *int64
		When  *time.Time
		ID    *uuid.UUID
		Value int64
	}
	plan, err := serialization.Compile(reflect.TypeFor[nullable]())
	if err != nil {
		t.Fatal(err)
	}
	var schema struct{ Properties map[string]map[string]any }
	if err = json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schema.Properties["flag"]["type"], []any{"boolean", "null"}) || schema.Properties["count"]["format"] != "int64?" || schema.Properties["when"]["format"] != "date-time?" || schema.Properties["id"]["format"] != "uuid?" || schema.Properties["value"]["format"] != "int64" {
		t.Fatalf("schema = %s", plan.Schema())
	}
	data, err := plan.Marshal(nullable{})
	if err != nil || string(data) != `{"value":0}` {
		t.Fatalf("nil = %s, %v", data, err)
	}
	flag, count, when, id := false, int64(0), time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	data, err = plan.Marshal(nullable{Flag: &flag, Count: &count, When: &when, ID: &id})
	if err != nil || string(data) != `{"count":0,"flag":false,"id":"00112233-4455-6677-8899-aabbccddeeff","value":0,"when":"2026-01-02T03:04:05Z"}` {
		t.Fatalf("present = %s, %v", data, err)
	}
}

func TestUnsignedMongoDBRange(t *testing.T) {
	for _, value := range []any{struct{ N uint64 }{1 << 63}, struct{ N Amount }{Amount(1<<64 - 1)}, struct{ N []uint64 }{[]uint64{1 << 63}}} {
		plan, err := serialization.Compile(reflect.TypeOf(value))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = plan.Marshal(value); !errors.Is(err, faults.ErrUnsupported) {
			t.Fatalf("unsigned overflow accepted: %v", err)
		}
	}
}

func TestDictionaryIntegerPrecision(t *testing.T) {
	for _, tc := range []struct {
		name     string
		value    any
		rejected bool
	}{
		{"unsigned boundary", struct{ Values map[string]uint64 }{map[string]uint64{"n": 1 << 53}}, false},
		{"unsigned overflow", struct{ Values map[string]uint64 }{map[string]uint64{"n": 1<<53 + 1}}, true},
		{"signed boundary", struct{ Values map[string]int64 }{map[string]int64{"n": -(1 << 53)}}, false},
		{"signed overflow", struct{ Values map[string]int64 }{map[string]int64{"n": -(1 << 53) - 1}}, true},
		{"nested array", struct{ Values map[string][]Amount }{map[string][]Amount{"n": {1<<53 + 1}}}, true},
		{"nested pointer", struct{ Values map[string]*uint64 }{map[string]*uint64{"n": new(uint64(1<<53 + 1))}}, true},
		{"nested struct", struct{ Values map[string]struct{ N uint64 } }{map[string]struct{ N uint64 }{"n": {1<<53 + 1}}}, true},
		{"ordinary wide array", struct{ Values []uint64 }{[]uint64{1<<53 + 1}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := serialization.Compile(reflect.TypeOf(tc.value))
			if err != nil {
				t.Fatal(err)
			}
			_, err = plan.Marshal(tc.value)
			if tc.rejected && !errors.Is(err, faults.ErrUnsupported) || !tc.rejected && err != nil {
				t.Fatalf("error = %v, rejected = %v", err, tc.rejected)
			}
		})
	}
}

type zeroNumber int

func (n zeroNumber) IsZero() bool { return n == -1 }

type pointerZeroNumber int

func (n *pointerZeroNumber) IsZero() bool { return *n == -1 }

type zeroStruct struct {
	N int `json:"n"`
}

func (v zeroStruct) IsZero() bool { return v.N == -1 }

func TestOmitZeroMatchesEncodingJSON(t *testing.T) {
	type shape struct {
		Number          zeroNumber         `json:"number,omitzero"`
		PointerReceiver pointerZeroNumber  `json:"pointerReceiver,omitzero"`
		Pointer         *pointerZeroNumber `json:"pointer,omitzero"`
		Struct          zeroStruct         `json:"struct,omitzero"`
		When            time.Time          `json:"when,omitzero"`
	}
	plan, err := serialization.Compile(reflect.TypeFor[shape]())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []shape{
		{},
		{Number: -1, PointerReceiver: -1, Pointer: new(pointerZeroNumber(-1)), Struct: zeroStruct{-1}, When: time.Time{}.In(time.FixedZone("zero", 0))},
		{Number: 1, PointerReceiver: 1, Pointer: new(pointerZeroNumber(1)), Struct: zeroStruct{1}, When: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
	} {
		for _, input := range []any{value, &value} {
			got, err := plan.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			want, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			var gotMap, wantMap map[string]json.RawMessage
			if err = json.Unmarshal(got, &gotMap); err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(want, &wantMap); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotMap, wantMap) {
				t.Fatalf("got %s, want %s", got, want)
			}
		}
	}
}
