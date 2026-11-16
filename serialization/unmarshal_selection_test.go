// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

func TestDecodeSelectsExactNameBeforeSortedCaseInsensitiveFallback(t *testing.T) {
	type document struct {
		Value string `json:"value"`
	}
	plan, err := serialization.Compile(reflect.TypeFor[document](), serialization.LegacyGoCamelCase)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, data, want string
	}{
		{"fallback", `{"Value":"second","VALUE":"first"}`, "first"},
		{"exact", `{"Value":"second","value":"exact","VALUE":"first"}`, "exact"},
		{"missing", `{"other":"ignored"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got document
			if err := plan.Unmarshal([]byte(tc.data), &got); err != nil || got.Value != tc.want {
				t.Fatalf("decode = %+v, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestDecodeFixedArrayIgnoresExcessItems(t *testing.T) {
	type document struct {
		Values [1]int `json:"values"`
	}
	plan, err := serialization.Compile(reflect.TypeFor[document](), serialization.LegacyGoCamelCase)
	if err != nil {
		t.Fatal(err)
	}
	var got document
	if err := plan.Unmarshal([]byte(`{"values":[7,"not decoded"]}`), &got); err != nil || got.Values != [1]int{7} {
		t.Fatalf("decode = %+v, %v; want [7]", got, err)
	}
}
