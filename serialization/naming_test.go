// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Golden cases from Fundamentals d2accc4a79b6bcf2708213c97093ab5ba6c06381,
// Source/DotNET/Fundamentals.Specs/Strings/for_ToCamelCase/when_converting.cs.
// Null has no Go string counterpart; unpaired UTF-16 surrogates have no valid
// UTF-8 counterpart. Supplementary letters must remain unchanged like C# chars.
func TestFundamentalsCamelCaseGolden(t *testing.T) {
	for _, tc := range [][2]string{
		{"URLValue", "URLValue"}, {"URL", "URL"}, {"ID", "ID"}, {"I", "i"}, {"", ""},
		{"😀葛🀄", "😀葛🀄"}, {"ΆλφαΒήταΓάμμα", "άλφαΒήταΓάμμα"}, {"𐐀𐐨𐐨𐐀𐐨𐐨", "𐐀𐐨𐐨𐐀𐐨𐐨"},
		{"Person", "person"}, {"iPhone", "iPhone"}, {"IPhone", "IPhone"}, {"I Phone", "i Phone"},
		{"I  Phone", "i  Phone"}, {" IPhone", " IPhone"}, {" IPhone ", " IPhone "},
		{"IsCIA", "isCIA"}, {"VmQ", "vmQ"}, {"Xml2Json", "xml2Json"}, {"SnAkEcAsE", "snAkEcAsE"},
		{"SnA__kEcAsE", "snA__kEcAsE"}, {"SnA__ kEcAsE", "snA__ kEcAsE"},
		{"already_snake_case_ ", "already_snake_case_ "}, {"IsJSONProperty", "isJSONProperty"},
		{"SHOUTING_CASE", "SHOUTING_CASE"}, {"9999-12-31T23:59:59.9999999Z", "9999-12-31T23:59:59.9999999Z"},
		{"Hi!! This is text. Time to test.", "hi!! This is text. Time to test."}, {"BUILDING", "BUILDING"},
		{"BUILDING Property", "BUILDING Property"}, {"Building Property", "building Property"}, {"BUILDING PROPERTY", "BUILDING PROPERTY"},
	} {
		t.Run(tc[0], func(t *testing.T) {
			if got := CamelCase.name(tc[0]); got != tc[1] {
				t.Fatalf("got %q, want %q", got, tc[1])
			}
		})
	}
}

func TestNamingPoliciesCompileOneFieldPlan(t *testing.T) {
	type nested struct {
		Person   string
		URLValue string
	}
	type event struct {
		ID       string
		URLValue string
		Person   string
		Stable   string `json:"Id"`
		Nested   nested
	}
	for _, tc := range []struct {
		policy                  NamingPolicy
		id, url, person, nested string
	}{
		{PreservePropertyNames, "ID", "URLValue", "Person", "Nested"},
		{CamelCase, "ID", "URLValue", "person", "nested"},
		{LegacyGoCamelCase, "id", "urlValue", "person", "nested"},
	} {
		plan, err := Compile(reflect.TypeFor[event](), tc.policy)
		if err != nil {
			t.Fatal(err)
		}
		data, err := plan.Marshal(event{ID: "1", URLValue: "url", Person: "Ada", Stable: "shared", Nested: nested{"Grace", "nested-url"}})
		if err != nil {
			t.Fatal(err)
		}
		var content map[string]json.RawMessage
		var schema struct{ Properties map[string]json.RawMessage }
		if err := json.Unmarshal(data, &content); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{tc.id, tc.url, tc.person, tc.nested, "Id"} {
			if content[name] == nil || schema.Properties[name] == nil {
				t.Fatalf("missing %q: %s / %s", name, data, plan.Schema())
			}
			if _, ok := FieldAt(plan.Fields(), name); !ok {
				t.Fatalf("missing metadata for %q", name)
			}
		}
		if _, ok := FieldAt(plan.Fields(), tc.nested+"."+tc.person); !ok {
			t.Fatal("nested policy not applied")
		}
	}
}

func TestNamingCollisionsAndInvalidPoliciesFailClosed(t *testing.T) {
	type collision struct {
		Person   string
		Explicit string `json:"person"`
	}
	if _, err := Compile(reflect.TypeFor[collision]()); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []NamingPolicy{CamelCase, LegacyGoCamelCase, 255} {
		if _, err := Compile(reflect.TypeFor[collision](), policy); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("policy %d: %v", policy, err)
		}
	}
}
