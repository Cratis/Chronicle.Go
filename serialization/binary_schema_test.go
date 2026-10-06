// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/serialization"
)

func TestBinarySchemaMatchesPackagedGenerators(t *testing.T) {
	checks, records := 0, 0
	for _, profile := range loadBinaryCapture(t).Profiles {
		for _, s := range profile.Schemas {
			var want struct {
				Properties map[string]any
				Required   []string
			}
			if json.Unmarshal(s.Result.Output, &want) != nil {
				t.Fatal("invalid packaged schema")
			}
			payload := "Payload"
			if strings.Contains(profile.NamingPolicy, "CamelCase") {
				payload = "payload"
			}
			if s.DeclaredType == "BinaryRecord" {
				records++
				if !reflect.DeepEqual(want.Required, []string{payload}) {
					t.Fatal("record required-list control changed")
				}
			}
			if s.DeclaredType != "BinaryEvent" && s.DeclaredType != "BinaryModel" {
				continue
			}
			checks++
			compile := serialization.Compile
			if s.Operation == "GenerateForReadModel" {
				compile = serialization.CompileReadModel
			}
			p, err := compile(reflect.TypeFor[binaryEvent](), binaryPolicy(profile.NamingPolicy))
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Properties map[string]any
				Required   []string
			}
			if json.Unmarshal([]byte(p.Schema()), &got) != nil {
				t.Fatal("invalid Go schema")
			}
			key := "Nested"
			if strings.Contains(profile.NamingPolicy, "CamelCase") {
				key = "nested"
			}
			delete(want.Properties[key].(map[string]any), "title")
			delete(got.Properties[key].(map[string]any), "required")
			if !reflect.DeepEqual(got.Properties, want.Properties) || len(want.Required) != 0 || !reflect.DeepEqual(got.Required, []string{key}) {
				t.Fatalf("binary property schema differs: %s vs %s", p.Schema(), s.Result.Output)
			}
		}
	}
	if checks != 8 || records != 4 {
		t.Fatal("incomplete schema matrix")
	}
}

func TestBinaryProviderProtectionRefusesAndUnrelatedProtectionPreservesSchema(t *testing.T) {
	p := binaryPlan(t, serialization.PreservePropertyNames)
	for _, option := range []compliance.Declaration{
		compliance.Property("Payload", compliance.Classification{PII: true}),
		compliance.Property("Optional", compliance.Classification{Encrypted: true}),
		compliance.For[binaryNested](compliance.Classification{PII: true}),
		compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
			return compliance.Classification{PII: target.Field == "Chunks"}, nil
		}),
	} {
		if _, err := p.ProtectedSchema(option); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatal("binary protection admitted", err)
		}
	}
	type mixed struct {
		Optional *[]byte
		Name     string
	}
	mixedPlan, err := serialization.Compile(reflect.TypeFor[mixed]())
	if err != nil {
		t.Fatal(err)
	}
	schema, err := mixedPlan.ProtectedSchema(compliance.Property("Name", compliance.Classification{PII: true}))
	if err != nil {
		t.Fatal(err)
	}
	var before, after struct{ Properties map[string]any }
	if json.Unmarshal([]byte(schema), &after) != nil || json.Unmarshal([]byte(mixedPlan.Schema()), &before) != nil {
		t.Fatal("invalid classified schema")
	}
	if !reflect.DeepEqual(before.Properties["Optional"], after.Properties["Optional"]) {
		t.Fatal("unrelated classification rewrote nullable binary schema")
	}
}
