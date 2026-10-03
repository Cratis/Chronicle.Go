// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type ordinaryFamilyHolder struct{ Selected any }
type wrappedVariant struct{ Holder ordinaryFamilyHolder }

func TestDerivedOrdinaryObjectInsideVariantKeepsDeclaredFamilyContext(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[any, wrappedVariant]("wrapped"), serialization.Derived[any, derivedfixtures.RobotValue]("robot"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		policy serialization.NamingPolicy
	}{{"DefaultNamingPolicy", serialization.PreservePropertyNames}, {"CamelCaseNamingPolicy", serialization.CamelCase}} {
		plan := derivedPlan[derivedEnvelope](t, codecs, tc.policy)
		field, _ := serialization.FieldAt(plan.Fields(), "value")
		want := wrappedVariant{Holder: ordinaryFamilyHolder{Selected: derivedfixtures.RobotValue{Count: 42}}}
		data, err := field.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		captured, err := os.ReadFile("testdata/derived/" + tc.name + ".wrapped.payload.json")
		if err != nil || !bytes.Equal(data, bytes.TrimSpace(captured)) {
			t.Fatalf("ordinary nested context: %s %v; want %s", data, err, captured)
		}
		var value derivedEnvelope
		if err := plan.Unmarshal(append(append([]byte(`{"value":`), data...), '}'), &value); err != nil || !reflect.DeepEqual(value.Value, want) {
			t.Fatalf("decode: %#v %v", value, err)
		}
	}
}

func TestDerivedRecursiveValuesNullElementsAndDiscardedConcreteCodecs(t *testing.T) {
	plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), serialization.LegacyGoCamelCase)
	want := derivedfixtures.MembersChanged{Primary: &derivedfixtures.HumanValue{Children: []derivedfixtures.Member{&derivedfixtures.HumanValue{Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}}}}
	data, err := plan.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var value derivedfixtures.MembersChanged
	if err := plan.Unmarshal(data, &value); err != nil || !reflect.DeepEqual(value, want) {
		t.Fatalf("recursive: %#v %v", value, err)
	}
	for _, data := range []string{`{"members":[null]}`, `{"lookup":{"one":null}}`} {
		if err := plan.Unmarshal([]byte(data), &value); !errors.Is(err, faults.ErrProtocol) {
			t.Fatalf("null family collection element: %v", err)
		}
	}
	codecs, err := serialization.NewCodecs(serialization.Derived[any, encodedConcept[string]]("concept-object"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := serialization.CompileWith(reflect.TypeFor[derivedEnvelope](), serialization.Config{Codecs: codecs}); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("nonobject variant codec admitted: %v", err)
	}
	// Preserve the actual failing C# fixture, not just a manually reproduced shape.
	fixture, err := os.ReadFile("testdata/derived/DefaultNamingPolicy.unsafe-direct.payload.json")
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(fixture, &object); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(object["Actor"], &object); err != nil {
		t.Fatal(err)
	}
	child := map[string]json.RawMessage{}
	if err := json.Unmarshal(object["child"], &child); err != nil {
		t.Fatal(err)
	}
	if child["_derivedTypeId"] != nil || child["Name"] == nil {
		t.Fatal("captured failing child no longer demonstrates loss")
	}
}
