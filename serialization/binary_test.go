// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/serialization"
)

type binaryNamed []byte
type binaryElement byte
type binaryFamily interface{ binaryMember() }
type binaryVariant struct{ Payload []byte }

func (binaryVariant) binaryMember() {}

func TestBinaryMissingNullEmptyAndNullable(t *testing.T) {
	for _, compile := range []func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error){serialization.Compile, serialization.CompileReadModel} {
		p, err := compile(reflect.TypeFor[struct {
			Payload  []byte
			Optional *[]byte
		}]())
		if err != nil {
			t.Fatal(err)
		}
		for _, input := range []string{`{}`, `{"Payload":null,"Optional":null}`, `{"Payload":""}`} {
			var value struct {
				Payload  []byte
				Optional *[]byte
			}
			if err := p.Unmarshal([]byte(input), &value); err != nil {
				t.Fatal(err)
			}
			if value.Payload == nil || len(value.Payload) != 0 || value.Optional != nil {
				t.Fatal("missing/null binary was not normalized")
			}
		}
	}
}

func TestBinaryDecodedValuesDoNotAliasInput(t *testing.T) {
	p := binaryPlan(t, serialization.PreservePropertyNames)
	input := []byte(`{"Payload":"AQ==","Optional":"AQ==","Nested":{"Inner":"AQ=="}}`)
	var a, b binaryEvent
	if err := p.Unmarshal(input, &a); err != nil {
		t.Fatal(err)
	}
	if err := p.Unmarshal(input, &b); err != nil {
		t.Fatal(err)
	}
	for i := range input {
		input[i] = 0
	}
	a.Payload[0] = 9
	if b.Payload[0] != 1 || (*a.Optional)[0] != 1 || a.Nested.Inner[0] != 1 {
		t.Fatal("decoded binary storage aliases")
	}
	original := b
	if err := p.Unmarshal([]byte(`{"Payload":"secret-invalid"}`), &b); !errors.Is(err, chronicle.ErrProtocol) || !reflect.DeepEqual(b, original) {
		t.Fatal("failed decode replaced target")
	}
}

func TestBinaryFieldMetadata(t *testing.T) {
	p := binaryPlan(t, serialization.PreservePropertyNames)
	for _, name := range []string{"Payload", "Optional", "Nested.Inner"} {
		f, ok := serialization.FieldAt(p.Fields(), name)
		if !ok || f.Scalar != serialization.Binary || f.Scalar.IsPrimitive() || !f.ContainsBinary() || f.Format != "byte-array" || f.Collection || f.Nullable != (name == "Optional") {
			t.Fatalf("invalid binary field metadata: %#v", f)
		}
		if _, ok := f.Element(); ok {
			t.Fatal("binary leaf exposed byte element")
		}
	}
}

func TestBinaryUnsupportedPlacementsFailBeforeRegistration(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[binaryFamily, binaryVariant]("binary"))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{
		struct{ ByKey map[string][]byte }{}, struct{ Payload []binaryElement }{}, struct{ Payload json.RawMessage }{},
		struct{ Payload [][][]byte }{}, struct{ Payload [2][]byte }{}, struct{ Payload **[]byte }{}, struct{ Payload *[][]byte }{},
		struct{ Rows []binaryNested }{},
		struct {
			Payload []byte `chronicle:"pii"`
		}{}, struct {
			Payload []byte `chronicle:"encrypted"`
		}{}, struct {
			Payload []byte `chronicle:"index"`
		}{},
		struct {
			Nested binaryNested `chronicle:"encrypted"`
		}{}, struct{ Payload binaryFamily }{},
	} {
		config := serialization.Config{}
		if reflect.TypeOf(value).Field(0).Type == reflect.TypeFor[binaryFamily]() {
			config.Codecs = codecs
		}
		if _, err := serialization.CompileWith(reflect.TypeOf(value), config); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("binary placement admitted: %T: %v", value, err)
		}
	}
}

func TestBinaryConceptsAndUnusedDerivativesRefuse(t *testing.T) {
	if _, err := serialization.Compile(reflect.TypeFor[struct{ Payload encodedConcept[[]byte] }]()); err == nil {
		t.Fatal("binary concept admitted")
	}
	codecs, err := serialization.NewCodecs(serialization.Derived[binaryFamily, binaryVariant]("binary"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := serialization.CompileWith(reflect.TypeFor[struct{ Name string }](), serialization.Config{Codecs: codecs}); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("unused binary derivative admitted", err)
	}
}

func TestBinaryNamedSlicesAndFixedArrays(t *testing.T) {
	type document struct {
		Payload binaryNamed
		Fixed   [2]byte
	}
	p, err := serialization.Compile(reflect.TypeFor[document]())
	if err != nil {
		t.Fatal(err)
	}
	data, err := p.Marshal(document{binaryNamed{1}, [2]byte{1, 2}})
	if err != nil || string(data) != `{"Payload":"AQ==","Fixed":[1,2]}` {
		t.Fatalf("named binary/fixed array: %s %v", data, err)
	}
	var got document
	if err := p.Unmarshal(data, &got); err != nil || !bytes.Equal(got.Payload, []byte{1}) || got.Fixed != [2]byte{1, 2} {
		t.Fatal("named binary roundtrip failed")
	}
}

func TestBinaryNamingRebindPreservesRepresentation(t *testing.T) {
	p := binaryPlan(t, serialization.PreservePropertyNames)
	next, err := p.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"Payload":"\u002B/8=","Optional":null,"Nested":{"Inner":"AQ=="}}`)
	rebound, err := p.RebindJSON(data, next)
	if err != nil {
		t.Fatal(err)
	}
	var got, want binaryEvent
	if err := p.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	if err := next.Unmarshal(rebound, &got); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("snapshot rebind changed binary representation", err)
	}
	before, _ := serialization.FieldAt(p.Fields(), "Payload")
	after, _ := serialization.FieldAt(next.Fields(), "payload")
	if !before.SameRepresentation(after) {
		t.Fatal("binary representation changed during rebind")
	}
}
