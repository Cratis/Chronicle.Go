// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/serialization"
)

type enumSample int32
type enumBits int32
type enumNoZero int32

type enumDocument struct {
	Value    enumSample
	Optional *enumSample
	Values   []enumSample
	Bits     enumBits
}

func enumCodecs(t *testing.T) *serialization.Codecs {
	t.Helper()
	codecs, err := serialization.NewCodecs(
		serialization.Enum(serialization.EnumMember[enumSample]{Name: "Negative", Value: -1},
			serialization.EnumMember[enumSample]{Name: "One", Value: 1},
			serialization.EnumMember[enumSample]{Name: "Zero", Value: 0},
			serialization.EnumMember[enumSample]{Name: "Min", Value: -2147483648},
			serialization.EnumMember[enumSample]{Name: "Max", Value: 2147483647}),
		serialization.Flags(serialization.EnumMember[enumBits]{Name: "None", Value: 0},
			serialization.EnumMember[enumBits]{Name: "A", Value: 1},
			serialization.EnumMember[enumBits]{Name: "B", Value: 2},
			serialization.EnumMember[enumBits]{Name: "AB", Value: 3},
			serialization.EnumMember[enumBits]{Name: "All", Value: -1}),
		serialization.Enum(serialization.EnumMember[enumNoZero]{Name: "One", Value: 1}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return codecs
}

func enumPlan[T any](t *testing.T) *serialization.Plan {
	t.Helper()
	p, err := serialization.CompileWith(reflect.TypeFor[T](), serialization.Config{Codecs: enumCodecs(t)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestEnumDeclaredValuesRoundTrip(t *testing.T) {
	p := enumPlan[enumDocument](t)
	for _, token := range []string{`1`, `"One"`, `"one"`, `"ONE"`, `" One "`, `"+1"`, `"01"`, `"One,One"`} {
		t.Run(token, func(t *testing.T) {
			var got enumDocument
			if err := p.Unmarshal([]byte(`{"Value":`+token+`,"Bits":"a, b","Values":["Min","Max","Negative"]}`), &got); err != nil {
				t.Fatal(err)
			}
			if got.Value != 1 || got.Bits != 3 || !reflect.DeepEqual(got.Values, []enumSample{-2147483648, 2147483647, -1}) {
				t.Fatalf("decoded %#v", got)
			}
			data, err := p.Marshal(got)
			if err != nil || string(data) != `{"Value":1,"Values":[-2147483648,2147483647,-1],"Bits":3}` {
				t.Fatalf("encoded %s: %v", data, err)
			}
		})
	}
}

func TestEnumInvalidPayloadsAreAtomicAndRedacted(t *testing.T) {
	p := enumPlan[enumDocument](t)
	for _, token := range []string{`null`, `9`, `"9"`, `"hostile-secret"`, `1.0`, `1e0`, `2147483648`, `-2147483649`, `true`, `false`, `{}`, `[]`, `""`, `" "`, `"One,"`, `"1, 1"`, `"1.0"`, `"1e0"`} {
		t.Run(token, func(t *testing.T) {
			original := enumDocument{Value: -1, Values: []enumSample{1}}
			got := original
			err := p.Unmarshal([]byte(`{"Value":`+token+`}`), &got)
			if err == nil {
				t.Fatal("invalid value accepted")
			}
			if !reflect.DeepEqual(got, original) {
				t.Fatal("target changed on error")
			}
			if strings.Contains(err.Error(), "hostile-secret") {
				t.Fatal("payload leaked")
			}
		})
	}
	for _, input := range []string{`{"Bits":5}`, `{"Bits":"A, All, B","Value":9}`, `{"Optional":"missing"}`, `{"Values":[1,9]}`, `{"Values":[null]}`} {
		var target enumDocument
		if err := p.Unmarshal([]byte(input), &target); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	for _, value := range []enumDocument{{Value: 9}, {Bits: 5}, {Bits: 8}, {Values: []enumSample{1, 9}}} {
		if _, err := p.Marshal(value); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("unknown encoded: %v", err)
		}
	}
}

func TestEnumMissingAndNull(t *testing.T) {
	p := enumPlan[enumDocument](t)
	for _, input := range []string{`{}`, `{"Optional":null,"Values":null}`} {
		var got enumDocument
		if err := p.Unmarshal([]byte(input), &got); err != nil {
			t.Fatal(err)
		}
		if got.Value != 0 || got.Optional != nil || got.Values == nil || len(got.Values) != 0 {
			t.Fatalf("presence: %#v", got)
		}
	}
	nonzero := enumPlan[struct{ Value enumNoZero }](t)
	var got struct{ Value enumNoZero }
	if err := nonzero.Unmarshal([]byte(`{}`), &got); err == nil {
		t.Fatal("manufactured undeclared zero")
	}
}

func TestEnumConfigurationRefusesAmbiguity(t *testing.T) {
	one := serialization.EnumMember[enumSample]{Name: "One", Value: 1}
	valid := serialization.Enum(one)
	for name, registrations := range map[string][]serialization.Codec{
		"zero declaration": {{}},
		"empty":            {serialization.Enum[enumSample]()},
		"builtin":          {serialization.Enum(serialization.EnumMember[int32]{Name: "One", Value: 1})},
		"duplicate type":   {valid, valid},
		"conflicting kind": {valid, serialization.Flags(one)},
		"alias":            {serialization.Enum(one, serialization.EnumMember[enumSample]{Name: "Alias", Value: 1})},
		"case collision":   {serialization.Enum(one, serialization.EnumMember[enumSample]{Name: "one", Value: 2})},
		"blank name":       {serialization.Enum(serialization.EnumMember[enumSample]{Name: "", Value: 1})},
		"comma":            {serialization.Enum(serialization.EnumMember[enumSample]{Name: "One,Two", Value: 1})},
		"unicode":          {serialization.Enum(serialization.EnumMember[enumSample]{Name: "Öne", Value: 1})},
		"numeric":          {serialization.Enum(serialization.EnumMember[enumSample]{Name: "1", Value: 1})},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := serialization.NewCodecs(registrations...); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("configuration error: %v", err)
			}
		})
	}
	// Codec remains comparable, including enum declarations.
	if valid != valid {
		t.Fatal("non-reflexive declaration")
	}
}
