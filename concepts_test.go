// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/fundamentals.go/concepts"
)

type sharedValues struct {
	ID       concepts.UUID
	Author   conceptfixtures.AuthorID
	Name     conceptfixtures.Name
	Number   conceptfixtures.Number
	Date     concepts.DateOnly
	Time     concepts.TimeOnly
	Duration concepts.TimeSpan
	Optional **conceptfixtures.AuthorID
	Names    []conceptfixtures.Name
	Numbers  map[string]conceptfixtures.Number
	Zero     conceptfixtures.AuthorID `json:",omitzero"`
}

func TestFundamentalsEventRegistrationSchemaAndJSON(t *testing.T) {
	event, err := chronicle.RegisterEvent[sharedValues](chronicle.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	var schema struct{ Properties map[string]map[string]any }
	if err := json.Unmarshal([]byte(event.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	for name, format := range map[string]string{"ID": "uuid", "Author": "uuid", "Number": "int64", "Date": "date", "Time": "time", "Duration": "duration", "Optional": "uuid?"} {
		if schema.Properties[name]["format"] != format {
			t.Fatalf("%s: %v", name, schema.Properties[name])
		}
	}
	id, err := concepts.ParseUUID("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	var date concepts.DateOnly
	var clock concepts.TimeOnly
	var duration concepts.TimeSpan
	for text, target := range map[string]interface{ UnmarshalText([]byte) error }{"2026-01-02": &date, "03:04:05.1234567": &clock, "1.02:03:04.1234567": &duration} {
		if err := target.UnmarshalText([]byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	author := conceptfixtures.AuthorID(id)
	pointer := &author
	value := sharedValues{ID: id, Author: author, Name: "Ada", Number: 42, Date: date, Time: clock, Duration: duration, Optional: &pointer, Names: []conceptfixtures.Name{"Grace"}, Numbers: map[string]conceptfixtures.Number{"n": 7}}
	data, err := event.Descriptor().Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"ID":"00112233-4455-6677-8899-aabbccddeeff","Author":"00112233-4455-6677-8899-aabbccddeeff","Name":"Ada","Number":42,"Date":"2026-01-02","Time":"03:04:05.1234567","Duration":"1.02:03:04.1234567","Optional":"00112233-4455-6677-8899-aabbccddeeff","Names":["Grace"],"Numbers":{"n":7}}`
	if string(data) != want {
		t.Fatalf("got %s\nwant %s", data, want)
	}
	var decoded sharedValues
	if err := json.Unmarshal(data, &decoded); err != nil || !reflect.DeepEqual(decoded, value) {
		t.Fatalf("roundtrip: %+v, %v", decoded, err)
	}
	value.Optional, value.Names, value.Numbers = nil, nil, nil
	data, err = event.Descriptor().Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]json.RawMessage
	if err := json.Unmarshal(data, &content); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Optional", "Names", "Numbers", "Zero"} {
		if _, exists := content[name]; exists {
			t.Fatalf("nil/omitzero property %s present", name)
		}
	}
}

type invalidConcept string

func (invalidConcept) ConceptValue() string { panic("discovery must not execute") }

type invalidConceptEvent struct{ Value invalidConcept }

func TestInvalidConceptRegistrationPreservesTypedError(t *testing.T) {
	_, err := chronicle.RegisterEvent[invalidConceptEvent](chronicle.NewRegistry())
	var detail *concepts.TypeError
	if !errors.Is(err, chronicle.ErrInvalidConfiguration) || !errors.Is(err, concepts.ErrInvalidConcept) || !errors.As(err, &detail) || detail.Reason != concepts.ReasonMissingCodec {
		t.Fatalf("invalid declaration: %v (%+v)", err, detail)
	}
}

// Valid metadata, intentionally dishonest codecs. CheckJSON must inspect actual bytes.
type badConcept string

func (badConcept) ConceptValue() string           { panic("must not execute") }
func (v badConcept) MarshalJSON() ([]byte, error) { return []byte(v), nil }
func (*badConcept) UnmarshalJSON([]byte) error    { return nil }
func (v badConcept) MarshalText() ([]byte, error) { return []byte(v), nil }
func (*badConcept) UnmarshalText([]byte) error    { return nil }

func TestConceptCodecsAreCheckedBeforeDispatch(t *testing.T) {
	type event struct{ Value badConcept }
	plan, err := serialization.Compile(reflect.TypeFor[event]())
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []badConcept{"null", "{}", "[]", "42", "true"} {
		if _, err := plan.Marshal(event{bad}); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("accepted %s: %v", bad, err)
		}
	}
}

type unsignedConceptValues struct {
	Value   conceptfixtures.Unsigned
	Pointer *conceptfixtures.Unsigned
	Slice   []conceptfixtures.Unsigned
	Array   [1]conceptfixtures.Unsigned
}

func TestUnsignedConceptFullRangeRoundTrips(t *testing.T) {
	for _, number := range []uint64{0, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		t.Run(strconv.FormatUint(number, 10), func(t *testing.T) {
			value := conceptfixtures.Unsigned{Value: number}
			want := unsignedConceptValues{Value: value, Pointer: &value, Slice: []conceptfixtures.Unsigned{value}, Array: [1]conceptfixtures.Unsigned{value}}
			assertUnsignedConceptRoundTrip(t, want)
		})
	}
}

func assertUnsignedConceptRoundTrip(t *testing.T, want unsignedConceptValues) {
	t.Helper()
	plan, err := serialization.Compile(reflect.TypeFor[unsignedConceptValues]())
	if err != nil {
		t.Fatal(err)
	}
	data, err := plan.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	number := strconv.FormatUint(want.Value.Value, 10)
	literal := `{"Value":` + number + `,"Pointer":` + number + `,"Slice":[` + number + `],"Array":[` + number + `]}`
	if string(data) != literal {
		t.Fatalf("concept integer JSON = %s, want %s", data, literal)
	}
	var got unsignedConceptValues
	if err := plan.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("concept round trip = %#v, want %#v", got, want)
	}
}

func TestUnsignedConceptSchemaHasNoSignedMaximum(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[unsignedConceptValues]())
	if err != nil {
		t.Fatal(err)
	}
	var schema struct{ Properties map[string]map[string]any }
	if err := json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	integer := map[string]any{"type": "integer", "format": "uint64", "minimum": float64(0)}
	if !reflect.DeepEqual(schema.Properties["Value"], integer) {
		t.Fatalf("concept schema = %s", plan.Schema())
	}
	for _, name := range []string{"Slice", "Array"} {
		if !reflect.DeepEqual(schema.Properties[name]["items"], integer) {
			t.Fatalf("concept collection schema = %s", plan.Schema())
		}
	}
	integer["format"] = "uint64?"
	if !reflect.DeepEqual(schema.Properties["Pointer"], integer) {
		t.Fatalf("nullable concept schema = %s", plan.Schema())
	}
}

func TestConceptIntegerRangesUseWireRepresentation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		value    any
		rejected bool
	}{
		{"uint boundary", struct{ Value conceptfixtures.Unsigned }{conceptfixtures.Unsigned{Value: 1<<63 - 1}}, false},
		{"uint above signed range", struct{ Value conceptfixtures.Unsigned }{conceptfixtures.Unsigned{Value: 1 << 63}}, false},
		{"slice above signed range", struct{ Values []conceptfixtures.Unsigned }{[]conceptfixtures.Unsigned{{Value: 1 << 63}}}, false},
		{"dictionary maximum", struct {
			Values map[string]conceptfixtures.Unsigned
		}{map[string]conceptfixtures.Unsigned{"n": {Value: math.MaxUint64}}}, true},
		{"dictionary array", struct {
			Values map[string][1]conceptfixtures.Unsigned
		}{map[string][1]conceptfixtures.Unsigned{"n": {{Value: 1<<53 + 1}}}}, true},
		{"dictionary positive", struct {
			Values map[string]conceptfixtures.Number
		}{map[string]conceptfixtures.Number{"n": 1<<53 + 1}}, true},
		{"dictionary negative", struct {
			Values map[string]conceptfixtures.Number
		}{map[string]conceptfixtures.Number{"n": -(1 << 53) - 1}}, true},
		{"dictionary boundary", struct {
			Values map[string]conceptfixtures.Number
		}{map[string]conceptfixtures.Number{"n": -(1 << 53)}}, false},
		{"nested dictionary", struct {
			Values map[string][]*conceptfixtures.Unsigned
		}{map[string][]*conceptfixtures.Unsigned{"n": {{Value: 1<<53 + 1}}}}, true},
		{"ordinary wide", struct{ Value conceptfixtures.Number }{1<<53 + 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := serialization.Compile(reflect.TypeOf(tc.value))
			if err != nil {
				t.Fatal(err)
			}
			_, err = plan.Marshal(tc.value)
			if tc.rejected {
				if !errors.Is(err, chronicle.ErrUnsupported) {
					t.Fatalf("error=%v, want ErrUnsupported", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
