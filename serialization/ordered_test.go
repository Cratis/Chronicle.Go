// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func golden(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSuffix(data, []byte{'\n'})
}

func TestDeclarationOrderMatchesCSharpGolden(t *testing.T) {
	type person struct {
		Surname string
		Age     int
	}
	type event struct {
		Name    string
		Age     int
		Nested  person
		Pointer *person
		Alias   conceptfixtures.Name
		Nil     *string
		NilMap  map[string]string
		NilList []string
		Omitted string `json:",omitempty"`
		Zeroed  int    `json:",omitzero"`
		Zero    int
		False   bool
		Empty   []string
		Items   []person
		When    time.Time
	}
	// Hand-derived equivalent C# declarations; null properties are omitted by
	// Chronicle, and JsonIgnore(WhenWritingDefault) corresponds to the two tags.
	value := event{Name: "Ada", Age: 37, Nested: person{"Lovelace", 36}, Pointer: &person{"Hopper", 85}, Alias: "Countess", Empty: []string{}, Items: []person{{"Byron", 36}}, When: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	plan, err := serialization.Compile(reflect.TypeFor[event]())
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []any{value, &value} {
		got, err := plan.Marshal(input)
		if err != nil || !bytes.Equal(got, golden(t, "csharp-ordered-event")) {
			t.Fatalf("got %s (%v), want %s", got, err, golden(t, "csharp-ordered-event"))
		}
	}
}

func TestDefaultEscapingMatchesCSharpGolden(t *testing.T) {
	// Captured from .NET 10 System.Text.Json: Serialize(SerializeToNode(value,
	// new JsonSerializerOptions { DefaultIgnoreCondition = WhenWritingNull,
	// WriteIndented = false, PropertyNameCaseInsensitive = true })). These are
	// ChronicleClient/EventSerializer/EventSeeding's default options and stages.
	value := struct {
		Name string
		Age  int
		Text string
	}{"Ada", 37, "é葛😀<>&'+`\"\\/\b\t\n\f\r\x00\x1f\x7f\u2028\u2029"}
	plan, err := serialization.Compile(reflect.TypeOf(value))
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.Marshal(value)
	if err != nil || !bytes.Equal(got, golden(t, "csharp-escaped-event")) {
		t.Fatalf("got %s (%v), want %s", got, err, golden(t, "csharp-escaped-event"))
	}
	var decoded any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatal(err)
	}
}

func TestEscapingAppliesToNamesConceptCodecsAndMapKeys(t *testing.T) {
	value := struct {
		Z string `json:"é"`
		A encodedConcept[string]
		M map[string]string
	}{"é<>&'+", encodedConcept[string]{`"\u00e9\u003c\u003e\u0026'+\""`}, map[string]string{"z": "last", "é": "non-ASCII", "a+": "first"}}
	plan, err := serialization.Compile(reflect.TypeOf(value))
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.Marshal(value)
	want := `{"\u00E9":"\u00E9\u003C\u003E\u0026\u0027\u002B","A":"\u00E9\u003C\u003E\u0026\u0027\u002B\u0022","M":{"a\u002B":"first","z":"last","\u00E9":"non-ASCII"}}`
	if err != nil || string(got) != want {
		t.Fatalf("got %s (%v), want %s", got, err, want)
	}
}

func TestDefaultEscapingAllASCII(t *testing.T) {
	var text string
	for i := range 128 {
		text += string(rune(i))
	}
	value := struct{ Text string }{text}
	plan, err := serialization.Compile(reflect.TypeOf(value))
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.Marshal(value)
	// Captured from the same .NET probe, covering the entire default ASCII block.
	want := "{\"Text\":" + `"\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000B\f\r\u000E\u000F\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F !\u0022#$%\u0026\u0027()*\u002B,-./0123456789:;\u003C=\u003E?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_\u0060abcdefghijklmnopqrstuvwxyz{|}~\u007F"` + "}"
	if err != nil || string(got) != want {
		t.Fatalf("got %s (%v), want %s", got, err, want)
	}
}

type OrderedEmbedded struct {
	Zebra string
	Age   int
}
type OrderedPointer struct {
	Year  int
	Alias string
}

func TestEmbeddedFieldsFollowEncodingJSONOrder(t *testing.T) {
	type event struct {
		Name string
		OrderedEmbedded
		Between bool
		*OrderedPointer
		Tail string
	}
	plan, err := serialization.Compile(reflect.TypeFor[event]())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []event{
		{Name: "Ada", OrderedEmbedded: OrderedEmbedded{"z", 37}, Tail: "end"},
		{Name: "Ada", OrderedEmbedded: OrderedEmbedded{"z", 37}, OrderedPointer: &OrderedPointer{1843, "Countess"}, Tail: "end"},
	} {
		want, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := plan.Marshal(value)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("got %s (%v), want %s", got, err, want)
		}
		var decoded event
		if err := plan.Unmarshal(got, &decoded); err != nil || !reflect.DeepEqual(value, decoded) {
			t.Fatalf("decode = %+v, %v", decoded, err)
		}
	}
	field, ok := serialization.FieldAt(plan.Fields(), "Alias")
	if !ok || !reflect.DeepEqual(field.Index, []int{3, 1}) || field.GoField != "OrderedPointer.Alias" {
		t.Fatalf("promoted field metadata = %+v", field)
	}
	var schema struct{ Required []string }
	if err := json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schema.Required, []string{"Name", "Zebra", "Age", "Between", "Tail"}) {
		t.Fatal("optional promoted fields marked required", schema.Required)
	}
}

func TestEmbeddedShadowingTagsAndRecursivePromotion(t *testing.T) {
	type other struct {
		Zebra string
		Age   int `json:"Age"`
	}
	type recursive struct {
		Name string
		*recursive
		Age int
	}
	for _, value := range []any{
		struct {
			OrderedEmbedded
			Age int
		}{OrderedEmbedded{"z", 1}, 2},
		struct {
			OrderedEmbedded
			other
		}{OrderedEmbedded{"z", 1}, other{"ignored", 2}},
		struct {
			OrderedEmbedded `json:"nested"`
			Age             int
		}{OrderedEmbedded{"z", 1}, 2},
		recursive{Name: "outer", recursive: &recursive{Name: "ignored"}, Age: 37},
	} {
		plan, err := serialization.Compile(reflect.TypeOf(value))
		if err != nil {
			t.Fatal(err)
		}
		want, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := plan.Marshal(value)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("got %s (%v), want %s", got, err, want)
		}
	}
}

func TestOrderedEmbeddedFieldsHonorNamingAndOmission(t *testing.T) {
	type named struct {
		URLValue string
		Gone     string `json:",omitempty"`
		Zero     int    `json:",omitzero"`
		Person   string
	}
	type event struct {
		Z bool
		named
		A int `json:"fixed"`
	}
	value := event{named: named{URLValue: "/", Person: "Ada"}}
	for _, tc := range []struct {
		policy serialization.NamingPolicy
		want   string
	}{
		{serialization.PreservePropertyNames, `{"Z":false,"URLValue":"/","Person":"Ada","fixed":0}`},
		{serialization.CamelCase, `{"z":false,"URLValue":"/","person":"Ada","fixed":0}`},
		{serialization.LegacyGoCamelCase, `{"z":false,"urlValue":"/","person":"Ada","fixed":0}`},
	} {
		plan, err := serialization.Compile(reflect.TypeFor[event](), tc.policy)
		if err != nil {
			t.Fatal(err)
		}
		got, err := plan.Marshal(value)
		if err != nil || string(got) != tc.want {
			t.Fatalf("got %s (%v), want %s", got, err, tc.want)
		}
	}
}

func TestEmbeddedDeclarationsCannotBeSilentlyIgnored(t *testing.T) {
	type declared struct {
		Name string `chronicle:"unique"`
	}
	for _, typ := range []reflect.Type{
		reflect.TypeFor[struct {
			declared
			Name string
		}](),
		reflect.TypeFor[struct {
			OrderedEmbedded `chronicle:"unique"`
		}](),
	} {
		if _, err := serialization.Compile(typ); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("ignored declaration accepted: %v", err)
		}
	}
}

func TestDictionaryValuesRetainStructDeclarationOrder(t *testing.T) {
	value := struct{ Values map[string]OrderedEmbedded }{map[string]OrderedEmbedded{"z": {"Z", 1}, "a": {"A", 2}}}
	plan, err := serialization.Compile(reflect.TypeOf(value))
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.Marshal(value)
	want := `{"Values":{"a":{"Zebra":"A","Age":2},"z":{"Zebra":"Z","Age":1}}}`
	if err != nil || string(got) != want {
		t.Fatalf("got %s (%v), want %s", got, err, want)
	}
}

func TestOrderedObjectsPreserveValidation(t *testing.T) {
	type promoted struct{ Value uint64 }
	for _, value := range []any{
		struct{ promoted }{promoted{1 << 63}},
		struct{ Values map[string]promoted }{map[string]promoted{"a": {1<<53 + 1}}},
	} {
		plan, err := serialization.Compile(reflect.TypeOf(value))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := plan.Marshal(value); !errors.Is(err, faults.ErrUnsupported) {
			t.Fatalf("range accepted: %v", err)
		}
	}
	value := struct{ Nested struct{ Value float64 } }{}
	value.Nested.Value = math.NaN()
	plan, err := serialization.Compile(reflect.TypeOf(value))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.Marshal(value); err == nil {
		t.Fatal("NaN accepted")
	}
}
