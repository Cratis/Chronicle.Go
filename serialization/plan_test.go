// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type Amount uint64
type fixture struct {
	URLValue string
	ID       uuid.UUID `json:"id"`
	Amount   Amount
	Enabled  bool
	Null     *string
	Skipped  string `json:"-"`
	Empty    string `json:"empty,omitempty"`
	Optional int    `json:"optional,omitzero"`
	When     time.Time
	Nested   struct{ FullName string }
	Values   []int
}

func TestSchemaAndSerializationShareNames(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[fixture]())
	if err != nil {
		t.Fatal(err)
	}
	value := fixture{URLValue: "https://example.test", ID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), Amount: 9223372036854775807, Values: []int{}, When: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	value.Nested.FullName = "Ada"
	data, err := plan.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"URLValue":"https://example.test","id":"00112233-4455-6677-8899-aabbccddeeff","Amount":9223372036854775807,"Enabled":false,"When":"2026-01-02T03:04:05Z","Nested":{"FullName":"Ada"},"Values":[]}`
	if string(data) != want {
		t.Fatalf("JSON = %s\nwant = %s", data, want)
	}
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if err = json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	if len(schema.Properties) != 10 {
		t.Fatalf("schema properties = %v", schema.Properties)
	}
	for _, name := range []string{"URLValue", "id", "Amount", "Enabled", "Null", "empty", "optional", "When", "Nested", "Values"} {
		if _, ok := schema.Properties[name]; !ok {
			t.Fatalf("missing %s", name)
		}
	}
	if _, err := plan.Marshal((*fixture)(nil)); err == nil {
		t.Fatal("nil event accepted")
	}
}

type protected struct {
	Email string `chronicle:"pii(unknown=true)"`
}
type custom string

func (custom) MarshalJSON() ([]byte, error) { return []byte(`"custom"`), nil }

func TestUnsupportedShapesFailBeforeRegistration(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[protected](), reflect.TypeFor[struct{ Value any }](), reflect.TypeFor[struct{ Value custom }](), reflect.TypeFor[struct{ Value map[int]string }](), reflect.TypeFor[struct {
		Value string `json:",string"`
	}](), reflect.StructOf([]reflect.StructField{
		{Name: "First", Type: reflect.TypeFor[string](), Tag: `json:"x"`},
		{Name: "Second", Type: reflect.TypeFor[string](), Tag: `json:"x"`},
	})} {
		t.Run(typ.String(), func(t *testing.T) {
			if _, err := serialization.Compile(typ); err == nil {
				t.Fatal("unsupported schema accepted")
			}
		})
	}
}
