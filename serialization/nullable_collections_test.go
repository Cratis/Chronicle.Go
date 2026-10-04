// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

type nullableCollections struct {
	Labels     *[]events.Tag      `json:"labels"`
	Attributes *map[string]string `json:"attributes"`
}

func TestNullableCollectionsExistingPresenceContract(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[nullableCollections]())
	if err != nil {
		t.Fatal(err)
	}
	var nilSlice []events.Tag
	var nilMap map[string]string
	emptySlice, emptyMap := []events.Tag{}, map[string]string{}
	for _, tc := range []struct {
		name  string
		value nullableCollections
		want  string
	}{
		{"nil outer pointers", nullableCollections{}, `{}`},
		{"pointers to nil collections", nullableCollections{&nilSlice, &nilMap}, `{}`},
		{"empty collections", nullableCollections{&emptySlice, &emptyMap}, `{"attributes":{},"labels":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := plan.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			var got, want map[string]json.RawMessage
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %s want %s", data, tc.want)
			}
		})
	}
	for _, data := range []string{`{}`, `{"labels":null,"attributes":null}`, `{"labels":[],"attributes":{}}`} {
		t.Run(data, func(t *testing.T) {
			var raw map[string]json.RawMessage
			if err := json.Unmarshal([]byte(data), &raw); err != nil {
				t.Fatal(err)
			}
			var got nullableCollections
			if err := plan.Unmarshal([]byte(data), &got); err != nil {
				t.Fatal(err)
			}
			empty := data == `{"labels":[],"attributes":{}}`
			if (got.Labels != nil) != empty || (got.Attributes != nil) != empty {
				t.Fatalf("decoded presence = %+v", got)
			}
			if empty && (len(*got.Labels) != 0 || len(*got.Attributes) != 0) {
				t.Fatal("empty did not decode empty")
			}
			if data == `{"labels":null,"attributes":null}` && (string(raw["labels"]) != "null" || string(raw["attributes"]) != "null") {
				t.Fatal("explicit raw null lost")
			}
			if data == `{}` && len(raw) != 0 {
				t.Fatal("missing properties became present")
			}
		})
	}
}
