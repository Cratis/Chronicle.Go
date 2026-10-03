// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

type snapshotChild struct{ City string }
type snapshotModel struct {
	Name     string
	Note     *string
	Children []snapshotChild
	Lookup   map[string]snapshotChild
}

func TestSnapshotRebindPreservesPresenceNumbersAndDefaultEscaping(t *testing.T) {
	before, err := serialization.CompileReadModel(reflect.TypeFor[snapshotModel]())
	if err != nil {
		t.Fatal(err)
	}
	after, err := serialization.CompileReadModel(reflect.TypeFor[snapshotModel](), serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	data, err := before.Marshal(snapshotModel{Name: "+Oslo'", Children: []snapshotChild{}, Lookup: map[string]snapshotChild{"City": {City: "Bergen"}}})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := before.RebindJSON(data, after)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"children":[],"lookup":{"City":{"city":"Bergen"}},"name":"\u002BOslo\u0027"}`
	if string(bound) != want {
		t.Fatalf("snapshot=%s want=%s", bound, want)
	}
	field, ok := serialization.FieldAt(before.Fields(), "Note")
	if !ok {
		t.Fatal("missing field")
	}
	null, err := field.Marshal((*string)(nil))
	if err != nil || string(null) != "null" {
		t.Fatalf("scalar null=%s error=%v", null, err)
	}
	bound, err = before.RebindJSON([]byte(`{"Note":null}`), after)
	if err != nil || string(bound) != `{"note":null}` {
		t.Fatalf("null snapshot=%s error=%v", bound, err)
	}
}

func TestSnapshotRebindRejectsUnknownOrInvalidObjectShapes(t *testing.T) {
	plan, err := serialization.CompileReadModel(reflect.TypeFor[snapshotModel]())
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{`[]`, `42`, `{"unknown":0}`, `{"Children":[42]}`, `{"Lookup":{"item":[]}}`} {
		t.Run(data, func(t *testing.T) {
			if _, err := plan.RebindJSON([]byte(data), plan); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}
