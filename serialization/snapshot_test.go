// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/declarations"
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

type snapshotEmbedded struct{ Name string }
type snapshotShadow struct {
	snapshotEmbedded
	Other string `json:"name"`
}
type snapshotIdentity struct {
	*snapshotEmbedded
	Other  string `json:"name"`
	Nested *snapshotIdentity
}

func TestSnapshotRebindRejectsFieldsLostToPromotion(t *testing.T) {
	before, err := serialization.CompileReadModel(reflect.TypeFor[snapshotShadow]())
	if err != nil {
		t.Fatal(err)
	}
	after, err := serialization.CompileReadModel(reflect.TypeFor[snapshotShadow](), serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{`{"Name":"embedded"}`, `{"Name":"embedded","name":"other"}`} {
		_, err := before.RebindJSON([]byte(data), after)
		var configuration *declarations.DeclarationError
		if !errors.Is(err, chronicle.ErrInvalidConfiguration) || !errors.As(err, &configuration) || configuration.GoField != "snapshotEmbedded.Name" {
			t.Fatalf("snapshot=%s error=%v", data, err)
		}
	}
	bound, err := before.RebindJSON([]byte(`{"name":"other"}`), after)
	if err != nil || string(bound) != `{"name":"other"}` {
		t.Fatalf("surviving field rebound to another field: %s %v", bound, err)
	}
}

func TestSnapshotRebindPreservesEmbeddedPointerAndRecursiveFieldIdentity(t *testing.T) {
	before, err := serialization.CompileReadModel(reflect.TypeFor[snapshotIdentity]())
	if err != nil {
		t.Fatal(err)
	}
	after, err := serialization.CompileReadModel(reflect.TypeFor[snapshotIdentity](), serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	// The promoted pointer field disappears at every recursive level, but
	// Other retains its own Go index even though its traversal position changes.
	bound, err := before.RebindJSON([]byte(`{"name":"root","Nested":{"name":"child","Nested":null}}`), after)
	want := `{"name":"root","nested":{"name":"child","nested":null}}`
	if err != nil || string(bound) != want {
		t.Fatalf("snapshot=%s want=%s error=%v", bound, want, err)
	}
	if _, err := before.RebindJSON([]byte(`{"Nested":{"Name":"lost"}}`), after); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("lost nested identity accepted: %v", err)
	}
	// In the reverse direction promotion adds an earlier field. A null for
	// Nested still belongs to Nested, not Other or the newly promoted Name.
	bound, err = after.RebindJSON([]byte(`{"name":"other","nested":null}`), before)
	if err != nil || string(bound) != `{"Nested":null,"name":"other"}` {
		t.Fatalf("reverse snapshot=%s error=%v", bound, err)
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
