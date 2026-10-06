// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type binaryDottedAliasFirst struct {
	Alias string `json:"Data.Payload"`
	Data  struct{ Payload []byte }
}
type binaryDottedAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"Data.Payload"`
}
type binaryReboundAliasFirst struct {
	Alias string `json:"data.payload"`
	Data  struct{ Payload []byte }
}
type binaryReboundAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"data.payload"`
}
type binaryDeepAlias struct {
	Alias string `json:"Next.Next.Data.Payload"`
	Data  struct{ Payload []byte }
	Next  *binaryDeepAlias
}

func TestBinaryDottedAmbiguityRefusesPlan(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"alias first":       reflect.TypeFor[binaryDottedAliasFirst](),
		"alias last":        reflect.TypeFor[binaryDottedAliasLast](),
		"recursive segment": reflect.TypeFor[binaryDeepAlias](),
		"binary alias": reflect.TypeFor[struct {
			Alias []byte `json:"Data.Payload"`
			Data  struct{ Payload string }
		}](),
		"binary owner": reflect.TypeFor[struct {
			Alias string `json:"Data.Payload"`
			Data  struct{ Payload struct{ Bytes []byte } }
		}](),
		"nested alias": reflect.TypeFor[struct{ Object binaryDottedAliasFirst }](),
		"dotted intermediate": reflect.TypeFor[struct {
			Alias string `json:"Data.Payload.Bytes"`
			Data  struct {
				Payload []byte `json:"Payload.Bytes"`
			}
		}](),
	} {
		t.Run(name, func(t *testing.T) {
			for _, compile := range []func(reflect.Type) (*serialization.Plan, error){
				func(typ reflect.Type) (*serialization.Plan, error) { return serialization.Compile(typ) },
				func(typ reflect.Type) (*serialization.Plan, error) { return serialization.CompileReadModel(typ) },
			} {
				plan, err := compile(typ)
				if !errors.Is(err, faults.ErrUnsupported) || plan != nil {
					t.Fatalf("ambiguous plan = %v, error = %v, want ErrUnsupported", plan, err)
				}
			}
		})
	}
}

func TestBinaryDottedAmbiguityRefusesNamingPlan(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"alias first": reflect.TypeFor[binaryReboundAliasFirst](),
		"alias last":  reflect.TypeFor[binaryReboundAliasLast](),
	} {
		t.Run(name, func(t *testing.T) {
			plan, err := serialization.Compile(typ)
			if err != nil {
				t.Fatal(err)
			}
			for _, naming := range []serialization.NamingPolicy{serialization.CamelCase, serialization.LegacyGoCamelCase} {
				next, err := plan.WithNamingPolicy(naming)
				if !errors.Is(err, faults.ErrUnsupported) || next != nil {
					t.Fatalf("rebound plan = %v, error = %v, want ErrUnsupported", next, err)
				}
			}
		})
	}
}

func TestUnambiguousDottedBinaryPayloadKeepsCodec(t *testing.T) {
	value := struct {
		Payload []byte `json:"data.payload"`
	}{Payload: []byte{1}}
	plan, err := serialization.Compile(reflect.TypeOf(value))
	if err != nil {
		t.Fatal(err)
	}
	next, err := plan.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	data, err := next.Marshal(value)
	if err != nil || string(data) != `{"data.payload":"AQ=="}` {
		t.Fatalf("unambiguous payload = %s, %v", data, err)
	}
}

func TestNonBinaryDottedCollisionKeepsFirstMatchAndSnapshot(t *testing.T) {
	for _, aliasFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "alias first", false: "alias last"}[aliasFirst], func(t *testing.T) {
			typ := reflect.TypeFor[struct {
				Alias  string `json:"data.payload"`
				Data   struct{ Payload string }
				Binary []byte
			}]()
			if !aliasFirst {
				typ = reflect.TypeFor[struct {
					Data   struct{ Payload string }
					Alias  string `json:"data.payload"`
					Binary []byte
				}]()
			}
			before, err := serialization.Compile(typ)
			if err != nil {
				t.Fatal(err)
			}
			after, err := before.WithNamingPolicy(serialization.CamelCase)
			if err != nil {
				t.Fatal(err)
			}
			field, ok := serialization.FieldAt(after.Fields(), "data.payload")
			want := "Alias"
			if !aliasFirst {
				want = "Data.Payload"
			}
			if !ok || field.GoField != want {
				t.Fatalf("first match = %s, want %s", field.GoField, want)
			}
			guarded, ok := serialization.FieldAtWithCapability(after.Fields(), "data.payload", serialization.Field.ContainsBinary)
			if !ok || guarded.GoField != want {
				t.Fatalf("guard changed ordinary first match: %+v", guarded)
			}
			data, err := before.RebindJSON([]byte(`{"data.payload":"alias","Data":{"Payload":"nested"}}`), after)
			if err != nil || string(data) != `{"data":{"payload":"nested"},"data.payload":"alias"}` {
				t.Fatalf("snapshot = %s, %v", data, err)
			}
		})
	}
}
