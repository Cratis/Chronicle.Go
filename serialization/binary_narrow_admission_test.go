// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func TestBinarySinkAndValueNamesRefusePlan(t *testing.T) {
	for _, name := range []string{"ID", "iD", "id", "Id", "_id", "_ID", "_member", "__lastHandledEventSequenceNumber", "__initialized", "__subject", "__subjects", "value", "Value", "VALUE"} {
		t.Run(name, func(t *testing.T) {
			object := reflect.StructOf([]reflect.StructField{{Name: "Payload", Type: reflect.TypeFor[[]byte](), Tag: reflect.StructTag(`json:"` + name + `"`)}})
			typ := reflect.StructOf([]reflect.StructField{{Name: "Attachment", Type: object}})
			for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
				for _, compile := range []func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error){serialization.Compile, serialization.CompileReadModel} {
					if plan, err := compile(typ, policy); plan != nil || !errors.Is(err, faults.ErrUnsupported) {
						t.Fatalf("unsafe nested name admitted: %v, %v", plan, err)
					}
				}
			}
		})
	}
	for _, name := range []string{"__lastHandledEventSequenceNumber", "__initialized", "__subject", "__subjects", "_member", "value"} {
		t.Run("root "+name, func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{{Name: "Payload", Type: reflect.TypeFor[[]byte](), Tag: reflect.StructTag(`json:"` + name + `"`)}})
			if plan, err := serialization.Compile(typ); plan != nil || !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("unsafe root name %s admitted: %v, %v", name, plan, err)
			}
		})
	}
	for _, reverse := range []bool{false, true} {
		fields := []reflect.StructField{
			{Name: "Identity", Type: reflect.TypeFor[string](), Tag: `json:"Id"`},
			{Name: "Payload", Type: reflect.TypeFor[[]byte](), Tag: `json:"_id"`},
		}
		if reverse {
			fields[0], fields[1] = fields[1], fields[0]
		}
		typ := reflect.StructOf([]reflect.StructField{{Name: "Attachment", Type: reflect.StructOf(fields)}})
		t.Run(map[bool]string{false: "Id/_id", true: "_id/Id"}[reverse], func(t *testing.T) {
			if _, err := serialization.Compile(typ); !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("nested Id/_id collision admitted: %v", err)
			}
		})
	}
}

func TestBinaryValueNamesRefuseAfterNaming(t *testing.T) {
	type document struct {
		Blob struct{ Value []byte }
	}
	// Case-insensitive refusal also covers the Preserve spelling before a
	// naming recompile can expose the kernel's exact single-"value" unwrap.
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		if _, err := serialization.Compile(reflect.TypeFor[document](), policy); !errors.Is(err, faults.ErrUnsupported) {
			t.Fatalf("value owner admitted under %v: %v", policy, err)
		}
	}
}

func TestBinaryReadModelOnlyAdmitsRootLeaves(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[struct{ Attachment struct{ Inner []byte } }](),
		reflect.TypeFor[struct{ Attachment *struct{ Inner *[]byte } }](),
	} {
		if _, err := serialization.Compile(typ); err != nil {
			t.Fatalf("event object payload refused: %v", err)
		}
		if plan, err := serialization.CompileReadModel(typ); plan != nil || !errors.Is(err, faults.ErrUnsupported) {
			t.Fatalf("unqualified nested read-model binary admitted: %v, %v", plan, err)
		}
	}
	type promoted struct{ Payload []byte }
	for _, typ := range []reflect.Type{
		reflect.TypeFor[struct {
			Payload  []byte
			Optional *[]byte
		}](),
		reflect.TypeFor[struct{ promoted }](),
	} {
		if _, err := serialization.CompileReadModel(typ); err != nil {
			t.Fatalf("emitted root binary leaf refused: %v", err)
		}
	}
}

func TestBinaryCompileErrorsPrecedeDeferredDuplicates(t *testing.T) {
	type invalidDeclaration struct {
		Hidden string `json:"-" chronicle:"index"`
	}
	for name, typ := range map[string]reflect.Type{
		"declaration": reflect.TypeFor[struct {
			Payload []byte
			First   string `json:"duplicate"`
			Second  string `json:"duplicate"`
			Invalid invalidDeclaration
		}](),
		"unsupported type": reflect.TypeFor[struct {
			Payload []byte
			First   string `json:"duplicate"`
			Second  string `json:"duplicate"`
			Invalid chan int
		}](),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := serialization.Compile(typ)
			if name == "declaration" {
				var declaration *declarations.DeclarationError
				if !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &declaration) {
					t.Fatalf("declaration error masked: %v", err)
				}
			} else if !errors.Is(err, faults.ErrUnsupported) || !strings.Contains(err.Error(), "unsupported JSON shape") {
				t.Fatalf("type error masked: %v", err)
			}
		})
	}
	for _, leaf := range []reflect.Type{reflect.TypeFor[string](), reflect.TypeFor[[]byte]()} {
		typ := reflect.StructOf([]reflect.StructField{
			{Name: "Payload", Type: leaf},
			{Name: "First", Type: reflect.TypeFor[string](), Tag: `json:"duplicate"`},
			{Name: "Second", Type: reflect.TypeFor[string](), Tag: `json:"duplicate"`},
		})
		if _, err := serialization.Compile(typ); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("exact duplicate changed class for %v: %v", leaf, err)
		}
	}
}

func TestBinaryFreeSinkAndValueNamesRemainAdmitted(t *testing.T) {
	type document struct {
		Blob struct {
			Value string `json:"value"`
			ID    string
			Id    string
			Mongo string `json:"_id"`
		}
		Watermark string `json:"__lastHandledEventSequenceNumber"`
	}
	for _, compile := range []func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error){serialization.Compile, serialization.CompileReadModel} {
		plan, err := compile(reflect.TypeFor[document]())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := plan.WithNamingPolicy(serialization.CamelCase); err != nil {
			t.Fatal(err)
		}
	}
}
