// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type BinaryEmbedded struct {
	Payload []byte
	Count   int
}
type binaryEmbedded struct {
	Payload []byte
	Count   int
}
type binaryExportedOwner struct{ *BinaryEmbedded }
type binaryUnexportedOwner struct{ *binaryEmbedded }

func TestBinaryMissingFieldsPreserveAbsentEmbeddedPointers(t *testing.T) {
	for name, compile := range map[string]func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error){"event": serialization.Compile, "model": serialization.CompileReadModel} {
		t.Run(name, func(t *testing.T) {
			p, err := compile(reflect.TypeFor[binaryExportedOwner]())
			if err != nil {
				t.Fatal(err)
			}
			var got binaryExportedOwner
			if err := p.Unmarshal([]byte(`{}`), &got); err != nil {
				t.Fatal(err)
			}
			if got.BinaryEmbedded != nil {
				t.Fatal("missing binary allocated embedded pointer")
			}
			data, err := p.Marshal(got)
			if err != nil || string(data) != `{}` {
				t.Fatalf("empty object changed: %s %v", data, err)
			}
			if err := p.Unmarshal([]byte(`{"Count":1}`), &got); err != nil {
				t.Fatal(err)
			}
			if got.BinaryEmbedded == nil || got.Payload == nil || len(got.Payload) != 0 || got.Count != 1 {
				t.Fatalf("existing container's binary not normalized: %#v", got.BinaryEmbedded)
			}
			p, err = compile(reflect.TypeFor[binaryUnexportedOwner]())
			if err != nil {
				t.Fatal(err)
			}
			var private binaryUnexportedOwner
			if err := p.Unmarshal([]byte(`{}`), &private); err != nil {
				t.Fatal("absent unexported embedded pointer failed", err)
			}
			if private.binaryEmbedded != nil {
				t.Fatal("missing binary allocated private embedded pointer")
			}
		})
	}
}

func TestBinaryClassificationDoesNotInheritPrimitiveCapabilities(t *testing.T) {
	for _, scalar := range []serialization.Scalar{serialization.NotScalar, serialization.Binary, serialization.Scalar(255)} {
		if scalar.IsPrimitive() {
			t.Fatalf("unqualified scalar %d inherited primitive capabilities", scalar)
		}
	}
	for _, scalar := range []serialization.Scalar{serialization.String, serialization.Boolean, serialization.Integer, serialization.Number} {
		if !scalar.IsPrimitive() {
			t.Fatalf("primitive scalar %d lost capabilities", scalar)
		}
	}
	p := binaryPlan(t, serialization.PreservePropertyNames)
	for _, name := range []string{"Payload", "Optional", "Nested.Inner"} {
		field, ok := serialization.FieldAt(p.Fields(), name)
		if !ok || field.Scalar == serialization.String {
			t.Fatalf("binary field inherits String capabilities: %#v", field)
		}
	}
}

func TestBinaryCapabilityMetadataIncludesRecursiveOwners(t *testing.T) {
	type tree struct {
		Payload []byte
		Count   int
		Next    *tree
	}
	p, err := serialization.Compile(reflect.TypeFor[tree]())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"Payload", "Next", "Next.Next", "Next.Payload"} {
		f, ok := serialization.FieldAt(p.Fields(), path)
		if !ok || !f.ContainsBinary() {
			t.Fatal("binary capability lost through owner/reference", path)
		}
	}
	f, ok := serialization.FieldAt(p.Fields(), "Count")
	if !ok || f.ContainsBinary() {
		t.Fatal("ordinary field acquired binary capability")
	}
	var value tree
	if err := p.Unmarshal([]byte(`{"Next":{"Count":1}}`), &value); err != nil {
		t.Fatal(err)
	}
	if value.Next == nil || value.Payload == nil || value.Next.Payload == nil || value.Next.Next != nil {
		t.Fatal("binary normalization changed recursive container presence")
	}
}

func TestScalarArrayFieldsHaveNoElement(t *testing.T) {
	type document struct {
		ID       uuid.UUID
		Optional *uuid.UUID
		Fixed    [2]byte
		Values   []string
	}
	p, err := serialization.Compile(reflect.TypeFor[document]())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ID", "Optional"} {
		f, ok := serialization.FieldAt(p.Fields(), name)
		if !ok {
			t.Fatal("missing field", name)
		}
		if _, ok := f.Element(); ok {
			t.Fatalf("scalar array %s exposed an element", name)
		}
	}
	for _, name := range []string{"Fixed", "Values"} {
		f, ok := serialization.FieldAt(p.Fields(), name)
		if !ok {
			t.Fatal("missing field", name)
		}
		if _, ok := f.Element(); !ok {
			t.Fatalf("collection %s lost its element", name)
		}
	}
}

// A large binary-free item graph must not add per-element graph-walk allocations.
// Both documents decode exactly the same present property; the extra types are
// absent, so normal JSON/reflection allocations are identical.
func TestBinaryFreeArrayDecodeAllocationsIgnoreAbsentItemGraph(t *testing.T) {
	type small struct {
		Count  int
		Unused int
	}
	type a int
	type b int
	type c int
	type d int
	type e int
	type f int
	type g int
	type h int
	type i int
	type j int
	type k int
	type l int
	type large struct {
		Count  int
		Unused struct {
			A a
			B b
			C c
			D d
			E e
			F f
			G g
			H h
			I i
			J j
			K k
			L l
		}
	}
	type smallDoc struct{ Rows []small }
	type largeDoc struct{ Rows []large }
	input := []byte(`{"Rows":[` + strings.TrimSuffix(strings.Repeat(`{"Count":1},`, 32), ",") + `]}`)
	smallPlan, err := serialization.Compile(reflect.TypeFor[smallDoc]())
	if err != nil {
		t.Fatal(err)
	}
	largePlan, err := serialization.Compile(reflect.TypeFor[largeDoc]())
	if err != nil {
		t.Fatal(err)
	}
	var smallValue smallDoc
	var largeValue largeDoc
	measure := func(plan *serialization.Plan, target any) float64 {
		return testing.AllocsPerRun(20, func() {
			if err := plan.Unmarshal(input, target); err != nil {
				panic(fmt.Sprintf("decode: %v", err))
			}
		})
	}
	smallAllocs, largeAllocs := measure(smallPlan, &smallValue), measure(largePlan, &largeValue)
	if len(smallValue.Rows) != 32 || len(largeValue.Rows) != 32 {
		t.Fatal("allocation test decoded no elements")
	}
	t.Logf("small graph %.0f allocations; large graph %.0f allocations for 32 elements", smallAllocs, largeAllocs)
	if largeAllocs > smallAllocs+1 {
		t.Fatalf("binary-free item graph adds per-element allocations: small=%.0f large=%.0f", smallAllocs, largeAllocs)
	}
}
