// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

type binaryNestedPath struct {
	Data struct{ Payload []byte }
	Next *struct {
		Data struct{ Payload []byte }
		Next *struct{ Data struct{ Payload []byte } }
	}
}

func TestBinaryPathCapabilityIncludesAmbiguousNestedCandidates(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[binaryNestedPath]())
	if err != nil {
		t.Fatal(err)
	}
	// Compiled plans refuse ambiguity; detached metadata still supports
	// capability-aware lookup as defense in depth for consumer guards.
	alias, err := serialization.Compile(reflect.TypeFor[struct {
		Alias string `json:"Data.Payload"`
	}]())
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"Data.Payload", "Next.Data.Payload", "Next.Next.Data.Payload"}
	var fields []serialization.Field
	for i, path := range paths {
		field := alias.Fields()[0]
		field.Name, field.Path, field.Index = path, path, []int{2 + i}
		fields = append(fields, field)
	}
	fields = append(fields, plan.Fields()...)
	for _, path := range paths {
		ordinary, ok := serialization.FieldAt(fields, path)
		if !ok || ordinary.ContainsBinary() {
			t.Fatalf("expected the ordinary alias to hide %s from first-match lookup", path)
		}
		field, ok := serialization.FieldAtWithCapability(fields, path, serialization.Field.ContainsBinary)
		if !ok || !field.ContainsBinary() || field.Path != path || field.Collection {
			t.Fatalf("binary candidate hidden at %s: %+v, %v", path, field, ok)
		}
	}
	if roots := serialization.EmittedRootFields(fields); len(roots) != 5 || roots[0].Name != "Data.Payload" {
		t.Fatalf("root ownership confused with dotted names: %+v", roots)
	}
}
