// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

type binaryRecursivePath struct {
	Alias string `json:"Data.Payload"`
	Data  struct{ Payload []byte }
	Next  *binaryRecursivePath
}

func TestBinaryPathCapabilityIncludesAmbiguousRecursiveCandidates(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[binaryRecursivePath]())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"Data.Payload", "Next.Data.Payload", "Next.Next.Data.Payload"} {
		field, ok := serialization.FieldAtWithCapability(plan.Fields(), path, serialization.Field.ContainsBinary)
		if !ok || !field.ContainsBinary() || field.Path != path || field.Collection {
			t.Fatalf("binary candidate hidden at %s: %+v, %v", path, field, ok)
		}
	}
	if roots := serialization.EmittedRootFields(plan.Fields()); len(roots) != 3 || roots[0].Name != "Data.Payload" {
		t.Fatalf("root ownership confused with dotted names: %+v", roots)
	}
}
