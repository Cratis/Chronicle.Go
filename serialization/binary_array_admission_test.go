// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/serialization"
)

type namedBinaryArray [][]byte
type binaryArrayDeclaration struct{ Chunks [][]byte }

func TestBinaryArrayArtifactRegistrationRefusesBeforeClient(t *testing.T) {
	if _, err := chronicle.RegisterEvent[binaryArrayDeclaration](chronicle.NewRegistry()); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("binary event reached registration", err)
	}
	if _, err := chronicle.RegisterReadModel[binaryArrayDeclaration](chronicle.NewRegistry()); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal("binary model reached registration", err)
	}
}

func TestBinaryArraysFailBeforeRegistration(t *testing.T) {
	for _, value := range []any{
		struct{ Chunks [][]byte }{}, struct{ Chunks namedBinaryArray }{},
		struct{ Chunks []binaryNamed }{}, struct{ Nested struct{ Chunks [][]byte } }{},
	} {
		t.Run(reflect.TypeOf(value).String(), func(t *testing.T) {
			if _, err := serialization.Compile(reflect.TypeOf(value)); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("unsafe binary array event admitted: %v", err)
			}
			if _, err := serialization.CompileReadModel(reflect.TypeOf(value)); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("unsafe binary array model admitted: %v", err)
			}
		})
	}
}
