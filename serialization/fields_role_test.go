// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/serialization"
)

type reservedRoleVariant struct {
	Subject string `json:"__SuBjEcTs" chronicle:"index"`
}

func TestDerivativeModelRoleRejectsReservedPropertyBeforeUnsupportedDirective(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[any, reservedRoleVariant]("reserved"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := serialization.CompileWith(reflect.TypeFor[derivedEnvelope](), serialization.Config{Codecs: codecs})
	if err != nil {
		t.Fatal(err)
	}
	err = plan.ValidateRole(declarations.Model)
	var detail *serialization.CodecError
	if !errors.As(err, &detail) {
		t.Fatalf("expected codec error, got %v", err)
	}
	if detail.Message != "reserved compliance property" || detail.Field != "Subject" {
		t.Fatalf("unexpected first failure: %+v", detail)
	}
}

func TestRecursiveCollectionFieldResolutionPreservesQualification(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[recursiveTree]())
	if err != nil {
		t.Fatal(err)
	}
	field, ok := serialization.FieldAt(plan.Fields(), "children.children.name")
	if !ok {
		t.Fatal("recursive collection field not found")
	}
	if field.Path != "children.children.name" || field.GoField != "Children.Children.Name" || !field.Collection {
		t.Fatalf("unexpected qualification: %+v", field)
	}
	if !reflect.DeepEqual(field.Index, []int{1, 1, 0}) {
		t.Fatalf("indices = %v", field.Index)
	}
}
