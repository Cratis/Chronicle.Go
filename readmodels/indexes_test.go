// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type IndexedAddress struct {
	City string `chronicle:"index"`
	Next *IndexedAddress
}
type IndexedModel struct {
	ID        string
	Email     string `json:"email" chronicle:"index"`
	Home      IndexedAddress
	Work      IndexedAddress
	Addresses []IndexedAddress
}

func TestIndexTagsShareExplicitMetadataAndKeepRepeatedNestedTypes(t *testing.T) {
	model, err := readmodels.Define[IndexedModel](readmodels.WithIndexes("Id"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Id", "email", "Home.City", "Work.City", "Addresses.City"}
	if !reflect.DeepEqual(model.Descriptor().Indexes(), want) {
		t.Fatalf("indexes=%v want=%v", model.Descriptor().Indexes(), want)
	}
	descriptor, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"id", "email", "home.city", "work.city", "addresses.city"}
	if !reflect.DeepEqual(descriptor.Indexes(), want) {
		t.Fatalf("indexes=%v want=%v", descriptor.Indexes(), want)
	}
	indexes := descriptor.Indexes()
	indexes[0] = "mutated"
	if descriptor.Indexes()[0] != "id" || model.Descriptor().Indexes()[0] != "Id" {
		t.Fatal("index snapshot leaked")
	}
}

type RepeatedIndex struct {
	Value string `chronicle:"index;index"`
}
type EventTagOnModel struct {
	Value string `chronicle:"unique"`
}

func TestIndexDeclarationsRejectDuplicatesAndWrongRoles(t *testing.T) {
	_, err := readmodels.Define[IndexedModel](readmodels.WithIndexes("email"))
	_, duplicate := readmodels.Define[RepeatedIndex]()
	_, role := readmodels.Define[EventTagOnModel]()
	for _, err := range []error{err, duplicate, role} {
		var declaration *declarations.DeclarationError
		if !errors.As(err, &declaration) {
			t.Fatalf("expected typed error: %v", err)
		}
	}
}
