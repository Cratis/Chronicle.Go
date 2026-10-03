// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type MapIndexedModel struct {
	Locations map[string]IndexedAddress `json:"locations" chronicle:"index"`
	Pointer   *map[string]IndexedAddress
	Lists     []map[string]IndexedAddress
	Nested    struct {
		ByName map[string][]IndexedAddress
		Home   IndexedAddress
	}
	Home      IndexedAddress
	Addresses []IndexedAddress
}

func TestIndexTagsSkipMapValuesLikeCSharpDictionaries(t *testing.T) {
	model, err := readmodels.Define[MapIndexedModel]()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"locations", "Nested.Home.City", "Home.City", "Addresses.City"}
	if got := model.Descriptor().Indexes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("indexes = %v, want %v", got, want)
	}
	camel, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"locations", "nested.home.city", "home.city", "addresses.city"}
	if got := camel.Indexes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("camel indexes = %v, want %v", got, want)
	}
}
