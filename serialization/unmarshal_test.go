// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/fundamentals.go/concepts"
)

type decodeNested struct {
	URLValue string
	Value    int `json:"explicit_value"`
	Next     *decodeNested
}
type decodeEvent struct {
	Nodes      []decodeNested
	Fixed      [1]decodeNested
	Dictionary map[string]decodeNested
	Node       *decodeNested
	Author     conceptfixtures.AuthorID
	Optional   *concepts.UUID
}

func TestPlanDecodeUsesGenerationFieldNamesAndConcepts(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[decodeEvent](), serialization.LegacyGoCamelCase)
	if err != nil {
		t.Fatal(err)
	}
	var actual decodeEvent
	err = plan.Unmarshal([]byte(`{"nodes":[{"uRLValue":"slice","explicit_value":3}],"fixed":[{"uRLValue":"array"}],"dictionary":{"one":{"uRLValue":"map"}},"node":{"uRLValue":"pointer","next":{"uRLValue":"recursive"}},"author":"00112233-4455-6677-8899-aabbccddeeff","optional":null}`), &actual)
	if err != nil {
		t.Fatal(err)
	}
	if actual.Nodes[0].URLValue != "slice" || actual.Nodes[0].Value != 3 || actual.Fixed[0].URLValue != "array" || actual.Dictionary["one"].URLValue != "map" || actual.Node.URLValue != "pointer" || actual.Node.Next.URLValue != "recursive" || actual.Optional != nil {
		t.Fatalf("decoded: %+v", actual)
	}
	encoded, err := plan.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip decodeEvent
	if err := plan.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, roundTrip) {
		t.Fatal("concept/collection roundtrip changed")
	}
	before := actual
	if err := plan.Unmarshal([]byte(`{"nodes":[{"explicit_value":"invalid"}]}`), &actual); !errors.Is(err, faults.ErrProtocol) {
		t.Fatalf("invalid content: %v", err)
	}
	if !reflect.DeepEqual(actual, before) {
		t.Fatal("failed decode partially mutated target")
	}
	for _, data := range []string{"null", "[]", "{} trailing", `{"author":42}`} {
		if err := plan.Unmarshal([]byte(data), &actual); !errors.Is(err, faults.ErrProtocol) {
			t.Fatalf("invalid %s: %v", data, err)
		}
	}
	if err := plan.Unmarshal([]byte("{}"), actual); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("nonpointer: %v", err)
	}
}
