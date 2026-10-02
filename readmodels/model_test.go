// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestReadModelDefaultsAndCatalogIsolation(t *testing.T) {
	indexes := []string{"name", "children.names"}
	model := person(t, readmodels.WithIndexes(indexes...))
	indexes[0] = "changed"
	d := model.Descriptor()
	if d.Identifier() != "github.com/cratis/chronicle.go/readmodels_test.Person" || d.ContainerName() != "People" || d.DisplayName() != "Person" || d.Generation() != 1 || d.Sink().Type != readmodels.MongoDB || d.Sink().ConfigurationID != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("defaults: %s %s %+v", d.Identifier(), d.ContainerName(), d.Sink())
	}
	kind, id := d.Observer()
	if kind != readmodels.Projection || id != "" || d.EventSequence() != "event-log" {
		t.Fatal("wrong producer defaults")
	}
	catalog, err := readmodels.NewCatalog(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{Person{}, &Person{}} {
		if got, ok := catalog.Lookup(value); !ok || got.GoType() != reflect.TypeFor[Person]() {
			t.Fatal("lookup failed")
		}
	}
	var nilPerson *Person
	if _, ok := catalog.Lookup(nilPerson); ok {
		t.Fatal("nil pointer resolved")
	}
	if got, ok := catalog.LookupType(reflect.TypeFor[*Person]()); !ok || got.Identifier() != d.Identifier() {
		t.Fatal("type lookup failed")
	}
	d.Indexes()[0] = "changed"
	catalog.Descriptors()[0] = readmodels.Descriptor{}
	if got, _ := catalog.LookupIdentifier(d.Identifier()); got.Indexes()[0] != "name" {
		t.Fatal("catalog mutated")
	}
	if _, err = readmodels.NewCatalog(d, d); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}

func TestReadModelInvalidDeclarationsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		option readmodels.ModelOption
		err    error
	}{
		{"nil", nil, chronicle.ErrInvalidConfiguration}, {"blank identity", readmodels.WithIdentifier(" "), chronicle.ErrInvalidConfiguration},
		{"generation zero", readmodels.WithGeneration(0), chronicle.ErrInvalidConfiguration}, {"blank container", readmodels.WithContainerName(""), chronicle.ErrInvalidConfiguration},
		{"unknown index", readmodels.WithIndexes("missing"), chronicle.ErrInvalidConfiguration}, {"duplicate index", readmodels.WithIndexes("name", "name"), chronicle.ErrInvalidConfiguration},
		{"nonstring PII", readmodels.WithPII("count"), chronicle.ErrUnsupported}, {"key PII", readmodels.WithPII("id"), chronicle.ErrInvalidConfiguration},
		{"unknown sink", readmodels.WithSink(readmodels.Sink{Type: "future"}), chronicle.ErrUnsupported}, {"invalid sink UUID", readmodels.WithSink(readmodels.Sink{Type: readmodels.MongoDB, ConfigurationID: "not-uuid"}), chronicle.ErrInvalidConfiguration},
		{"unknown observer", readmodels.WithObserver(0, ""), chronicle.ErrInvalidConfiguration},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := readmodels.Define[Person](tc.option); !errors.Is(err, tc.err) {
				t.Fatal(err)
			}
		})
	}
	if _, err := readmodels.Define[*Person](); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if _, err := readmodels.Define[Person](readmodels.WithObserver(readmodels.Reducer, "r"), readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink})); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal(err)
	}
	type Reserved struct {
		Value string `json:"__subject"`
	}
	if _, err := readmodels.Define[Reserved](); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	type Protected struct {
		Value string `chronicle:"encrypted"`
	}
	if _, err := readmodels.Define[Protected](); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestReadModelMetadataUsesSerializationPlan(t *testing.T) {
	model := person(t, readmodels.WithPII("name"), readmodels.WithIndexes("children.names"))
	var schema map[string]any
	if err := json.Unmarshal([]byte(model.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	name := schema["properties"].(map[string]any)["name"].(map[string]any)
	pii := name["compliance"].([]any)[0].(map[string]any)
	if pii["metadataType"] != "PII" || pii["details"] != "" {
		t.Fatal(pii)
	}
	data, err := model.Descriptor().Marshal(Person{Name: "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err = json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	if value["name"] != "Ada" {
		t.Fatal("schema and codec differ")
	}
}

func TestPassiveReadModelRequiresObserver(t *testing.T) {
	for _, id := range []string{"", " \t\n"} {
		if _, err := readmodels.Define[Person](readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}), readmodels.WithObserver(readmodels.Projection, id)); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("observer %q: %v", id, err)
		}
	}
	if _, err := readmodels.Define[Person](readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}), readmodels.WithObserver(readmodels.Reducer, "")); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("blank reducer observer: %v", err)
	}
	if _, err := readmodels.Define[Person](readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink})); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("omitted observer: %v", err)
	}
	if _, err := readmodels.Define[Person](readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}), readmodels.WithObserver(readmodels.Projection, "producer")); err != nil {
		t.Fatal(err)
	}
}

func TestReadModelPropertyOptionsAccumulate(t *testing.T) {
	type OptionModel struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	indexes := []string{"name"}
	pii := []string{"name"}
	indexOption, piiOption := readmodels.WithIndexes(indexes...), readmodels.WithPII(pii...)
	indexes[0], pii[0] = "changed", "changed"
	// Reusing an option must not retain or mutate another declaration's paths.
	for range 2 {
		model, err := readmodels.Define[OptionModel](indexOption, readmodels.WithIndexes("email"), piiOption, readmodels.WithPII("email"))
		if err != nil {
			t.Fatal(err)
		}
		if got := model.Descriptor().Indexes(); !reflect.DeepEqual(got, []string{"name", "email"}) {
			t.Fatalf("indexes = %v", got)
		}
		var schema map[string]any
		if err := json.Unmarshal([]byte(model.Descriptor().Schema()), &schema); err != nil {
			t.Fatal(err)
		}
		properties := schema["properties"].(map[string]any)
		name := properties["name"].(map[string]any)
		email := properties["email"].(map[string]any)
		if name["compliance"] == nil || email["compliance"] == nil {
			t.Fatal("PII options did not accumulate")
		}
	}
	for _, options := range [][]readmodels.ModelOption{
		{readmodels.WithIndexes("name"), readmodels.WithIndexes("name")},
		{readmodels.WithPII("name"), readmodels.WithPII("name")},
		{readmodels.WithPII("name", "name")},
	} {
		if _, err := readmodels.Define[Person](options...); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("duplicate paths: %v", err)
		}
	}
}

type GenericReadModel[T any] struct {
	Value T `json:"value"`
}

func TestGenericReadModelRequiresExplicitNames(t *testing.T) {
	for _, options := range [][]readmodels.ModelOption{
		nil,
		{readmodels.WithContainerName("Boxes")},
		{readmodels.WithIdentifier("Shared.Box")},
	} {
		if _, err := chronicle.RegisterReadModel[GenericReadModel[string]](chronicle.NewRegistry(), options...); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("implicit generic names: %v", err)
		}
	}
	model, err := chronicle.RegisterReadModel[GenericReadModel[string]](chronicle.NewRegistry(), readmodels.WithIdentifier("Shared.Box"), readmodels.WithContainerName("Boxes"))
	if err != nil {
		t.Fatal(err)
	}
	if model.Identifier() != "Shared.Box" || model.Descriptor().ContainerName() != "Boxes" {
		t.Fatal("explicit generic names lost")
	}
}
