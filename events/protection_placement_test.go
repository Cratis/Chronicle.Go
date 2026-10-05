// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

type personalNames []string
type personalMap map[string]string
type unsupportedProtection struct {
	Maps    map[string]classifiedAddress
	Arrays  []*personalNames
	MapRows []personalMap
}

func TestUnsupportedProtectionPlacementsFailBeforeRegistration(t *testing.T) {
	for _, metadata := range []compliance.Classification{{PII: true}, {Encrypted: true}} {
		for _, declaration := range []compliance.Declaration{
			compliance.For[classifiedAddress](metadata),
			compliance.For[personalNames](metadata),
			compliance.For[personalMap](metadata),
			compliance.Property("Maps.street", metadata),
			compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
				if target.Field == "Street" {
					return metadata, nil
				}
				return compliance.Classification{}, nil
			}),
		} {
			for _, define := range []func() error{
				func() error {
					_, err := events.Define[unsupportedProtection](events.WithProtection(declaration))
					return err
				},
				func() error {
					_, err := readmodels.Define[unsupportedProtection](readmodels.WithProtection(declaration))
					return err
				},
			} {
				var typed *declarations.DeclarationError
				if err := define(); !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &typed) {
					t.Fatalf("unsupported placement did not fail closed: %v", err)
				}
			}
		}
	}
	type TaggedMap struct {
		Values map[string]struct {
			Email string `chronicle:"pii"`
		}
	}
	if _, err := events.Define[TaggedMap](); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal("map value protection tag was ignored")
	}
}

func TestCoarsePropertyProtectionCoversMapAndArrayDescendants(t *testing.T) {
	type Coarse struct {
		Map  map[string]classifiedAddress `chronicle:"pii"`
		Rows []personalNames              `chronicle:"pii"`
	}
	if _, err := events.Define[Coarse](events.WithProtection(compliance.For[personalNames](compliance.Classification{PII: true}))); err != nil {
		t.Fatal(err)
	}
	type RecursiveMap struct {
		Name     string `chronicle:"pii"`
		Children map[string]*RecursiveMap
	}
	if _, err := events.Define[RecursiveMap](); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal("recursive map reference bypassed placement validation")
	}
}

func TestRecursivePropertyOverrideDoesNotRepeat(t *testing.T) {
	type Tree struct {
		Name string
		Next *Tree
	}
	protection := compliance.Property("Next.Name", compliance.Classification{PII: true})
	event, err := events.Define[Tree](events.WithProtection(protection))
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[Tree](readmodels.WithProtection(protection))
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{event.Descriptor().Schema(), model.Descriptor().Schema()} {
		var schema map[string]any
		if err := json.Unmarshal([]byte(text), &schema); err != nil {
			t.Fatal(err)
		}
		definitions, _ := schema["definitions"].(map[string]any)
		for depth := range 6 {
			for schema["$ref"] != nil {
				schema = definitions[strings.TrimPrefix(schema["$ref"].(string), "#/definitions/")].(map[string]any)
			}
			properties := schema["properties"].(map[string]any)
			if protected := properties["Name"].(map[string]any)["compliance"] != nil; protected != (depth == 1) {
				t.Fatalf("override classification at depth %d = %t", depth, protected)
			}
			schema = properties["Next"].(map[string]any)
		}
	}
}

func TestRecursiveOverrideSchemaReferencesAllResolve(t *testing.T) {
	type Tree struct {
		Name string
		Next *Tree
	}
	model, err := readmodels.Define[Tree](readmodels.WithProtection(compliance.Property("Next.Name", compliance.Classification{PII: true})))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal([]byte(model.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	definitions, _ := schema["definitions"].(map[string]any)
	count := 0
	var inspect func(any)
	inspect = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			if ref, ok := value["$ref"].(string); ok {
				count++
				name, local := strings.CutPrefix(ref, "#/definitions/")
				if !local || definitions[name] == nil {
					t.Errorf("unresolved reference %q", ref)
				}
			}
			for _, child := range value {
				inspect(child)
			}
		case []any:
			for _, child := range value {
				inspect(child)
			}
		}
	}
	inspect(schema)
	if count == 0 {
		t.Fatal("recursive schema lost its references")
	}
}
