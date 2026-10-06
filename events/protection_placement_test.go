// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type personalNames []string
type personalMap map[string]string
type nestedProtection struct {
	Maps    map[string]classifiedAddress
	Arrays  []*personalNames
	MapRows []personalMap
}

// Chronicle#4551 and #4552 are fixed in 19.32.2: the kernel applies metadata
// beneath unprotected maps and protects classified collection-valued array
// elements as a whole, so these placements register and keep their metadata.
func TestProtectionBeneathMapsAndCollectionElementsIsAdmitted(t *testing.T) {
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
			for _, define := range []func() (string, error){
				func() (string, error) {
					d, err := events.Define[nestedProtection](events.WithProtection(declaration))
					return d.Descriptor().Schema(), err
				},
				func() (string, error) {
					d, err := readmodels.Define[nestedProtection](readmodels.WithProtection(declaration))
					return d.Descriptor().Schema(), err
				},
			} {
				schema, err := define()
				if err != nil {
					t.Fatalf("nested placement refused: %v", err)
				}
				if !strings.Contains(schema, `"compliance"`) && !strings.Contains(schema, `"security"`) {
					t.Fatalf("nested placement lost its metadata: %s", schema)
				}
			}
		}
	}
	type TaggedMap struct {
		Values map[string]struct {
			Email string `chronicle:"pii"`
		}
	}
	event, err := events.Define[TaggedMap]()
	if err != nil {
		t.Fatal("map value protection tag refused", err)
	}
	var schema struct {
		Properties map[string]struct {
			AdditionalProperties struct {
				Properties map[string]map[string]any `json:"properties"`
			} `json:"additionalProperties"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(event.Descriptor().Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Properties["Values"].AdditionalProperties.Properties["Email"]["compliance"] == nil {
		t.Fatalf("map value tag metadata missing: %s", event.Descriptor().Schema())
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
	if _, err := events.Define[RecursiveMap](); err != nil {
		t.Fatal("recursive map placement refused", err)
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

// Registration on kernels before 19.32.2 refuses exactly these placements.
func TestNestedCollectionProtectionDetectsOnlyKernelSensitivePlacements(t *testing.T) {
	pii := compliance.Classification{PII: true}
	type Coarse struct {
		Map  map[string]classifiedAddress `chronicle:"pii"`
		Rows []personalNames              `chronicle:"pii"`
	}
	type ScalarItems struct {
		Names []string `chronicle:"pii"`
		Rows  [][]string
		Nodes []classifiedAddress
	}
	for _, tc := range []struct {
		name   string
		schema func() (string, error)
		nested bool
	}{
		{"map value member", func() (string, error) {
			d, err := events.Define[nestedProtection](events.WithProtection(compliance.Property("Maps.street", pii)))
			return d.Descriptor().Schema(), err
		}, true},
		{"collection-valued array element", func() (string, error) {
			d, err := events.Define[nestedProtection](events.WithProtection(compliance.For[personalNames](pii)))
			return d.Descriptor().Schema(), err
		}, true},
		{"map-valued array element", func() (string, error) {
			d, err := events.Define[nestedProtection](events.WithProtection(compliance.For[personalMap](pii)))
			return d.Descriptor().Schema(), err
		}, true},
		{"coarse containers", func() (string, error) {
			d, err := events.Define[Coarse](events.WithProtection(compliance.For[personalNames](pii)))
			return d.Descriptor().Schema(), err
		}, false},
		{"scalar items and declared members", func() (string, error) {
			d, err := events.Define[ScalarItems](events.WithProtection(compliance.For[classifiedAddress](pii)))
			return d.Descriptor().Schema(), err
		}, false},
	} {
		schema, err := tc.schema()
		if err != nil {
			t.Fatal(tc.name, err)
		}
		if got := serialization.NestedCollectionProtection(schema); got != tc.nested {
			t.Errorf("%s: nested = %v", tc.name, got)
		}
	}
	if !serialization.NestedCollectionProtection(`{"properties":`) {
		t.Fatal("malformed schema must fail closed")
	}
}
