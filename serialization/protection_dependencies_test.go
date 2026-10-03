// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func TestProtectionRootsInspectsSchemaDependencies(t *testing.T) {
	for _, tc := range []struct {
		kind    string
		subject bool
	}{
		{"EncryptedNamespace", false}, {"EncryptedGlobal", false},
		{"EncryptedSubject", true}, {"PII", true}, {"FutureProtection", false},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			metadata := `{"security":[{"metadataType":"` + tc.kind + `"}]}`
			if tc.kind == "PII" {
				metadata = `{"compliance":[{"metadataType":"PII"}]}`
			}
			for _, reference := range []bool{false, true} {
				child, definitions := metadata, ""
				if reference {
					child = `{"$ref":"#/definitions/private"}`
					definitions = `,"definitions":{"private":` + metadata + `}`
				}
				dependency := `{"dependencies":{"id":{"properties":{"name":` + child + `}}}}`
				roots, err := serialization.ProtectionRoots(`{"properties":{"nested":` + dependency + `}` + definitions + `}`)
				if err != nil || !reflect.DeepEqual(roots, map[string]bool{"nested": tc.subject}) {
					t.Fatalf("reference=%v roots=%v error=%v", reference, roots, err)
				}
				// Conditional root protection cannot be represented as a named release group.
				roots, err = serialization.ProtectionRoots(`{"properties":{"id":{"type":"string"},"name":{"type":"string"}},"dependencies":{"id":{"properties":{"name":` + child + `}}}` + definitions + `}`)
				if !errors.Is(err, faults.ErrInvalidConfiguration) || roots != nil {
					t.Fatalf("root dependency reference=%v became plain: %v %v", reference, roots, err)
				}
			}
		})
	}
}

func TestProtectionRootsInspectsDraftSevenSchemaEdges(t *testing.T) {
	classified := `{"security":[{"metadataType":"EncryptedNamespace"}]}`
	for _, keyword := range []string{
		"allOf", "anyOf", "oneOf", "not", "if", "then", "else", "items",
		"additionalItems", "additionalProperties", "propertyNames", "patternProperties", "contains", "dependencies",
	} {
		t.Run(keyword, func(t *testing.T) {
			child := classified
			switch keyword {
			case "allOf", "anyOf", "oneOf":
				child = `[true,false,` + classified + `]`
			case "patternProperties", "dependencies":
				child = `{"trigger":` + classified + `}`
			}
			edge := `{"` + keyword + `":` + child + `}`
			roots, err := serialization.ProtectionRoots(`{"properties":{"nested":` + edge + `}}`)
			if err != nil || !reflect.DeepEqual(roots, map[string]bool{"nested": false}) {
				t.Fatalf("roots=%v error=%v", roots, err)
			}
			roots, err = serialization.ProtectionRoots(edge)
			if !errors.Is(err, faults.ErrInvalidConfiguration) || roots != nil {
				t.Fatal("outside protection became plain", roots, err)
			}
		})
	}
}

func TestProtectionRootsRejectsAmbiguousDependencyAndExtensionContainers(t *testing.T) {
	for _, container := range []string{
		`"dependencies":null`, `"dependencies":[]`,
		`"dependencies":{"":{}}`, `"dependencies":{" ":{}}`,
		`"dependencies":{"id":null}`, `"dependencies":{"id":42}`, `"dependencies":{"id":"PRIVATE"}`,
		`"dependencies":{"id":["name","name"]}`,
		`"dependencies":{"id":["name",{"security":[{"metadataType":"PRIVATE"}]}]}`,
		`"dependencies":{"id":{"properties":{"name":{"security":null}}}}`,
		`"dependencies":{"id":{"properties":{"name":{"compliance":[{}]}}}}`,
		`"dependencies":{"id":{"$ref":"#/definitions/PRIVATE"}}`,
		`"dependencies":{"id":{"security":[{"metadataType":"PII"}]},"id":{}}`,
		`"definitions":{"unused":{"security":"PRIVATE"}}`,
		`"definitions":{"unused":null}`,
		`"propertyNames":{"security":{}}`,
		`"unknown":{"nested":{"security":null}}`,
		`"unknown":[{"compliance":[{"metadataType":"PRIVATE"}]}]`,
		`"unknown":{"$ref":"#/definitions/PRIVATE"}`,
	} {
		for _, outside := range []bool{false, true} {
			schema := `{"properties":{"nested":{` + container + `}}}`
			if outside {
				schema = `{"properties":{"id":{"type":"string"}},` + container + `}`
			}
			roots, err := serialization.ProtectionRoots(schema)
			if !errors.Is(err, faults.ErrInvalidConfiguration) || roots != nil || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("outside=%v container=%s roots=%v error=%v", outside, container, roots, err)
			}
		}
	}
}

func TestProtectionRootsAllowsUnclassifiedDependenciesAndInstanceAnnotations(t *testing.T) {
	for _, schema := range []string{
		`{"properties":{"id":{"type":"string"},"name":true,"absent":false},"dependencies":{"id":["name","security","compliance"],"name":[]}}`,
		`{"properties":{"id":{"type":"string"}},"dependencies":{"id":false,"name":{"properties":{"value":{"type":"number"}}}}}`,
		`{"properties":{"id":{"type":"string"}},"dependencies":{"id":{"$ref":"#/definitions/node"}},"definitions":{"node":{"properties":{"next":{"$ref":"#/definitions/node"}}}}}`,
		`{"properties":{"nested":{"dependencies":{"id":{"$ref":"#/definitions/node"}}}},"definitions":{"node":{"dependencies":{"next":{"$ref":"#/definitions/node"}}}}}`,
		`{"properties":{"id":{"type":"string"}},"dependencies":{"id":{"$ref":"#/definitions/boolean"}},"definitions":{"boolean":false}}`,
		`{"properties":{"nested":{"allOf":[true,false],"anyOf":[false],"oneOf":[true],"propertyNames":false,"patternProperties":{"x":true}}}}`,
		`{"properties":{"name":{"default":{"security":[{"metadataType":"PII"}]},"enum":[{"compliance":[{}]}],"const":{"$ref":"PRIVATE"},"examples":[{"security":null}]}}}`,
		`{"default":{"security":null},"enum":[{"compliance":null}],"dependencies":{"id":{"default":{"security":null},"enum":[{"security":null}]}}}`,
		`{"properties":{"id":{"type":"string"}},"unknown":{"plain":{"type":"string"}}}`,
	} {
		roots, err := serialization.ProtectionRoots(schema)
		if err != nil || len(roots) != 0 {
			t.Fatalf("unclassified schema refused: %s roots=%v error=%v", schema, roots, err)
		}
	}
}

func TestProtectionRootsBoundsDependencyReferenceExpansion(t *testing.T) {
	// Each definition references the next twice. This is shallow JSON but an
	// exponentially expanding graph; the shared inspector budget must refuse it.
	definitions := make([]string, 20)
	for n := range definitions {
		next := "end"
		if n+1 < len(definitions) {
			next = strings.Repeat("x", n+2)
		}
		definitions[n] = `"` + strings.Repeat("x", n+1) + `":{"dependencies":{"a":{"$ref":"#/definitions/` + next + `"},"b":{"$ref":"#/definitions/` + next + `"}}}`
	}
	schema := `{"properties":{"nested":{"dependencies":{"id":{"$ref":"#/definitions/x"}}}},"definitions":{"end":true,` + strings.Join(definitions, ",") + `}}`
	roots, err := serialization.ProtectionRoots(schema)
	if !errors.Is(err, faults.ErrInvalidConfiguration) || roots != nil {
		t.Fatal("unbounded reference expansion admitted", roots, err)
	}
}
