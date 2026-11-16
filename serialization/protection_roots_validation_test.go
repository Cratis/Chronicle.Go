// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func TestProtectionRootsRejectsInvalidReferenceTargetsAndSchemaContainers(t *testing.T) {
	for _, schema := range []string{
		`{"properties":{"secret":{"$ref":"external.json"}}}`,
		`{"properties":{"secret":{"$ref":"#/%zz"}}}`,
		`{"properties":{"secret":{"$ref":"#/title"}},"title":"PRIVATE"}`,
		`{"properties":{"secret":{"$ref":"#/title/child"}},"title":"PRIVATE"}`,
		`{"properties":{"secret":{"security":[{"metadataType":" "}]}}}`,
		`{"properties":{"secret":{"patternProperties":[]}}}`,
		`{"properties":{"secret":{"definitions":[]}}}`,
		`{"properties":{"secret":{"allOf":[]}}}`,
	} {
		t.Run(schema, func(t *testing.T) {
			roots, err := serialization.ProtectionRoots(schema)
			if !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("error = %v", err)
			}
			if roots != nil {
				t.Fatalf("roots = %v", roots)
			}
		})
	}
}

func TestProtectionRootsValidatesButDoesNotApplyUnusedDefinitions(t *testing.T) {
	roots, err := serialization.ProtectionRoots(`{"properties":{"plain":{}},"definitions":{"unused":{"security":[{"metadataType":"PII"}]}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 0 {
		t.Fatalf("roots = %v", roots)
	}
}
