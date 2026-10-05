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

func TestProtectionRootsPreservesScopesAndUnknownMetadataThroughReferences(t *testing.T) {
	schema := `{"properties":{
		"plain":{"type":"string"},
		"private":{"$ref":"#/definitions/a~1b~0c"},
		"shared":{"items":{"security":[{"metadataType":"EncryptedNamespace"}]}},
		"global":{"additionalProperties":{"security":[{"metadataType":"EncryptedGlobal"}]}},
		"future":{"allOf":[{"security":[{"metadataType":"FutureProtection"}]}]}
	},"definitions":{"a/b~c":{"properties":{"next":{"$ref":"#/definitions/a~1b~0c"},"secret":{"compliance":[{"metadataType":"PII"}]}}}}}`
	got, err := serialization.ProtectionRoots(schema)
	want := map[string]bool{"private": true, "shared": false, "global": false, "future": false}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("roots=%v error=%v", got, err)
	}
}

func TestProtectionRootsRejectsMalformedOrUnrepresentableMetadata(t *testing.T) {
	for _, schema := range []string{
		`null`, `[]`, `{"properties":"PRIVATE"}`, `{"properties":{"secret":null}}`,
		`{"properties":{"secret":{"security":null}}}`, `{"properties":{"secret":{"security":{}}}}`,
		`{"properties":{"secret":{"compliance":["PRIVATE"]}}}`, `{"properties":{"secret":{"compliance":[{"metadataType":false}]}}}`,
		`{"properties":{"secret":{"$ref":42}}}`, `{"properties":{"secret":{"$ref":"#/definitions/missing"}}}`,
		`{"properties":{"secret":{"$ref":"#/definitions/a~2b"}},"definitions":{"a~2b":{}}}`,
		`{"properties":{"secret":{"items":null}}}`, `{"properties":{"secret":{"anyOf":{}}}}`,
		`{"properties":{"secret":{"compliance":[{"metadataType":"PII"}]},"secret":{}}}`,
		`{"properties":{"id":{"type":"string"}},"security":[{"metadataType":"EncryptedGlobal"}]}`,
		`{"properties":{"id":{"type":"string"}},"additionalProperties":{"compliance":[{"metadataType":"PII"}]}}`,
	} {
		t.Run(schema, func(t *testing.T) {
			roots, err := serialization.ProtectionRoots(schema)
			if !errors.Is(err, faults.ErrInvalidConfiguration) || roots != nil || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal("metadata became plain", roots, err)
			}
		})
	}
}
