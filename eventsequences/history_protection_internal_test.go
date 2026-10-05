// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

func TestRevisionSchemaInspectionFailsClosed(t *testing.T) {
	for _, schema := range []string{
		`invalid synthetic-private schema`,
		`{"properties":{"Secret":{"security":[{"metadataType":"EncryptedNamespace"}]}}}`,
		`{"properties":{"Secret":{"security":[{"metadataType":"EncryptedGlobal"}]}}}`,
	} {
		err := validateRevisionSchema(schema)
		var unknown *MutationOutcomeUnknownError
		if !errors.Is(err, faults.ErrUnsupported) || errors.As(err, &unknown) || err != errProtectedRevisionUnsupported {
			t.Fatalf("metadata rejection error = %v", err)
		}
	}
	if err := validateRevisionSchema(`{"properties":{"Value":{"type":"string"}}}`); err != nil {
		t.Fatal(err)
	}
}
