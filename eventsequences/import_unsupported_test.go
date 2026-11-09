// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//go:build !chronicle_imports

package eventsequences

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

func TestImportsFailExplicitlyWithoutTheImportsContract(t *testing.T) {
	sequence, _ := importFixture(t)
	sequence.imports = nil
	if _, err := sequence.Import(t.Context(), ImportEntries(entries(1))); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("Import err = %v", err)
	}
	if _, err := sequence.BeginImport(t.Context()); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("BeginImport err = %v", err)
	}
	id, _ := NewImportID()
	if _, err := sequence.ReattachImport(t.Context(), id); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("ReattachImport err = %v", err)
	}
}
