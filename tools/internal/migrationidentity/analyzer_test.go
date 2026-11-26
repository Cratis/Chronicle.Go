// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package migrationidentity_test

import (
	"errors"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	sdk "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/tools/internal/migrationidentity"
	"github.com/cratis/chronicle.go/tools/testdata/migrationconsumer"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestMigrationIdentityDiagnostics(t *testing.T) {
	t.Setenv("GOWORK", "off")
	t.Setenv("GOTOOLCHAIN", "local")
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOFLAGS", "")
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	results := analysistest.Run(t, dir, migrationidentity.Analyzer, "./testdata/migrationconsumer")
	if len(results) != 1 {
		t.Fatalf("got %d analyzed packages, want 1", len(results))
	}
	assertMigrationLocations(t, results[0])
}

func assertMigrationLocations(t *testing.T, result *analysistest.Result) {
	t.Helper()
	want := map[token.Position]int{
		{Filename: "migration.go", Line: 24, Column: 25}: 40,
		{Filename: "migration.go", Line: 46, Column: 24}: 39,
	}
	diagnostics := result.Action.Diagnostics
	if len(diagnostics) != len(want) {
		t.Fatalf("got %d diagnostics, want %d", len(diagnostics), len(want))
	}
	fset := result.Action.Package.Fset
	for _, diagnostic := range diagnostics {
		start := fixturePosition(fset, diagnostic.Pos)
		column, ok := want[start]
		end := start
		end.Column = column
		if !ok || end != fixturePosition(fset, diagnostic.End) {
			t.Errorf("unexpected diagnostic range at %s", start)
		}
		delete(want, start)
		assertFixedDiagnostic(t, diagnostic)
	}
	if len(want) != 0 {
		t.Errorf("missing migration diagnostics at %v", want)
	}
}

func fixturePosition(fset *token.FileSet, pos token.Pos) token.Position {
	p := fset.Position(pos)
	return token.Position{Filename: filepath.Base(p.Filename), Line: p.Line, Column: p.Column}
}

func assertFixedDiagnostic(t *testing.T, diagnostic analysis.Diagnostic) {
	t.Helper()
	const want = "CHR0037: migration endpoints have different event type IDs; derive the previous generation from the current event type"
	if diagnostic.Category != "CHR0037" || diagnostic.Message != want {
		t.Error("expected fixed CHR0037 diagnostic and remedy")
	}
	if strings.Contains(diagnostic.Message, "CANARY_") || len(diagnostic.SuggestedFixes) != 0 {
		t.Error("diagnostic must not reveal endpoint IDs or suggest a fix")
	}
}

func TestRuntimeMigrationIdentityWitness(t *testing.T) {
	mapping := events.Migration[migrationconsumer.V2, migrationconsumer.V1]{
		Upcast:   func(*events.MigrationBuilder[migrationconsumer.V2, migrationconsumer.V1]) {},
		Downcast: func(*events.MigrationBuilder[migrationconsumer.V1, migrationconsumer.V2]) {},
	}
	previous, upgrade, compileErr, setupErr := migrationconsumer.Mismatch(mapping)
	if setupErr != nil {
		t.Fatalf("expected authoring and catalog creation to succeed: %v", setupErr)
	}
	if previous.Generation != 1 || upgrade.Generation != 2 {
		t.Fatal("witness must have adjacent generations 1 and 2")
	}
	if previous.ID == upgrade.ID {
		t.Fatal("witness must have different endpoint IDs")
	}
	if !errors.Is(compileErr, sdk.ErrInvalidConfiguration) {
		t.Fatal("expected catalog compilation to reject the migration")
	}
	if !strings.HasSuffix(compileErr.Error(), ": migration requires the same ID and adjacent increasing generations") {
		t.Fatal("expected catalog compilation to reject migration endpoint identities")
	}
	if err := migrationconsumer.DerivedHistory(mapping); err != nil {
		t.Fatalf("same-lineage migration rejected: %v", err)
	}
}
