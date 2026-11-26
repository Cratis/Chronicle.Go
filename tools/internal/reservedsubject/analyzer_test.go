// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reservedsubject_test

import (
	"errors"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	sdk "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/tools/internal/reservedsubject"
	"github.com/cratis/chronicle.go/tools/testdata/modelconsumer"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestReservedSubjectDiagnostics(t *testing.T) {
	t.Setenv("GOWORK", "off")
	t.Setenv("GOTOOLCHAIN", "local")
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOFLAGS", "")
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	results := analysistest.Run(t, dir, reservedsubject.Analyzer, "./testdata/modelconsumer")
	if len(results) != 1 {
		t.Fatalf("got %d analyzed packages, want 1", len(results))
	}
	assertTagLocations(t, results[0])
}

func assertTagLocations(t *testing.T, result *analysistest.Result) {
	t.Helper()
	want := map[token.Position]int{
		{Filename: "model.go", Line: 14, Column: 21}: 39,
		{Filename: "model.go", Line: 18, Column: 15}: 42,
		{Filename: "model.go", Line: 22, Column: 15}: 43,
		{Filename: "model.go", Line: 26, Column: 15}: 35,
		{Filename: "model.go", Line: 33, Column: 17}: 35,
		{Filename: "model.go", Line: 71, Column: 16}: 34,
		{Filename: "model.go", Line: 72, Column: 16}: 35,
	}
	diagnostics := result.Action.Diagnostics
	if len(diagnostics) != len(want) {
		t.Fatalf("got %d diagnostics, want %d", len(diagnostics), len(want))
	}
	fset := result.Action.Package.Fset
	for _, diagnostic := range diagnostics {
		start := fixturePosition(fset, diagnostic.Pos)
		end := start
		end.Column = want[start]
		if end.Column == 0 || end != fixturePosition(fset, diagnostic.End) {
			t.Errorf("unexpected tag range at %s", start)
		}
		delete(want, start)
		assertFixedDiagnostic(t, diagnostic)
	}
	if len(want) != 0 {
		t.Errorf("missing tag diagnostics at %v", want)
	}
}

func fixturePosition(fset *token.FileSet, pos token.Pos) token.Position {
	p := fset.Position(pos)
	return token.Position{Filename: filepath.Base(p.Filename), Line: p.Line, Column: p.Column}
}

func assertFixedDiagnostic(t *testing.T, diagnostic analysis.Diagnostic) {
	t.Helper()
	const want = "CHR0035: rename the JSON property; Chronicle reserves this name for compliance subjects"
	if diagnostic.Category != "CHR0035" || diagnostic.Message != want {
		t.Error("expected fixed CHR0035 diagnostic and remedy")
	}
	if len(diagnostic.SuggestedFixes) != 0 || len(diagnostic.Related) != 0 {
		t.Error("diagnostic must omit fixes and related text")
	}
	if diagnostic.URL != "#CHR0035" || strings.Contains(diagnostic.Message, "CANARY") {
		t.Error("diagnostic must not expose model or field names")
	}
}

func TestRuntimeReservedSubjectWitness(t *testing.T) {
	for _, tc := range []struct {
		name, property string
		register       func() error
	}{
		{"double underscore", "__subject", modelconsumer.RegisterDoubleUnderscore},
		{"single underscore", "_subject", modelconsumer.RegisterSingleUnderscore},
		{"plural", "__subjects", modelconsumer.RegisterPlural},
		{"mixed case", "__SuBjEcT", modelconsumer.RegisterMixedCase},
		{"scalar siblings", "__subject", modelconsumer.RegisterSiblings},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.register()
			if !errors.Is(err, sdk.ErrInvalidConfiguration) {
				t.Fatalf("registration error = %v; want invalid configuration", err)
			}
			if !strings.HasSuffix(err.Error(), ": reserved compliance property: "+tc.property) {
				t.Fatalf("registration error = %v; want reserved property rejection", err)
			}
		})
	}
	if err := modelconsumer.RegisterRenamed(); err != nil {
		t.Fatalf("renamed property rejected: %v", err)
	}
}
