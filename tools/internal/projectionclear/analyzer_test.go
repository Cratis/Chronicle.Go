// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projectionclear_test

import (
	"errors"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	sdk "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/tools/internal/projectionclear"
	"github.com/cratis/chronicle.go/tools/testdata/projectionconsumer"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestProjectionClearDiagnostics(t *testing.T) {
	t.Setenv("GOWORK", "off")
	t.Setenv("GOTOOLCHAIN", "local")
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOFLAGS", "")
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	results := analysistest.Run(t, dir, projectionclear.Analyzer, "./testdata/projectionconsumer")
	if len(results) != 1 {
		t.Fatalf("got %d analyzed packages, want 1", len(results))
	}
	result := results[0]
	if len(result.Action.Diagnostics) != 1 {
		t.Fatalf("got %d diagnostics, want 1", len(result.Action.Diagnostics))
	}
	assertClearDiagnostic(t, result.Action.Package.Fset, result.Action.Diagnostics[0])
}

func assertClearDiagnostic(t *testing.T, fset *token.FileSet, diagnostic analysis.Diagnostic) {
	t.Helper()
	start := fixturePosition(fset, diagnostic.Pos)
	end := fixturePosition(fset, diagnostic.End)
	wantStart := token.Position{Filename: "clear.go", Line: 38, Column: 15}
	wantEnd := token.Position{Filename: "clear.go", Line: 38, Column: 20}
	if start != wantStart || end != wantEnd {
		t.Errorf("got diagnostic range %s–%s, want %s–%s", start, end, wantStart, wantEnd)
	}
	const message = "CHR0048: clear requires a nullable field with a matching descriptor; otherwise set an explicit value"
	if diagnostic.Category != "CHR0048" || diagnostic.URL != "#CHR0048" {
		t.Error("expected fixed CHR0048 category and URL fragment")
	}
	if diagnostic.Message != message {
		t.Error("expected a fixed, payload-free message")
	}
	if len(diagnostic.Related) != 0 || len(diagnostic.SuggestedFixes) != 0 {
		t.Error("diagnostic must omit related information and suggested fixes")
	}
}

func fixturePosition(fset *token.FileSet, pos token.Pos) token.Position {
	p := fset.Position(pos)
	return token.Position{Filename: filepath.Base(p.Filename), Line: p.Line, Column: p.Column}
}

func TestRuntimeProjectionClearWitness(t *testing.T) {
	assertRequiredClearRejected(t, projectionconsumer.ClearRequired())
	if err := projectionconsumer.ClearOptional(); err != nil {
		t.Fatalf("nullable clear rejected: %v", err)
	}
}

func assertRequiredClearRejected(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, sdk.ErrInvalidConfiguration) {
		t.Fatalf("required clear error = %v; want invalid configuration from Build", err)
	}
	var declaration *projections.DeclarationError
	if !errors.As(err, &declaration) {
		t.Fatalf("required clear error = %v; want declaration error", err)
	}
	if declaration.Directive != "clear" {
		t.Fatalf("unexpected declaration directive: %v", declaration)
	}
	if declaration.Path != "note" || declaration.GoField != "Note" {
		t.Fatalf("unexpected clear target: %v", declaration)
	}
	const suffix = ": null requires a supported nullable pointer"
	if !strings.HasSuffix(declaration.Message, suffix) {
		t.Fatalf("unexpected clear rejection reason: %v", declaration)
	}
	if declaration.Cause == nil || !strings.HasSuffix(declaration.Cause.Error(), suffix) {
		t.Fatalf("missing underlying clear rejection: %v", declaration)
	}
}
