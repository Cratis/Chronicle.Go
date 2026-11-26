// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventprotection_test

import (
	"errors"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	sdk "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/tools/internal/eventprotection"
	"github.com/cratis/chronicle.go/tools/testdata/consumer"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

type tagSpan struct {
	file string
	line int
	tag  string
}

func TestEventProtectionDiagnostics(t *testing.T) {
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOWORK", "off")
	t.Setenv("GOTOOLCHAIN", "local")
	t.Setenv("GOPROXY", "off")
	for _, tc := range []struct {
		name, flags string
	}{
		{name: "default"},
		{name: "conflict tag", flags: "-tags=conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOFLAGS", tc.flags)
			want := []tagSpan{
				{"events.go", 12, "`" + `chronicle:"pii;encrypted(details=\"CANARY_CLASSIFICATION_DETAILS\")"` + "`"},
				{"events.go", 16, "`" + `json:",omitempty,omitzero" chronicle:"encrypted;pii"` + "`"},
				{"zz_generated.go", 11, "`" + `chronicle:"pii;encrypted"` + "`"},
			}
			if tc.flags != "" {
				want = append(want, tagSpan{"excluded.go", 11, "`" + `chronicle:"pii;encrypted"` + "`"})
			}
			results := analysistest.Run(t, dir, eventprotection.Analyzer, "./testdata/consumer")
			if len(results) != 1 {
				t.Fatalf("got %d analyzed packages, want 1", len(results))
			}
			assertTagDiagnostics(t, results[0], want)
		})
	}
}

func assertTagDiagnostics(t *testing.T, result *analysistest.Result, spans []tagSpan) {
	t.Helper()
	diagnostics := result.Action.Diagnostics
	if len(diagnostics) != len(spans) {
		t.Fatalf("got %d diagnostics, want %d", len(diagnostics), len(spans))
	}
	remaining := make(map[token.Position]token.Position, len(spans))
	for _, span := range spans {
		start := token.Position{Filename: span.file, Line: span.line, Column: 15}
		end := start
		end.Column += len(span.tag)
		remaining[start] = end
	}
	fset := result.Action.Package.Fset
	for _, diagnostic := range diagnostics {
		start := fixturePosition(fset, diagnostic.Pos)
		end := fixturePosition(fset, diagnostic.End)
		if wantEnd, ok := remaining[start]; !ok || end != wantEnd {
			t.Errorf("unexpected diagnostic range %s–%s (want end %s)", start, end, wantEnd)
		}
		delete(remaining, start)
		assertSafeDiagnostic(t, diagnostic)
	}
	if len(remaining) != 0 {
		t.Errorf("missing diagnostics for %v", remaining)
	}
}

func fixturePosition(fset *token.FileSet, pos token.Pos) token.Position {
	p := fset.Position(pos)
	return token.Position{Filename: filepath.Base(p.Filename), Line: p.Line, Column: p.Column}
}

func assertSafeDiagnostic(t *testing.T, diagnostic analysis.Diagnostic) {
	t.Helper()
	if diagnostic.Category != "CHR0053" || diagnostic.Message != "CHR0053: choose either pii or encrypted for this field; remove the other directive" {
		t.Error("expected fixed CHR0053 diagnostic and remedy")
	}
	if strings.Contains(diagnostic.Message, "CANARY_CLASSIFICATION_DETAILS") || len(diagnostic.SuggestedFixes) != 0 {
		t.Error("diagnostic must not reveal details or choose which protection to remove")
	}
}

func TestRuntimeProtectionWitness(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"registered order", definitionError[consumer.Conflict]()},
		{"reverse order", definitionError[consumer.Reversed]()},
		{"generated-style", definitionError[consumer.GeneratedConflict]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, sdk.ErrInvalidConfiguration) {
				t.Fatal("expected an invalid configuration")
			}
			var declaration *declarations.DeclarationError
			if !errors.As(tc.err, &declaration) || declaration.Directive != "protection" {
				t.Fatal("expected a protection declaration error")
			}
		})
	}
	if err := definitionError[consumer.Valid](); err != nil {
		t.Fatalf("valid protection declarations rejected: %v", err)
	}
}

func definitionError[T any]() error {
	_, err := events.Define[T]()
	return err
}
