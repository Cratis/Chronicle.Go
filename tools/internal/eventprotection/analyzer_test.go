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

const (
	conflictMessage  = "CHR0053: choose either pii or encrypted for this field; remove the other directive"
	piiMessage       = "CHR0034: remove pii from this event source identity; protect a separate field instead"
	encryptedMessage = "CHR0052: remove encrypted from this event source identity; protect a separate field instead"
)

type tagSpan struct {
	file              string
	line, column, end int
	category, message string
}

func (s tagSpan) start() token.Position {
	return token.Position{Filename: s.file, Line: s.line, Column: s.column}
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
			assertTagDiagnostics(t, analyzeConsumer(t, dir), expectedSpans(tc.flags != ""))
		})
	}
}

func analyzeConsumer(t *testing.T, dir string) *analysistest.Result {
	t.Helper()
	results := analysistest.Run(t, dir, eventprotection.Analyzer, "./testdata/consumer")
	if len(results) != 1 {
		t.Fatalf("got %d analyzed packages, want 1", len(results))
	}
	return results[0]
}

func expectedSpans(tagged bool) []tagSpan {
	spans := []tagSpan{
		{"events.go", 12, 15, 85, "CHR0053", conflictMessage},
		{"events.go", 16, 15, 69, "CHR0053", conflictMessage},
		{"zz_generated.go", 11, 15, 42, "CHR0053", conflictMessage},
		{"identities.go", 12, 22, 39, "CHR0034", piiMessage},
		{"identities.go", 18, 14, 107, "CHR0052", encryptedMessage},
	}
	if tagged {
		spans = append(spans,
			tagSpan{"excluded.go", 11, 15, 42, "CHR0053", conflictMessage},
			tagSpan{"excluded.go", 17, 22, 57, "CHR0034", piiMessage})
	}
	return spans
}

func assertTagDiagnostics(t *testing.T, result *analysistest.Result, spans []tagSpan) {
	t.Helper()
	diagnostics := result.Action.Diagnostics
	if len(diagnostics) != len(spans) {
		t.Fatalf("got %d diagnostics, want %d", len(diagnostics), len(spans))
	}
	remaining := make(map[token.Position]tagSpan, len(spans))
	for _, span := range spans {
		remaining[span.start()] = span
	}
	fset := result.Action.Package.Fset
	for _, diagnostic := range diagnostics {
		start := fixturePosition(fset, diagnostic.Pos)
		span, ok := remaining[start]
		if !ok {
			t.Errorf("unexpected diagnostic at %s", start)
			continue
		}
		delete(remaining, start)
		assertTagDiagnostic(t, fset, diagnostic, span)
	}
	for _, span := range spans {
		if _, missing := remaining[span.start()]; missing {
			t.Errorf("missing diagnostic at %s", span.start())
		}
	}
}

func fixturePosition(fset *token.FileSet, pos token.Pos) token.Position {
	p := fset.Position(pos)
	return token.Position{Filename: filepath.Base(p.Filename), Line: p.Line, Column: p.Column}
}

func assertTagDiagnostic(t *testing.T, fset *token.FileSet, diagnostic analysis.Diagnostic, span tagSpan) {
	t.Helper()
	wantEnd := span.start()
	wantEnd.Column = span.end
	if end := fixturePosition(fset, diagnostic.End); end != wantEnd {
		t.Errorf("unexpected diagnostic end %s (want %s)", end, wantEnd)
	}
	if diagnostic.Category != span.category || diagnostic.Message != span.message {
		t.Errorf("expected fixed %s diagnostic and remedy at %s", span.category, span.start())
	}
	if strings.Contains(diagnostic.Message, "CANARY_") || len(diagnostic.SuggestedFixes) != 0 {
		t.Error("diagnostic must not reveal details or suggest a fix")
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

func TestRuntimeIdentityWitness(t *testing.T) {
	const identityError = "event-source identity cannot be protected"
	for _, tc := range []struct {
		name, reason string
		err          error
	}{
		{"PII identity", identityError, definitionError[consumer.PIIIdentity]()},
		{"encrypted identity alias", identityError, definitionError[consumer.EncryptedIdentity]()},
		{"combined identity", "PII and confidentiality cannot protect the same value", definitionError[consumer.CombinedIdentity]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, sdk.ErrInvalidConfiguration) {
				t.Fatal("expected an invalid configuration")
			}
			var declaration *declarations.DeclarationError
			if !errors.As(tc.err, &declaration) || declaration.Directive != "protection" {
				t.Fatal("expected a protection declaration error")
			}
			if declaration.Message != tc.reason {
				t.Errorf("expected protection rejection reason %q", tc.reason)
			}
		})
	}
	if err := definitionError[consumer.ValidIdentity](); err != nil {
		t.Fatal("valid identity and protected string-like fields rejected")
	}
}

func definitionError[T any]() error {
	_, err := events.Define[T]()
	return err
}
