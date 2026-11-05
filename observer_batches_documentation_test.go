// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"go/scanner"
	"go/token"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestObserverBatchSnippetsMatchCompiledExample(t *testing.T) {
	source, err := os.ReadFile("example_observer_batches_test.go")
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile("Documentation/observer-batches.md")
	if err != nil {
		t.Fatal(err)
	}
	example := snippetTokens(t, string(source))
	blocks := strings.Split(string(page), "```go\n")[1:]
	if len(blocks) == 0 {
		t.Fatal("missing Go snippets")
	}
	for i, block := range blocks {
		block, _, _ = strings.Cut(block, "\n```")
		want := snippetTokens(t, block)
		found := false
		for start := range example {
			if len(example)-start >= len(want) && slices.Equal(example[start:start+len(want)], want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("snippet %d drifted from example_observer_batches_test.go", i+1)
		}
	}
}

// snippetTokens lexes Go without parsing, so excerpts need no package clause.
func snippetTokens(t *testing.T, source string) []string {
	t.Helper()
	var lexer scanner.Scanner
	lexer.Init(token.NewFileSet().AddFile("snippet.go", -1, len(source)), []byte(source), func(_ token.Position, message string) { t.Error(message) }, 0)
	var result []string
	for {
		_, kind, literal := lexer.Scan()
		if kind == token.EOF {
			return result
		}
		if kind != token.SEMICOLON {
			result = append(result, kind.String()+":"+literal)
		}
	}
}
