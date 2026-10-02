// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestGettingStartedSnippetsMatchCompiledExample(t *testing.T) {
	source, err := os.ReadFile("examples/getting-started/main.go")
	if err != nil {
		t.Fatal(err)
	}
	want := goTokens(t, string(source))
	for _, path := range []string{"README.md", "Documentation/clients/go/getting-started.md"} {
		t.Run(path, func(t *testing.T) {
			page, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, block, found := strings.Cut(string(page), "```go\n")
			if !found {
				t.Fatal("missing executable example")
			}
			block, _, found = strings.Cut(block, "\n```")
			if !found {
				t.Fatal("unclosed example")
			}
			if !reflect.DeepEqual(goTokens(t, block), want) {
				t.Fatal("documentation drifted from the compiled getting-started example")
			}
		})
	}
}

func goTokens(t *testing.T, source string) []string {
	t.Helper()
	set := token.NewFileSet()
	if _, err := parser.ParseFile(set, "example.go", source, parser.SkipObjectResolution); err != nil {
		t.Fatal(err)
	}
	var lexer scanner.Scanner
	lexer.Init(set.AddFile("tokens.go", -1, len(source)), []byte(source), func(_ token.Position, message string) { t.Error(message) }, 0)
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
