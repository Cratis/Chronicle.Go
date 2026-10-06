// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestConnectionDocumentationMatchesExecutableExamples(t *testing.T) {
	source, err := os.ReadFile("example_connection_test.go")
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "example_connection_test.go", source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	extract := func(node ast.Node) string {
		return string(source[set.Position(node.Pos()).Offset:set.Position(node.End()).Offset])
	}
	var hook, strategy string
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			if node.Name.Name == "ExampleWithOnConnected" {
				for _, statement := range node.Body.List[:3] {
					hook += extract(statement) + "\n"
				}
			}
			if node.Recv != nil && node.Name.Name == "Next" {
				strategy += extract(node) + "\n"
			}
		case *ast.GenDecl:
			if node.Tok == token.TYPE && len(node.Specs) == 1 {
				if typ, ok := node.Specs[0].(*ast.TypeSpec); ok && typ.Name.Name == "preferLastServer" {
					strategy += extract(node) + "\n"
				}
			}
		}
	}
	for _, tc := range []struct {
		path, code string
		function   bool
	}{
		{"Documentation/connection-strings/lifecycle.md", hook, true},
		{"Documentation/connection-strings/index.md", strategy, false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			if tc.code == "" {
				t.Fatal("compiled example was not found")
			}
			page, err := os.ReadFile(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			_, block, found := strings.Cut(string(page), "```go\n")
			if !found {
				t.Fatal("missing Go excerpt")
			}
			block, _, found = strings.Cut(block, "\n```")
			if !found {
				t.Fatal("unclosed Go excerpt")
			}
			wrap := func(code string) string {
				if tc.function {
					return "package sample\nfunc example() {\n" + code + "\n}\n"
				}
				return "package sample\n" + code
			}
			if !reflect.DeepEqual(goTokens(t, wrap(block)), goTokens(t, wrap(tc.code))) {
				t.Fatal("connection documentation drifted from executable example")
			}
		})
	}
}
