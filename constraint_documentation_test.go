// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestConstraintSnippetMatchesCompiledExample(t *testing.T) {
	source, err := os.ReadFile("examples/constraints/main.go")
	if err != nil {
		t.Fatal(err)
	}
	_, declaration, found := strings.Cut(string(source), "func declarations()")
	if !found {
		t.Fatal("example declaration missing")
	}
	declaration, _, found = strings.Cut(declaration, "\nfunc run(")
	if !found {
		t.Fatal("example declaration boundary missing")
	}
	page, err := os.ReadFile("Documentation/events/constraints.md")
	if err != nil {
		t.Fatal(err)
	}
	_, snippet, found := strings.Cut(string(page), "```go\n")
	if !found {
		t.Fatal("constraint snippet missing")
	}
	snippet, _, found = strings.Cut(snippet, "\n```")
	if !found {
		t.Fatal("unclosed constraint snippet")
	}
	if !reflect.DeepEqual(goTokens(t, "package main\n"+snippet), goTokens(t, "package main\nfunc declarations()"+declaration)) {
		t.Fatal("constraint docs drifted from the compiled example")
	}
}
