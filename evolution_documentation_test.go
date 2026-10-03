// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestEvolutionSnippetMatchesCompiledExample(t *testing.T) {
	source, err := os.ReadFile("examples/evolution/main.go")
	if err != nil {
		t.Fatal(err)
	}
	_, function, ok := strings.Cut(string(source), "func registry()")
	if !ok {
		t.Fatal("example function missing")
	}
	function, _, ok = strings.Cut(function, "\nfunc run()")
	if !ok {
		t.Fatal("example boundary missing")
	}
	page, err := os.ReadFile("Documentation/events/evolution.md")
	if err != nil {
		t.Fatal(err)
	}
	_, snippet, ok := strings.Cut(string(page), "```go\n")
	if !ok {
		t.Fatal("snippet missing")
	}
	snippet, _, ok = strings.Cut(snippet, "\n```")
	if !ok {
		t.Fatal("snippet closing fence missing")
	}
	if !reflect.DeepEqual(goTokens(t, "package main\n"+snippet), goTokens(t, "package main\nfunc registry()"+function)) {
		t.Fatal("evolution docs drifted from compiled example")
	}
}
