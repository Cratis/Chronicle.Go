// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestProjectionSnippetsMatchCompiledExample(t *testing.T) {
	source, err := os.ReadFile("examples/projections/main.go")
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile("Documentation/projections/index.md")
	if err != nil {
		t.Fatal(err)
	}
	remaining := string(page)
	for _, name := range []string{"model-bound", "fluent"} {
		_, snippet, found := strings.Cut(remaining, "```go\n")
		if !found {
			t.Fatal("missing snippet", name)
		}
		snippet, remaining, found = strings.Cut(snippet, "\n```")
		if !found {
			t.Fatal("unclosed snippet", name)
		}
		_, example, found := strings.Cut(string(source), "// "+name+":start\n")
		if !found {
			t.Fatal("missing source marker", name)
		}
		example, _, found = strings.Cut(example, "// "+name+":end")
		if !found {
			t.Fatal("missing source end", name)
		}
		if !reflect.DeepEqual(goTokens(t, "package main\n"+snippet), goTokens(t, "package main\n"+example)) {
			t.Fatal("projection snippet drifted:", name)
		}
	}
}
