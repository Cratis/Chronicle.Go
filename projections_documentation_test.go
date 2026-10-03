// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestOrderProjectionSnippetsMatchCompiledExample(t *testing.T) {
	checkProjectionSnippets(t, "examples/projections/orders.go", "Documentation/projections/orders.md", []string{"orders-events", "orders-model-bound", "orders-fluent"})
}

func TestVariantProjectionSnippetsMatchCompiledExample(t *testing.T) {
	checkProjectionSnippets(t, "examples/projections/variants.go", "Documentation/projections/variants.md", []string{"variant-models", "variant-model-bound", "variant-fluent"})
}

func TestProjectionSnippetsMatchCompiledExample(t *testing.T) {
	checkProjectionSnippets(t, "examples/projections/main.go", "Documentation/projections/index.md", []string{"model-bound", "fluent"})
}

func checkProjectionSnippets(t *testing.T, sourcePath, pagePath string, names []string) {
	t.Helper()
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile(pagePath)
	if err != nil {
		t.Fatal(err)
	}
	remaining := string(page)
	for _, name := range names {
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
