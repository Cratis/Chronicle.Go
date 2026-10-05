// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/seeding"
)

type ReferenceItemAdded struct{ Name string }
type ReferenceSeeds struct{}

func (ReferenceSeeds) Seed(builder *seeding.Builder) error {
	seeding.For(builder, "item-1", ReferenceItemAdded{Name: "Notebook"})
	builder.ForNamespace("demo").ForEventSource("item-2", ReferenceItemAdded{Name: "Sample"})
	return nil
}

func ExampleRegisterSeeder() {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ReferenceItemAdded](registry); err != nil {
		panic(err)
	}
	if err := chronicle.RegisterSeeder(registry, ReferenceSeeds{}); err != nil {
		panic(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}
	}()
	fmt.Println("Seeds prepared without connecting")
	// EventStore(ctx, "shop") registers observers, then sends the seed snapshot.
	// Output: Seeds prepared without connecting
}

func TestSeedingDocumentationMatchesCompiledExample(t *testing.T) {
	source, err := os.ReadFile("examples/seeding/main.go")
	if err != nil {
		t.Fatal(err)
	}
	_, snippet, found := strings.Cut(string(source), "// begin-seeding-example\n")
	if !found {
		t.Fatal("example start missing")
	}
	snippet, _, found = strings.Cut(snippet, "// end-seeding-example")
	if !found {
		t.Fatal("example end missing")
	}
	page, err := os.ReadFile("Documentation/events/seeding.md")
	if err != nil {
		t.Fatal(err)
	}
	_, documented, found := strings.Cut(string(page), "```go\n")
	if !found {
		t.Fatal("snippet missing")
	}
	documented, _, found = strings.Cut(documented, "\n```")
	if !found {
		t.Fatal("unclosed snippet")
	}
	if !reflect.DeepEqual(goTokens(t, "package main\n"+snippet), goTokens(t, "package main\n"+documented)) {
		t.Fatal("seeding docs drifted from compiled sample")
	}
}
