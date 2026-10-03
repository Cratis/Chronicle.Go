// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
)

type configuredOrderOpened struct{ Name string }
type configuredOrderView struct{ ID, Name string }
type projectionSettings struct{ Label string }

func ExampleRegisterProjectionFactory() {
	registry := chronicle.NewRegistry()
	opened, err := chronicle.RegisterEvent[configuredOrderOpened](registry)
	if err != nil {
		panic(err)
	}
	model, err := chronicle.RegisterReadModel[configuredOrderView](registry)
	if err != nil {
		panic(err)
	}
	// definition-factory:start
	settings := projectionSettings{Label: "orders"}
	err = chronicle.RegisterProjectionFactory(registry, "orders", model.Descriptor(),
		func() projectionSettings { return settings },
		func(_ context.Context, config projectionSettings) (projections.Declaration, error) {
			return projections.ModelBound(model,
				projections.WithIdentifier("orders"),
				projections.FromEvent(opened),
				projections.WithLabels(config.Label)), nil
		})
	if err != nil {
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
	// definition-factory:end
	artifacts, err := client.Artifacts("orders-store")
	if err != nil {
		panic(err)
	}
	fmt.Println(artifacts.Projections[0].KernelDefinition().Tags[0])
	// Output: orders
}

func TestDefinitionFactorySnippetMatchesCompiledExample(t *testing.T) {
	source, err := os.ReadFile("example_definition_factories_test.go")
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile("Documentation/definition-factories.md")
	if err != nil {
		t.Fatal(err)
	}
	extract := func(text, start, end string) string {
		_, snippet, found := strings.Cut(text, start)
		if !found {
			t.Fatal("missing snippet start", start)
		}
		snippet, _, found = strings.Cut(snippet, end)
		if !found {
			t.Fatal("missing snippet end", end)
		}
		// This excerpt is a function body, unlike the declaration snippets.
		return "package main\nfunc example() {\n" + snippet + "\n}"
	}
	actual := extract(string(page), "```go\n", "\n```")
	expected := extract(string(source), "// definition-factory:start\n", "// definition-factory:end")
	if !reflect.DeepEqual(goTokens(t, actual), goTokens(t, expected)) {
		t.Fatal("definition factory snippet drifted")
	}
}
