// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

func TestBothAuthoringPathsCompileAndFold(t *testing.T) {
	registry := chronicle.NewRegistry()
	if _, err := registerPlain(registry); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	registry = chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[AmountChanged](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[AccountDeleted](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[Balance](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := registerConvention(registry, model); err != nil {
		t.Fatal(err)
	}
	client, err = chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	var state *Balance
	for _, amount := range []int{-10, 8} {
		state, err = fold(t.Context(), AmountChanged{amount}, state, events.Context{SourceID: "account"})
		if err != nil {
			t.Fatal(err)
		}
	}
	if state.Amount != -6 {
		t.Fatal(state)
	}
}
func TestDocumentationMatchesCompiledSnippets(t *testing.T) {
	document, err := os.ReadFile("../../Documentation/reducers.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, snippet := range []struct{ file, marker string }{{"main.go", "plain"}, {"convention.go", "convention"}} {
		source, err := os.ReadFile(snippet.file)
		if err != nil {
			t.Fatal(err)
		}
		_, after, ok := strings.Cut(string(source), "// begin-"+snippet.marker+"\n")
		if !ok {
			t.Fatal("missing snippet start")
		}
		block, _, ok := strings.Cut(after, "// end-"+snippet.marker)
		if !ok {
			t.Fatal("missing snippet end")
		}
		lines := strings.Split(strings.TrimSpace(block), "\n")
		// The registration excerpt lives inside a function, unlike the type excerpt.
		if snippet.marker == "plain" {
			for i := 1; i < len(lines); i++ {
				lines[i] = strings.TrimPrefix(lines[i], "\t")
			}
		}
		expected := strings.ReplaceAll(strings.Join(lines, "\n"), "\t", "    ")
		if !strings.Contains(string(document), expected) {
			t.Fatalf("%s snippet drifted", snippet.marker)
		}
	}
}
func TestPlainGoReducerHasNoDependencyInjectionDependency(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "list", "-deps", ".")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if dependency == "github.com/cratis/fundamentals.go/dependencyinjection" || strings.HasPrefix(dependency, "github.com/cratis/fundamentals.go/dependencyinjection/") {
			t.Fatalf("plain Go reducer imports %s", dependency)
		}
	}
}
