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
)

type exampleGateway struct{}

func (exampleGateway) Reserve(context.Context, string, string) error { return nil }

func TestConventionRegistration(t *testing.T) {
	registry := chronicle.NewRegistry()
	if err := registerConfirmOrders(registry, exampleGateway{}); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConventionSnippetMatchesCompiledSource(t *testing.T) {
	source, err := os.ReadFile("convention.go")
	if err != nil {
		t.Fatal(err)
	}
	_, after, ok := strings.Cut(string(source), "// begin-convention\n")
	if !ok {
		t.Fatal("missing snippet start")
	}
	snippet, _, ok := strings.Cut(after, "// end-convention")
	if !ok {
		t.Fatal("missing snippet end")
	}
	document, err := os.ReadFile("../../Documentation/reactors.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(document), strings.ReplaceAll(strings.TrimSpace(snippet), "\t", "    ")) {
		t.Fatal("reactor convention snippet has drifted")
	}
}

func TestPlainGoExampleHasNoDependencyInjectionDependency(t *testing.T) {
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
			t.Fatalf("plain Go example imports %s", dependency)
		}
	}
}
