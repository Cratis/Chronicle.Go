// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/projections"
)

func TestScoreboardRegistryCompiles(t *testing.T) {
	registry, err := scoreboardDeclarations()
	if err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	artifacts, err := client.Artifacts("scoreboards")
	if err != nil {
		t.Fatal(err)
	}
	definitions := map[string]*contracts.ProjectionDefinition{}
	for _, projection := range artifacts.Projections {
		definitions[projection.Identifier()] = projection.KernelDefinition()
	}
	fluent, bound := definitions["scoreboards"], definitions["bound-scoreboards"]
	if fluent == nil || bound == nil {
		t.Fatalf("projections: %v", definitions)
	}
	var scored map[string]string
	for _, from := range fluent.From {
		if len(from.Value.Properties) > 1 {
			scored = from.Value.Properties
		}
	}
	if scored["points.$eventContent.player"] != "$add(amount)" || scored["startedBy"] != "$setOnce($causedBy(subject))" {
		t.Fatalf("scored: %v", scored)
	}
	if fluent.Children["rounds"].GetOrdering().GetSortBy() != "number" || fluent.Children["latestFirst"].GetOrdering().GetPlacement() != contracts.ChildrenPlacement_Prepend {
		t.Fatalf("children: %v", fluent.Children)
	}
	if bound.Children["rounds"].GetOrdering().GetDirection() != contracts.SortDirection_Descending {
		t.Fatalf("bound children: %v", bound.Children)
	}
}
