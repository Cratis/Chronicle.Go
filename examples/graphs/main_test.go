// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
)

func TestExampleGraphCompilesToTheOtherClientsDefinition(t *testing.T) {
	registry, err := declarations()
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
	artifacts, err := client.Artifacts("organization")
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts.Graphs) != 1 {
		t.Fatalf("graphs = %d", len(artifacts.Graphs))
	}
	graph := artifacts.Graphs[0].Projection().Graph
	if len(graph.Nodes) != 2 || len(graph.Edges) != 1 {
		t.Fatalf("graph = %v", graph)
	}
	if got := graph.Nodes["Group"].From["member-joined+1"].Properties["memberCount"]; got != "$increment" {
		t.Fatalf("memberCount = %q", got)
	}
	if got := graph.Edges["MEMBER_OF"].From["member-joined+1"]; got.SourceKey != "personId" || got.TargetKey != "groupId" || got.Properties["since"] != "$eventContext(occurred)" {
		t.Fatalf("edge = %v", got)
	}
	if got := artifacts.Graphs[0].ReadModel().Sink.TypeId; got != "Neo4j" {
		t.Fatalf("default sink = %q", got)
	}
}
