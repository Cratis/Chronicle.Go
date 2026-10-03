// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestSiblingRemovalsDoNotChangeVariantSource(t *testing.T) {
	created := mustEvent[IssueCreated](t)
	entered := mustEvent[PullRequestCreated](t, events.WithSourceStore("github"))
	renamed := mustEvent[TitleChanged](t)
	catalog, err := events.NewCatalog(created.Descriptor(), entered.Descriptor(), renamed.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	backlog := projections.ModelBound(mustModel[BacklogItem](t), projections.VariantOf[WorkItem](), projections.EntersOn(created))
	pullRequest := projections.ModelBound(mustModel[PullRequestItem](t), projections.VariantOf[WorkItem](), projections.EntersOn(entered))
	for _, group := range [][]projections.Declaration{{backlog, pullRequest}, {pullRequest, backlog}} {
		definitions, err := projections.CompileGroup(group, catalog, "local")
		if err != nil {
			t.Fatal(err)
		}
		if len(definitions) != 2 {
			t.Fatalf("definitions = %d, want 2", len(definitions))
		}
		for _, definition := range definitions {
			want := events.EventLog
			sibling := "PullRequestCreated"
			if definition.Model().GoType() == pullRequest.Model().GoType() {
				want = "inbox-github"
				sibling = "IssueCreated"
			}
			if definition.EventSequence() != want || definition.Model().EventSequence() != want {
				t.Fatalf("%s source = %s, want %s", definition.Identifier(), definition.EventSequence(), want)
			}
			wire := definition.KernelDefinition()
			if len(wire.RemovedWith) != 1 || wire.RemovedWith[0].Key.Id != sibling {
				t.Fatalf("sibling removal lost: %v", wire.RemovedWith)
			}
		}
	}
}

func TestSourceInferenceExplicitSequenceAndCompatibility(t *testing.T) {
	created := mustEvent[IssueCreated](t, events.WithSourceStore("origin"))
	renamed := mustEvent[TitleChanged](t, events.WithSourceStore("other"))
	catalog, _ := events.NewCatalog(created.Descriptor(), renamed.Descriptor())
	model := mustModel[PullRequestItem](t)
	for _, store := range []string{"origin", "consumer", ""} {
		defs, err := projections.CompileGroup([]projections.Declaration{projections.ModelBound(model, projections.FromEvent(created))}, catalog, store)
		if err != nil {
			t.Fatal(err)
		}
		want := events.SequenceID("inbox-origin")
		if store == "origin" {
			want = events.EventLog
		}
		if defs[0].EventSequence() != want || defs[0].Model().EventSequence() != want {
			t.Fatal("inference/binding mismatch")
		}
		rebound, err := defs[0].ForStore("origin")
		if err != nil || rebound.EventSequence() != events.EventLog {
			t.Fatal("store template did not resolve")
		}
		if store != "origin" && defs[0].EventSequence() != want {
			t.Fatal("ForStore mutated snapshot")
		}
	}
	mixed := projections.ModelBound(model, projections.FromEvent(created), projections.RemovedWith(renamed))
	if _, err := projections.Compile(mixed, catalog); err == nil {
		t.Fatal("incompatible removal origin accepted")
	}
	explicit := projections.ModelBound(model, projections.FromEvent(created), projections.RemovedWith(renamed), projections.WithEventLog())
	if def, err := projections.Compile(explicit, catalog); err != nil || def.EventSequence() != events.EventLog {
		t.Fatalf("explicit sequence did not win: %v", err)
	}
	modelOverride := mustModel[PullRequestItem](t, readmodels.WithEventSequence("custom"))
	if def, err := projections.Compile(projections.ModelBound(modelOverride, projections.FromEvent(created)), catalog); err != nil || def.EventSequence() != "custom" {
		t.Fatalf("model sequence lost: %v", err)
	}
	fluent := projections.NewBuilder("", model)
	projections.From(fluent, created, nil)
	d, err := fluent.Build()
	if err != nil {
		t.Fatal(err)
	}
	defs, err := projections.CompileGroup([]projections.Declaration{d}, catalog, "origin")
	if err != nil || defs[0].EventSequence() != events.EventLog {
		t.Fatalf("fluent local normalization: %v", err)
	}
}
