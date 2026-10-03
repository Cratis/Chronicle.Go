// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type WorkItem struct{}
type IssueCreated struct {
	Title string `json:"title"`
}
type PullRequestCreated struct {
	WorkItemID string `json:"workItemId"`
}
type TitleChanged struct {
	Title string `json:"title"`
}
type BacklogItem struct {
	ID      string    `json:"id" chronicle:"key"`
	Title   string    `json:"title" chronicle:"value(TitleChanged,value=\"local\")"`
	Updated time.Time `json:"updated" chronicle:"every(context=occurred)"`
	Source  string    `json:"source" chronicle:"all(context=eventSourceId)"`
}
type PullRequestItem struct {
	ID    string `json:"id" chronicle:"key"`
	Title string `json:"title"`
}
type WorkItemShared struct {
	Title string `json:"title" chronicle:"set(TitleChanged)"`
}
type FluentBacklog struct {
	ID      string    `json:"id"`
	Title   string    `json:"title"`
	Updated time.Time `json:"updated"`
	Source  string    `json:"source"`
}
type FluentShared struct {
	Title string `json:"title"`
}

// Hand-derived from ModelBoundProjectionBuilder.BuildVariant and
// VariantReclassifier at Chronicle 2e31b0d, not captured from a running C# client.
func TestVariantFrontEndsMatchCSharpGolden(t *testing.T) {
	created := mustEvent[IssueCreated](t)
	entered := mustEvent[PullRequestCreated](t)
	renamed := mustEvent[TitleChanged](t)
	catalog, err := events.NewCatalog(created.Descriptor(), entered.Descriptor(), renamed.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	global, err := projections.Global[WorkItemShared](projections.GlobalFor[WorkItem]())
	if err != nil {
		t.Fatal(err)
	}
	bound := projections.ModelBound(mustModel[BacklogItem](t, readmodels.WithIdentifier("Example.Backlog")), projections.WithIdentifier("Example.BacklogProjection"), projections.VariantOf[WorkItem](), projections.EntersOn(created))
	sibling := projections.ModelBound(mustModel[PullRequestItem](t), projections.VariantOf[WorkItem](), projections.EntersOn(entered, projections.UsingKey(projections.Path[PullRequestCreated, string]("workItemId"))))
	fluent := projections.NewBuilder("Example.BacklogProjection", mustModel[FluentBacklog](t, readmodels.WithIdentifier("Example.Backlog")), projections.VariantOf[WorkItem](), projections.VariantKey(projections.Path[FluentBacklog, string]("id")), projections.EntersOn(created))
	projections.From(fluent, renamed, func(b *projections.FromBuilder[FluentBacklog, TitleChanged]) {
		projections.Value(b, projections.Path[FluentBacklog, string]("title"), "local")
	})
	projections.Every(fluent, func(b *projections.EveryBuilder[FluentBacklog]) {
		projections.EveryContext(b, projections.Path[FluentBacklog, time.Time]("updated"), "occurred")
	})
	projections.All(fluent, func(b *projections.EveryBuilder[FluentBacklog]) {
		projections.EveryContext(b, projections.Path[FluentBacklog, string]("source"), "eventSourceId")
	})
	fd, err := fluent.Build()
	if err != nil {
		t.Fatal(err)
	}
	fg := projections.NewBuilder("", mustModel[FluentShared](t), projections.GlobalFor[WorkItem]())
	projections.From(fg, renamed, func(b *projections.FromBuilder[FluentShared, TitleChanged]) {
		projections.Map(b, projections.Path[FluentShared, string]("title"), projections.Path[TitleChanged, string]("title"))
	})
	fgd, err := fg.Build()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/variant.json")
	if err != nil {
		t.Fatal(err)
	}
	want := &contracts.ProjectionDefinition{}
	if err := protojson.Unmarshal(data, want); err != nil {
		t.Fatal(err)
	}
	var hash [32]byte
	for i, group := range [][]projections.Declaration{{bound, sibling, global}, {fd, sibling, fgd}, {global, sibling, bound}} {
		definitions, err := projections.CompileGroup(group, catalog)
		if err != nil {
			t.Fatal(err)
		}
		if len(definitions) != 2 {
			t.Fatal("global registered as projection")
		}
		got := definitions[0]
		if !proto.Equal(got.KernelDefinition(), want) {
			t.Fatalf("front end %d: got %s", i, protojson.Format(got.KernelDefinition()))
		}
		current, err := got.Hash()
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			hash = current
		} else if hash != current {
			t.Fatal("hash depends on frontend or declaration order")
		}
		if len(got.Diagnostics()) == 0 {
			t.Fatal("missing global overwrite diagnostic")
		}
		got.KernelDefinition().Join[0].Value.Properties["title"] = "mutated"
		if after, err := got.Hash(); err != nil || after != hash {
			t.Fatal("definition was mutable")
		}
		other := definitions[1].KernelDefinition()
		if other.From[0].Value.Key != "workItemId" || other.RemovedWith[0].Value.Key != "$eventSourceId" {
			t.Fatal("redirected entry or C# removal key changed")
		}
	}
}

func TestVariantValidationAndGlobalTargets(t *testing.T) {
	created := mustEvent[IssueCreated](t)
	renamed := mustEvent[TitleChanged](t)
	catalog, _ := events.NewCatalog(created.Descriptor(), renamed.Descriptor())
	type MissingKey struct {
		ID    string
		Title string `json:"title"`
	}
	type MissingTitle struct {
		ID string `json:"id" chronicle:"key"`
	}
	for name, declaration := range map[string]projections.Declaration{
		"missing enters":         projections.ModelBound(mustModel[PullRequestItem](t), projections.VariantOf[WorkItem]()),
		"missing key":            projections.ModelBound(mustModel[MissingKey](t), projections.VariantOf[WorkItem](), projections.EntersOn(created)),
		"enters without variant": projections.ModelBound(mustModel[PullRequestItem](t), projections.EntersOn(created)),
		"duplicate enters":       projections.ModelBound(mustModel[PullRequestItem](t), projections.VariantOf[WorkItem](), projections.EntersOn(created), projections.EntersOn(created)),
		"parent key":             projections.ModelBound(mustModel[PullRequestItem](t), projections.VariantOf[WorkItem](), projections.EntersOn(created, projections.UsingParentKey(projections.Path[IssueCreated, string]("title")))),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := projections.Compile(declaration, catalog); err == nil {
				t.Fatal("invalid variant accepted")
			}
		})
	}
	global, err := projections.Global[WorkItemShared](projections.GlobalFor[WorkItem]())
	if err != nil {
		t.Fatal(err)
	}
	_, err = projections.CompileGroup([]projections.Declaration{global, projections.ModelBound(mustModel[MissingTitle](t), projections.VariantOf[WorkItem](), projections.EntersOn(created))}, catalog)
	var target *projections.GlobalHandlerPropertyNotOnVariant
	var declaration *projections.DeclarationError
	if !errors.As(err, &target) || !errors.As(err, &declaration) || target.Property != "title" || target.Variant != reflect.TypeFor[MissingTitle]() {
		t.Fatalf("target failure: %v", err)
	}
}

type SharedTitleAndExtraChanged struct {
	Title string `json:"title"`
	Extra string `json:"extra"`
}
type SharedTitleAndExtra struct {
	Title string `json:"title" chronicle:"set(SharedTitleAndExtraChanged)"`
	Extra string `json:"extra"`
}

// C# MergeGlobalHandlers copies From.Properties, never AutoMap matches from a
// class-level FromEvent (ModelBoundProjectionBuilder at Chronicle 2e31b0d).
func TestGlobalDoesNotMaterializeAutoMapMatches(t *testing.T) {
	created := mustEvent[IssueCreated](t)
	changed := mustEvent[SharedTitleAndExtraChanged](t)
	catalog, err := events.NewCatalog(created.Descriptor(), changed.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	global, err := projections.Global[SharedTitleAndExtra](projections.GlobalFor[WorkItem](), projections.FromEvent(changed))
	if err != nil {
		t.Fatal(err)
	}
	variant := projections.ModelBound(mustModel[PullRequestItem](t), projections.VariantOf[WorkItem](), projections.EntersOn(created))
	defs, err := projections.CompileGroup([]projections.Declaration{variant, global}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	wire := defs[0].KernelDefinition()
	if len(wire.Join) != 1 || wire.Join[0].Key.Id != "SharedTitleAndExtraChanged" || len(wire.Join[0].Value.Properties) != 1 || wire.Join[0].Value.Properties["title"] != "title" {
		t.Fatalf("global copied more than explicit properties: %v", wire.Join)
	}
}

func TestFluentGlobalFromWithoutPropertiesFailsAtBuild(t *testing.T) {
	changed := mustEvent[TitleChanged](t)
	builder := projections.NewBuilder("", mustModel[FluentShared](t), projections.GlobalFor[WorkItem](), projections.AutoMap())
	projections.From(builder, changed, nil)
	_, err := builder.Build()
	var empty *projections.GlobalFromHasNoProperties
	var declaration *projections.DeclarationError
	if !errors.As(err, &empty) || !errors.As(err, &declaration) || empty.Global != reflect.TypeFor[FluentShared]() || empty.Event != changed.Ref() {
		t.Fatalf("missing typed global From failure: %v", err)
	}
}

func TestGlobalCopiesFromPropertiesOnly(t *testing.T) {
	created := mustEvent[IssueCreated](t)
	renamed := mustEvent[TitleChanged](t)
	catalog, _ := events.NewCatalog(created.Descriptor(), renamed.Descriptor())
	global := projections.NewBuilder("", mustModel[FluentShared](t), projections.GlobalFor[WorkItem](), projections.NoAutoMap(), projections.NotRewindable(), projections.RemovedWith(created))
	projections.From(global, renamed, func(b *projections.FromBuilder[FluentShared, TitleChanged]) {
		projections.Map(b, projections.Path[FluentShared, string]("title"), projections.Path[TitleChanged, string]("title"))
	}, projections.UsingConstantKey("ignored"))
	projections.Join(global, created, projections.Path[FluentShared, string]("title"), nil)
	projections.All(global, func(b *projections.EveryBuilder[FluentShared]) {
		projections.EveryMap(b, projections.Path[FluentShared, string]("title"), "title")
	})
	g, err := global.Build()
	if err != nil {
		t.Fatal(err)
	}
	defs, err := projections.CompileGroup([]projections.Declaration{g, projections.ModelBound(mustModel[PullRequestItem](t), projections.VariantOf[WorkItem](), projections.EntersOn(created))}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	d := defs[0].KernelDefinition()
	if len(d.Join) != 1 || d.Join[0].Value.Key != "$eventSourceId" || len(d.All.Properties) > 0 || d.SubscribesToAllEvents || len(d.RemovedWith) > 0 || !d.IsRewindable || d.AutoMap != contracts.AutoMap_Enabled {
		t.Fatalf("global leaked non-property state: %s", protojson.Format(d))
	}
}
