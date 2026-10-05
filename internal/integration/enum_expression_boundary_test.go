//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type ExpressionEntered struct {
	Status   boundaryStatus `json:"status"`
	Reserved boundaryStatus `json:"true"`
	Ready    bool
}
type ExpressionUpdated ExpressionEntered
type ExpressionChildAdded struct {
	ChildID  string
	ParentID string
	Status   boundaryStatus
	Ready    bool
}
type ExpressionDisabledChildAdded ExpressionChildAdded

type expressionLiteralView struct {
	ID       string `json:"id"`
	Status   boundaryStatus
	Reserved boundaryStatus `json:"true"`
	Ready    bool
}
type expressionExcludedView struct {
	ID       string `json:"id"`
	Status   boundaryStatus
	Reserved boundaryStatus `json:"true" chronicle:"no-auto"`
	Ready    bool
}
type expressionChild struct {
	ID     string `json:"id"`
	Status uint32
	Ready  bool
}
type expressionOwner struct {
	ID       string `json:"id"`
	Items    []expressionChild
	Disabled []expressionChild
}

// Unsafe profiles are refused by native zero-RPC tests. This live witness runs
// only admitted controls: safe automatic enum reads, explicit/excluded literal
// names, and child Inherit versus Disabled beneath a disabled root.
func TestKernelEnumExpressionAndInheritanceControls(t *testing.T) {
	f := newKernelFixture(t)
	f.storeName = chronicle.StoreName("ee-" + uuid.NewString()[:8])
	codecs, err := serialization.NewCodecs(serialization.Enum(
		serialization.EnumMember[boundaryStatus]{Name: "Zero", Value: 0},
		serialization.EnumMember[boundaryStatus]{Name: "One", Value: 1},
		serialization.EnumMember[boundaryStatus]{Name: "Negative", Value: -1},
	))
	if err != nil {
		t.Fatal(err)
	}
	r := chronicle.NewRegistry()
	entered, err := chronicle.RegisterEvent[ExpressionEntered](r, events.WithID("eee"), events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := chronicle.RegisterEvent[ExpressionUpdated](r, events.WithID("eeu"), events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	added, err := chronicle.RegisterEvent[ExpressionChildAdded](r, events.WithID("eea"), events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	disabledAdded, err := chronicle.RegisterEvent[ExpressionDisabledChildAdded](r, events.WithID("eed"), events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	literal, err := chronicle.RegisterReadModel[expressionLiteralView](r, readmodels.WithIdentifier("eel"), readmodels.WithContainerName("eel"), readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	excluded, err := chronicle.RegisterReadModel[expressionExcludedView](r, readmodels.WithIdentifier("eex"), readmodels.WithContainerName("eex"), readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := chronicle.RegisterReadModel[expressionOwner](r, readmodels.WithIdentifier("eeo"), readmodels.WithContainerName("eeo"))
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("eel", literal)
	projections.From(b, entered, func(from *projections.FromBuilder[expressionLiteralView, ExpressionEntered]) {
		projections.Value(from, projections.Path[expressionLiteralView, boundaryStatus]("true"), boundaryStatus(0))
	})
	projections.Join(b, updated, projections.Path[expressionLiteralView, string]("id"), func(from *projections.FromBuilder[expressionLiteralView, ExpressionUpdated]) {
		projections.Value(from, projections.Path[expressionLiteralView, boundaryStatus]("true"), boundaryStatus(0))
	})
	addBoundaryProjection(t, r, b)
	v := projections.NewBuilder("eex", excluded, projections.VariantOf[boundaryVariantGroup](), projections.VariantKey(projections.Path[expressionExcludedView, string]("id")), projections.EntersOn(entered))
	projections.From(v, updated, nil)
	addBoundaryProjection(t, r, v)
	o := projections.NewBuilder("eeo", owner, projections.NoAutoMap())
	projections.From(o, entered, func(from *projections.FromBuilder[expressionOwner, ExpressionEntered]) {
		projections.EventSourceID(from, projections.Path[expressionOwner, string]("id"))
	})
	addExpressionControlChild(o, added, "Items", false, 7)
	addExpressionControlChild(o, disabledAdded, "Disabled", true, 9)
	addBoundaryProjection(t, r, o)
	store, err := f.client(r, chronicle.WithNamingPolicy(serialization.PreservePropertyNames)).EventStore(f.ctx, f.storeName, chronicle.WithNamespace("ee"))
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, f.ctx, store, "subject", ExpressionEntered{Status: 1, Reserved: 1})
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), literal), "subject", func(v expressionLiteralView) bool {
		return v.ID == "subject" && v.Status == 1 && v.Reserved == 0 && !v.Ready
	})
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), excluded), "subject", func(v expressionExcludedView) bool {
		return v.ID == "subject" && v.Status == 1 && v.Reserved == 0 && !v.Ready
	})
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), owner), "subject", func(v expressionOwner) bool { return v.ID == "subject" })
	appendSuccessfully(t, f.ctx, store, "subject", ExpressionUpdated{Status: 0, Reserved: 1, Ready: true})
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), literal), "subject", func(v expressionLiteralView) bool { return v.Status == 0 && v.Reserved == 0 && v.Ready })
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), excluded), "subject", func(v expressionExcludedView) bool { return v.Status == 0 && v.Reserved == 0 && v.Ready })
	appendSuccessfully(t, f.ctx, store, "subject", ExpressionChildAdded{ChildID: "line", ParentID: "subject", Status: -1, Ready: true})
	appendSuccessfully(t, f.ctx, store, "subject", ExpressionDisabledChildAdded{ChildID: "line", ParentID: "subject", Status: -1, Ready: true})
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), owner), "subject", func(v expressionOwner) bool {
		return len(v.Items) == 1 && v.Items[0].ID == "line" && v.Items[0].Status == 7 && v.Items[0].Ready && len(v.Disabled) == 1 && v.Disabled[0].ID == "line" && v.Disabled[0].Status == 9 && !v.Disabled[0].Ready
	})
	for _, id := range []readmodels.Identifier{"eel", "eex", "eeo"} {
		raw, err := store.ReadModels().Get(f.ctx, id, "subject")
		if err != nil || !raw.Exists || !json.Valid(raw.Value) {
			t.Fatalf("raw model %s: %v", id, err)
		}
		t.Logf("kernel enum expression/inheritance model %s: %s", id, raw.Value)
	}
}

func addExpressionControlChild[E any](owner *projections.Builder[expressionOwner], event events.Type[E], path string, disabled bool, value uint32) {
	projections.Children(owner, projections.Path[expressionOwner, []expressionChild](path), func(child *projections.Builder[expressionChild]) {
		if disabled {
			child.Configure(projections.NoAutoMap())
		}
		projections.From(child, event, func(from *projections.FromBuilder[expressionChild, E]) {
			projections.Map(from, projections.Path[expressionChild, string]("id"), projections.Path[E, string]("ChildID"))
			projections.Value(from, projections.Path[expressionChild, uint32]("Status"), value)
		}, projections.UsingKey(projections.Path[E, string]("ChildID")), projections.UsingParentKey(projections.Path[E, string]("ParentID")))
	}, projections.IdentifiedBy(projections.Path[expressionChild, string]("id")))
}
