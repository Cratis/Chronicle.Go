//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type TreeCreated struct {
	Name string `json:"name"`
}
type TreeBranchAdded struct {
	TreeID   string `json:"treeId"`
	BranchID string `json:"branchId"`
	Name     string `json:"name"`
}
type TreeChildAdded struct {
	ParentID string `json:"parentId"`
	Name     string `json:"name"`
}
type TreeBranchRenamed struct {
	BranchID string `json:"branchId"`
	Name     string `json:"name"`
}
type TreeBranch struct {
	ID       string       `json:"id" chronicle:"key"`
	Name     string       `json:"name"`
	Local    string       `json:"local" chronicle:"no-auto"`
	Children []TreeBranch `json:"children" chronicle:"children(TreeChildAdded,identified-by=id,parent-key=parentId)"`
}
type RecursiveTree struct {
	ID       string       `json:"id" chronicle:"key"`
	Name     string       `json:"name"`
	Branches []TreeBranch `json:"branches" chronicle:"children(TreeBranchAdded,key=branchId,identified-by=id,parent-key=treeId)"`
}

func TestKernelRecursiveKeyedChildren(t *testing.T) {
	fixture := newKernelFixture(t)
	r := chronicle.NewRegistry()
	created, err := chronicle.RegisterEvent[TreeCreated](r)
	if err != nil {
		t.Fatal(err)
	}
	added, err := chronicle.RegisterEvent[TreeBranchAdded](r)
	if err != nil {
		t.Fatal(err)
	}
	child, err := chronicle.RegisterEvent[TreeChildAdded](r)
	if err != nil {
		t.Fatal(err)
	}
	renamed, err := chronicle.RegisterEvent[TreeBranchRenamed](r)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[RecursiveTree](r)
	if err != nil {
		t.Fatal(err)
	}
	declaration := projections.ModelBound(model, projections.FromEvent(created), projections.WithNodes(projections.Node[TreeBranch](
		projections.FromEvent(added),
		projections.FromEvent(child, projections.UsingParentKey(projections.Path[TreeChildAdded, string]("parentId"))),
		projections.FromEvent(renamed, projections.UsingKey(projections.Path[TreeBranchRenamed, string]("branchId"))),
	)))
	if err := r.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	store, err := fixture.client(r).EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	treeID, branchID, childID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	appendSuccessfully(t, fixture.ctx, store, events.SourceID(treeID), TreeCreated{Name: "tree"})
	reader := readmodels.For(store.ReadModels(), model)
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(treeID), func(m RecursiveTree) bool { return m.Name == "tree" })
	appendSuccessfully(t, fixture.ctx, store, events.SourceID(treeID), TreeBranchAdded{TreeID: treeID, BranchID: branchID, Name: "branch"})
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(treeID), func(m RecursiveTree) bool { return len(m.Branches) == 1 })
	appendSuccessfully(t, fixture.ctx, store, events.SourceID(childID), TreeChildAdded{ParentID: branchID, Name: "child"})
	state := awaitProjection(t, fixture.ctx, reader, readmodels.Key(treeID), func(m RecursiveTree) bool { return len(m.Branches) == 1 && len(m.Branches[0].Children) == 1 })
	if state.Value.Branches[0].ID != branchID || state.Value.Branches[0].Children[0].ID != childID || state.Value.Branches[0].Children[0].Name != "child" {
		t.Fatalf("recursive state: %+v", state.Value)
	}
	appendSuccessfully(t, fixture.ctx, store, events.SourceID(treeID), TreeBranchRenamed{BranchID: branchID, Name: "renamed"})
	state = awaitProjection(t, fixture.ctx, reader, readmodels.Key(treeID), func(m RecursiveTree) bool { return len(m.Branches) == 1 && m.Branches[0].Name == "renamed" })
	if len(state.Value.Branches[0].Children) != 1 || state.Value.Branches[0].Children[0].ID != childID {
		t.Fatal("keyed update created a self-child")
	}
}
