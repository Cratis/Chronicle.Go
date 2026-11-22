//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type DeferredComplianceRootCreated struct {
	Name string `json:"name"`
}

type DeferredComplianceChildAdded struct {
	ID     string `json:"id"`
	RootID string `json:"rootId"`
	Email  string `json:"email" chronicle:"pii"`
	Name   string `json:"name"`
}

type DeferredComplianceChildEmailChanged struct {
	ID     string `json:"id"`
	RootID string `json:"rootId"`
	Email  string `json:"email" chronicle:"pii"`
}

type DeferredComplianceChild struct {
	ID    string `json:"id" chronicle:"key"`
	Email string `json:"email" chronicle:"pii"`
	Name  string `json:"name"`
}

type DeferredComplianceRoot struct {
	ID       string                    `json:"id" chronicle:"key"`
	Name     string                    `json:"name" chronicle:"set(DeferredComplianceRootCreated)"`
	Children []DeferredComplianceChild `json:"children" chronicle:"children(DeferredComplianceChildAdded,key=id,parent-key=rootId)"`
}

const deferredComplianceContainer = "deferred-protected-children"

// These witnesses mirror the C# deferred-child PII scenarios for Chronicle#4599.
// The root subscription ensures a child arriving first must wait in a future.
func TestKernelDeferredProtectedChildBeforeRoot(t *testing.T) {
	f := newDeferredChildComplianceFixture(t)
	f.appendObserved(t, DeferredComplianceChildAdded{ID: "child", RootID: "root", Email: "initial@example.test", Name: "Child"})
	f.assertRootAbsent(t)
	f.appendObserved(t, DeferredComplianceRootCreated{Name: "Root"})
	f.assertReleasedChild(t, "initial@example.test", "Child")
	f.assertStoredChildProtected(t)
}

func TestKernelDeferredProtectedChildUpdateBeforeRoot(t *testing.T) {
	f := newDeferredChildComplianceFixture(t)
	f.appendObserved(t, DeferredComplianceChildEmailChanged{ID: "child", RootID: "root", Email: "updated@example.test"})
	f.assertRootAbsent(t)
	f.appendObserved(t, DeferredComplianceRootCreated{Name: "Root"})
	f.assertReleasedChild(t, "updated@example.test", "")
	f.assertStoredChildProtected(t)

	// As in C#, the later add may replace the email, but must still protect it.
	f.appendObserved(t, DeferredComplianceChildAdded{ID: "child", RootID: "root", Email: "initial@example.test", Name: "Child"})
	f.assertReleasedChild(t, "initial@example.test", "Child")
	f.assertStoredChildProtected(t)
}

type deferredChildComplianceFixture struct {
	kernel *kernelFixture
	store  *chronicle.EventStore
	reader *readmodels.Reader[DeferredComplianceRoot]
}

func newDeferredChildComplianceFixture(t *testing.T) *deferredChildComplianceFixture {
	t.Helper()
	f := newKernelFixture(t)
	registry := integrationRegistry[DeferredComplianceRootCreated](t)
	if _, err := chronicle.RegisterEvent[DeferredComplianceChildAdded](registry); err != nil {
		t.Fatal(err)
	}
	updated, err := chronicle.RegisterEvent[DeferredComplianceChildEmailChanged](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[DeferredComplianceRoot](registry, readmodels.WithContainerName(deferredComplianceContainer))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(projections.ModelBound(model, projections.WithNodes(projections.Node[DeferredComplianceChild](
		projections.FromEvent(updated,
			projections.UsingKey(projections.Path[DeferredComplianceChildEmailChanged, string]("id")),
			projections.UsingParentKey(projections.Path[DeferredComplianceChildEmailChanged, string]("rootId"))),
	)))); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	return &deferredChildComplianceFixture{kernel: f, store: store, reader: readmodels.For(store.ReadModels(), model)}
}

func (f *deferredChildComplianceFixture) appendObserved(t *testing.T, event any) {
	t.Helper()
	appended, err := f.store.EventLog().AppendWithMetadata(f.kernel.ctx, "root", event)
	if err != nil {
		t.Fatal("deferred-child append failed", err)
	}
	if err := appended.Result().Err(); err != nil {
		t.Fatal("deferred-child append was rejected", err)
	}
	completed, err := appended.WaitForCompletion(f.kernel.ctx, f.store.Observers(), 10*time.Second)
	if err != nil || !completed.IsSuccess() || completed.Trivial() {
		t.Fatal("deferred-child observation barrier failed")
	}
}

func (f *deferredChildComplianceFixture) assertRootAbsent(t *testing.T) {
	t.Helper()
	instance, err := f.reader.Get(f.kernel.ctx, "root")
	if err != nil {
		t.Fatal("deferred root read failed", err)
	}
	if instance.Exists {
		t.Fatal("child bypassed deferred resolution and created the root")
	}
}

func (f *deferredChildComplianceFixture) assertReleasedChild(t *testing.T, email, name string) {
	t.Helper()
	instance := awaitProjection(t, f.kernel.ctx, f.reader, "root", func(root DeferredComplianceRoot) bool {
		return root.Name == "Root" && len(root.Children) == 1 && root.Children[0].Email == email
	})
	child := instance.Value.Children[0]
	if child.ID != "child" || child.Name != name {
		t.Fatal("deferred resolution changed the child identity or unprotected name")
	}
}

func (f *deferredChildComplianceFixture) assertStoredChildProtected(t *testing.T) {
	t.Helper()
	assertStoredChildStringProtected(t, f.kernel, storedChildStringProtection{
		Collection: deferredComplianceContainer,
		Key:        "root",
		Children:   "children",
		Property:   "email",
		Plaintexts: []string{"initial@example.test", "updated@example.test"},
	})
}
