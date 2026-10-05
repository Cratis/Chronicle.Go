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

type JoinRemovalUserCreated struct {
	Name string `json:"Name"`
}
type JoinRemovalGroupCreated struct {
	Name string `json:"Name"`
}
type JoinRemovalUserAddedToGroup struct {
	UserID string `json:"UserId"`
}
type JoinRemovalGroupRemoved struct{}
type JoinRemovalGroup struct {
	GroupID string `json:"GroupId" chronicle:"key"`
}
type JoinRemovalIDGroup struct {
	ID string `json:"Id" chronicle:"key"`
}
type JoinRemovalUser[G any] struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Groups []G    `json:"Groups"`
}

// This is the C# Integration/Client/Projections/Scenarios/when_removing/
// child_removed_with_join scenario. Its GroupId child identifier is not MongoDB's
// special Id/id name, unlike the NodeLine regression in projection_nodes_test.go.
func TestKernelProjectionChildRemovedWithJoin(t *testing.T) {
	t.Run("GroupId", func(t *testing.T) {
		testKernelChildRemovedWithJoin(t, "GroupId", func(g JoinRemovalGroup) string { return g.GroupID })
	})
	t.Run("Id", func(t *testing.T) {
		t.Skip("MongoDB join removal does not translate child Id to _id: https://github.com/Cratis/Chronicle/issues/4538")
		testKernelChildRemovedWithJoin(t, "Id", func(g JoinRemovalIDGroup) string { return g.ID })
	})
}

func testKernelChildRemovedWithJoin[G any](t *testing.T, identifiedBy string, groupID func(G) string) {
	t.Helper()
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	created, err := chronicle.RegisterEvent[JoinRemovalUserCreated](registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = chronicle.RegisterEvent[JoinRemovalGroupCreated](registry); err != nil {
		t.Fatal(err)
	}
	added, err := chronicle.RegisterEvent[JoinRemovalUserAddedToGroup](registry)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := chronicle.RegisterEvent[JoinRemovalGroupRemoved](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[JoinRemovalUser[G]](registry, readmodels.WithIdentifier("JoinRemovalUser"), readmodels.WithContainerName("JoinRemovalUsers"))
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("join-removal-user", model)
	projections.From(builder, created, nil)
	projections.Children(builder, projections.Path[JoinRemovalUser[G], []G]("Groups"), func(child *projections.Builder[G]) {
		projections.From(child, added, nil, projections.UsingParentKey(projections.Path[JoinRemovalUserAddedToGroup, string]("UserId")))
		child.Configure(projections.RemovedWithJoin(removed))
	}, projections.IdentifiedBy(projections.Path[G, string](identifiedBy)))
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	user, firstGroup, secondGroup := uuid.NewString(), uuid.NewString(), uuid.NewString()
	appendEvent := func(source string, event any) {
		t.Helper()
		result, err := store.EventLog().Append(fixture.ctx, events.SourceID(source), event)
		if err != nil {
			t.Fatal(err)
		}
		if err = result.Err(); err != nil {
			t.Fatal(err)
		}
	}
	reader := readmodels.For(store.ReadModels(), model)
	appendEvent(firstGroup, JoinRemovalGroupCreated{Name: "SomeGroup"})
	appendEvent(secondGroup, JoinRemovalGroupCreated{Name: "SomeOtherGroup"})
	appendEvent(user, JoinRemovalUserCreated{Name: "Someone"})
	appendEvent(firstGroup, JoinRemovalUserAddedToGroup{UserID: user})
	appendEvent(secondGroup, JoinRemovalUserAddedToGroup{UserID: user})
	awaitProjection(t, fixture.ctx, reader, readmodels.Key(user), func(u JoinRemovalUser[G]) bool { return len(u.Groups) == 2 })
	appendEvent(firstGroup, JoinRemovalGroupRemoved{})
	remaining := awaitProjection(t, fixture.ctx, reader, readmodels.Key(user), func(u JoinRemovalUser[G]) bool { return len(u.Groups) == 1 })
	if remaining.Value.ID != user || groupID(remaining.Value.Groups[0]) != secondGroup {
		t.Fatalf("join removal: %+v", remaining.Value)
	}
}
