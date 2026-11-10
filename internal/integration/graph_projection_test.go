//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	observation "github.com/cratis/chronicle.go/contracts/observation"
	projections "github.com/cratis/chronicle.go/contracts/projections"
	readmodels "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/graphs"
)

type GraphPersonRegistered struct {
	PersonID string `json:"personId"`
	Name     string `json:"name"`
}
type GraphPersonRemoved struct {
	PersonID string `json:"personId"`
}
type GraphGroupCreated struct {
	GroupID string `json:"groupId"`
	Title   string `json:"title"`
}
type GraphMemberJoined struct {
	PersonID string `json:"personId"`
	GroupID  string `json:"groupId"`
	Role     string `json:"role"`
}
type GraphMemberLeft struct {
	PersonID string `json:"personId"`
	GroupID  string `json:"groupId"`
	Role     string `json:"role"`
}
type GraphPerson struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}
type GraphGroup struct {
	ID          string `json:"id" chronicle:"key"`
	Title       string `json:"title"`
	MemberCount int    `json:"memberCount"`
}
type GraphMemberOf struct {
	Role  string    `json:"role" chronicle:"key"`
	Since time.Time `json:"since"`
}

// TestKernelGraphProjectionRegistersAndObservesEvents proves the kernel accepts
// the graph definition the client builds and runs its observer over events. The
// in-memory graph sink needs no Neo4j; the client cannot read a graph back, so
// the witness is the stored definitions and an observer that handles every event
// without a failed partition.
func TestKernelGraphProjectionRegistersAndObservesEvents(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	registered, err := chronicle.RegisterEvent[GraphPersonRegistered](registry)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := chronicle.RegisterEvent[GraphPersonRemoved](registry)
	if err != nil {
		t.Fatal(err)
	}
	created, err := chronicle.RegisterEvent[GraphGroupCreated](registry)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := chronicle.RegisterEvent[GraphMemberJoined](registry)
	if err != nil {
		t.Fatal(err)
	}
	left, err := chronicle.RegisterEvent[GraphMemberLeft](registry)
	if err != nil {
		t.Fatal(err)
	}
	person := graphs.NewNode[GraphPerson]().
		From(registered, graphs.UsingKey("personId")).
		RemovedWith(removed, graphs.UsingKey("personId"), graphs.Detach())
	group := graphs.NewNode[GraphGroup]().
		From(created, graphs.UsingKey("groupId")).
		From(joined, graphs.UsingKey("groupId"), graphs.Increment("memberCount")).
		From(left, graphs.UsingKey("groupId"), graphs.Decrement("memberCount"))
	memberOf := graphs.NewEdge[GraphMemberOf](person, group).Named("MEMBER_OF").
		From(joined, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"), graphs.SetFromContext("since", "occurred")).
		RemovedWith(left, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"))
	declaration, err := graphs.New("GraphOrganization", person, group, memberOf).UsingSink(graphs.InMemoryGraph).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddGraph(declaration); err != nil {
		t.Fatal(err)
	}
	client := f.client(registry)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}

	definitions, err := readmodels.NewReadModelsClient(f.conn).GetDefinitions(f.ctx, &readmodels.GetDefinitionsRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	var model *readmodels.ReadModelDefinition
	for _, definition := range definitions.ReadModels {
		if definition.GetType().GetIdentifier() == "GraphOrganization" {
			model = definition
		}
	}
	if model == nil || model.GetSink().GetTypeId() != "InMemoryGraph" || !strings.Contains(model.GetSchema(), `"x-graph":true`) || !strings.Contains(model.GetSchema(), `"x-graph-identity":"role"`) {
		t.Fatalf("graph read model = %v", model)
	}
	all, err := projections.NewProjectionsClient(f.conn).GetAllDefinitions(f.ctx, &projections.GetAllDefinitionsRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	var projection *projections.ProjectionDefinition
	for _, definition := range all.GetItems() {
		if definition.GetIdentifier() == "GraphOrganization" {
			projection = definition
		}
	}
	if projection.GetGraph() == nil || len(projection.GetGraph().GetNodes()) != 2 || len(projection.GetGraph().GetEdges()) != 1 || !projection.GetGraph().GetNodes()["GraphPerson"].GetRemovedWith()[eventKey(removed)].GetDetach() {
		t.Fatalf("graph projection = %v", projection)
	}

	awaitObserversObserving(t, f, store.Namespace(), "GraphOrganization")
	facts := []struct {
		source events.SourceID
		event  any
	}{
		{"ada", GraphPersonRegistered{PersonID: "ada", Name: "Ada"}},
		{"chess", GraphGroupCreated{GroupID: "chess", Title: "Chess club"}},
		{"membership-1", GraphMemberJoined{PersonID: "ada", GroupID: "chess", Role: "captain"}},
		{"membership-1", GraphMemberLeft{PersonID: "ada", GroupID: "chess", Role: "captain"}},
		{"ada", GraphPersonRemoved{PersonID: "ada"}},
	}
	for _, fact := range facts {
		appendSuccessfully(t, f.ctx, store, fact.source, fact.event)
	}
	observers := observation.NewObserversClient(f.conn)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := observers.GetObserverInformation(f.ctx, &observation.GetObserverInformationRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), EventSequenceId: "event-log", ObserverId: "GraphOrganization"})
		if err != nil {
			t.Fatal(err)
		}
		if info.GetRunningState() == observation.ObserverRunningState_Active && info.GetHandledEventCount() >= uint64(len(facts)) {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatalf("graph observer did not handle the events: %v", info)
		case <-ticker.C:
		}
	}
	failed, err := observation.NewFailedPartitionsClient(f.conn).GetFailedPartitions(f.ctx, &observation.GetFailedPartitionsRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "GraphOrganization"})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed.GetItems()) != 0 {
		t.Fatalf("graph observer failed partitions: %v", failed.GetItems())
	}
}

func eventKey[T any](event events.Type[T]) string {
	ref := event.Ref()
	return fmt.Sprintf("%s+%d", ref.ID, ref.Generation)
}
