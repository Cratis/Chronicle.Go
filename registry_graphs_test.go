// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/graphs"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/proto"
)

type GxPersonRegistered struct {
	PersonID string `json:"personId"`
	Name     string `json:"name"`
}
type GxMemberAdded struct {
	PersonID string `json:"personId"`
	GroupID  string `json:"groupId"`
}
type GxPerson struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}
type GxGroup struct {
	ID          string `json:"id" chronicle:"key"`
	MemberCount int    `json:"memberCount"`
}
type GxMemberOf struct {
	Since string `json:"since"`
}

// graphRegistry registers the events and a graph of people and groups.
func graphRegistry(t *testing.T) (*Registry, events.Type[GxPersonRegistered], events.Type[GxMemberAdded]) {
	t.Helper()
	registry := NewRegistry()
	registered, err := RegisterEvent[GxPersonRegistered](registry, events.WithID("gx-person-registered"))
	if err != nil {
		t.Fatal(err)
	}
	added, err := RegisterEvent[GxMemberAdded](registry, events.WithID("gx-member-added"))
	if err != nil {
		t.Fatal(err)
	}
	person := graphs.NewNode[GxPerson]().From(registered, graphs.UsingKey("personId"))
	group := graphs.NewNode[GxGroup]().From(added, graphs.UsingKey("groupId"), graphs.Increment("memberCount"))
	memberOf := graphs.NewEdge[GxMemberOf](person, group).Named("MEMBER_OF").
		From(added, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"), graphs.SetFromContext("since", "occurred"))
	declaration, err := graphs.New("Organization", person, group, memberOf).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddGraph(declaration); err != nil {
		t.Fatal(err)
	}
	return registry, registered, added
}

func TestGraphIsRegisteredAsReadModelAndProjectionBesideOrdinaryOnes(t *testing.T) {
	registry, _, _ := graphRegistry(t)
	model, err := RegisterReadModel[ProjectionModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RegisterEvent[ProjectionOpened](registry); err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(projections.ModelBound(model)); err != nil {
		t.Fatal(err)
	}
	var models *modelcontracts.RegisterManyRequest
	var registered *contracts.RegisterRequest
	kernel := &supervisedKernel{}
	kernel.readModels = &readModelKernel{register: func(_ context.Context, r *modelcontracts.RegisterManyRequest) error {
		models = proto.Clone(r).(*modelcontracts.RegisterManyRequest)
		return nil
	}}
	kernel.projections = &projectionKernel{register: func(_ context.Context, r *contracts.RegisterRequest) error {
		if models == nil {
			t.Error("projections were registered before read models")
		}
		registered = proto.Clone(r).(*contracts.RegisterRequest)
		return nil
	}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	if _, err := client.EventStore(ctx, "store", WithNamespace("one")); err != nil {
		t.Fatal(err)
	}
	if len(models.ReadModels) != 2 || len(registered.Projections) != 2 || !registered.FullSet {
		t.Fatalf("read models = %d, projections = %d, full = %v", len(models.ReadModels), len(registered.Projections), registered.FullSet)
	}
	var graph *modelcontracts.ReadModelDefinition
	for _, definition := range models.ReadModels {
		if definition.Type.Identifier == "Organization" {
			graph = definition
		}
	}
	if graph == nil || graph.Sink.TypeId != "Neo4j" || graph.ObserverIdentifier != "Organization" || graph.ObserverType != modelcontracts.ReadModelObserverType_Projection || models.Owner != modelcontracts.ReadModelOwner_Client {
		t.Fatalf("graph read model = %v", graph)
	}
	var projection *contracts.ProjectionDefinition
	for _, definition := range registered.Projections {
		if definition.Graph != nil {
			projection = definition
		}
	}
	if projection == nil || projection.Identifier != "Organization" || projection.ReadModel != "Organization" || len(projection.Graph.Nodes) != 2 || len(projection.Graph.Edges) != 1 {
		t.Fatalf("graph projection = %v", projection)
	}
	if edge := projection.Graph.Edges["MEMBER_OF"].From["gx-member-added+1"]; edge == nil || edge.SourceKey != "personId" || edge.TargetKey != "groupId" || edge.Properties["since"] != "$eventContext(occurred)" {
		t.Fatalf("edge = %v", edge)
	}
	for _, definition := range registered.Projections {
		if definition != projection && definition.Graph != nil {
			t.Fatal("an ordinary projection carries a graph")
		}
	}
}

func TestGraphAloneIsRegisteredWithoutOrdinaryReadModels(t *testing.T) {
	registry, _, _ := graphRegistry(t)
	var models, projectionRequests int
	kernel := &supervisedKernel{}
	kernel.readModels = &readModelKernel{register: func(_ context.Context, r *modelcontracts.RegisterManyRequest) error {
		models += len(r.ReadModels)
		return nil
	}}
	kernel.projections = &projectionKernel{register: func(_ context.Context, r *contracts.RegisterRequest) error {
		projectionRequests += len(r.Projections)
		return nil
	}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), WithDefaultGraphSinkType(graphs.InMemoryGraph))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	if models != 1 || projectionRequests != 1 {
		t.Fatalf("read models = %d, projections = %d", models, projectionRequests)
	}
	artifacts, err := client.Artifacts("store")
	if err != nil || len(artifacts.Graphs) != 1 || artifacts.Graphs[0].Sink() != graphs.InMemoryGraph || artifacts.Graphs[0].Name() != "Organization" {
		t.Fatalf("artifacts = %v %v", artifacts.Graphs, err)
	}
	if len(store.Projections()) != 0 {
		t.Fatal("a graph is not an ordinary projection definition")
	}
}

func TestGraphsPerStoreRegistryAreKeptApart(t *testing.T) {
	registry, _, _ := graphRegistry(t)
	other := NewRegistry()
	var seen []string
	kernel := &supervisedKernel{}
	kernel.readModels = &readModelKernel{register: func(_ context.Context, r *modelcontracts.RegisterManyRequest) error {
		for _, definition := range r.ReadModels {
			seen = append(seen, r.EventStore+":"+definition.Type.Identifier)
		}
		return nil
	}}
	kernel.projections = &projectionKernel{register: func(context.Context, *contracts.RegisterRequest) error { return nil }}
	client, ctx := supervisionClient(t, kernel, WithRegistry(other), WithRegistryForStore("graphed", registry))
	for _, name := range []StoreName{"graphed", "plain"} {
		if _, err := client.EventStore(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 1 || seen[0] != "graphed:Organization" {
		t.Fatalf("registered = %v", seen)
	}
	for name, want := range map[StoreName]int{"graphed": 1, "plain": 0} {
		artifacts, err := client.Artifacts(name)
		if err != nil || len(artifacts.Graphs) != want {
			t.Fatalf("%s graphs = %d %v", name, len(artifacts.Graphs), err)
		}
	}
}

func TestGraphAdmissionFailsBeforeConnecting(t *testing.T) {
	declare := func(t *testing.T, registry *Registry, node func(events.Type[GxPersonRegistered]) *graphs.Node) {
		t.Helper()
		registered, err := RegisterEvent[GxPersonRegistered](registry)
		if err != nil {
			t.Fatal(err)
		}
		declaration, err := graphs.New("Organization", node(registered)).Build()
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.AddGraph(declaration); err != nil {
			t.Fatal(err)
		}
	}
	person := func(event events.Type[GxPersonRegistered]) *graphs.Node {
		return graphs.NewNode[GxPerson]().From(event, graphs.UsingKey("personId"))
	}
	t.Run("an event that is not registered with the client", func(t *testing.T) {
		registry := NewRegistry()
		unregistered, err := events.Define[GxPersonRegistered]()
		if err != nil {
			t.Fatal(err)
		}
		declaration, err := graphs.New("Organization", person(unregistered)).Build()
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.AddGraph(declaration); err != nil {
			t.Fatal(err)
		}
		if _, err = NewClient(WithConnectionString("chronicle://localhost:35000"), WithRegistry(registry)); !errors.Is(err, ErrNotRegistered) {
			t.Fatalf("NewClient() = %v", err)
		}
	})
	t.Run("a node that is also a read model", func(t *testing.T) {
		registry := NewRegistry()
		declare(t, registry, person)
		if _, err := RegisterReadModel[GxPerson](registry); err != nil {
			t.Fatal(err)
		}
		if _, err := NewClient(WithConnectionString("chronicle://localhost:35000"), WithRegistry(registry)); !errors.Is(err, graphs.ErrElementIsReadModel) || !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("NewClient() = %v", err)
		}
	})
	t.Run("a name that another artifact has", func(t *testing.T) {
		registry := NewRegistry()
		declare(t, registry, person)
		if _, err := RegisterReadModel[ProjectionModel](registry, readmodels.WithIdentifier("Organization")); err != nil {
			t.Fatal(err)
		}
		if _, err := NewClient(WithConnectionString("chronicle://localhost:35000"), WithRegistry(registry)); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("NewClient() = %v", err)
		}
	})
	t.Run("a property the event does not have", func(t *testing.T) {
		registry := NewRegistry()
		declare(t, registry, func(event events.Type[GxPersonRegistered]) *graphs.Node {
			return graphs.NewNode[GxPerson]().From(event, graphs.UsingKey("personId"), graphs.Set("name", "nickname"))
		})
		if _, err := NewClient(WithConnectionString("chronicle://localhost:35000"), WithRegistry(registry)); !errors.Is(err, graphs.ErrUnknownProperty) {
			t.Fatalf("NewClient() = %v", err)
		}
	})
}

func TestAddGraphRefusesDuplicatesAndZeroValues(t *testing.T) {
	registry, registered, _ := graphRegistry(t)
	declaration, err := graphs.New("Organization", graphs.NewNode[GxPerson]().From(registered)).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddGraph(declaration); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate graph = %v", err)
	}
	if err := registry.AddGraph(graphs.Declaration{}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("zero declaration = %v", err)
	}
	var nilRegistry *Registry
	if err := nilRegistry.AddGraph(declaration); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil registry = %v", err)
	}
}

func TestDefaultGraphSinkTypeIsValidated(t *testing.T) {
	for _, kind := range []graphs.SinkType{"", "MongoDB"} {
		if _, err := CaptureClient(WithConnectionString("chronicle://localhost:35000"), WithDefaultGraphSinkType(kind)); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("sink %q = %v", kind, err)
		}
	}
	for _, kind := range []graphs.SinkType{graphs.Neo4j, graphs.InMemoryGraph} {
		p, err := CaptureClient(WithConnectionString("chronicle://localhost:35000"), WithDefaultGraphSinkType(kind))
		if err != nil {
			t.Fatalf("sink %q = %v", kind, err)
		}
		_ = p.Client().Close()
	}
}

func TestDerivedRegistryKeepsGraphs(t *testing.T) {
	registry, _, _ := graphRegistry(t)
	model, err := RegisterReadModel[ProjectionModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RegisterEvent[ProjectionOpened](registry); err != nil {
		t.Fatal(err)
	}
	derived, err := registry.WithProjection(projections.ModelBound(model))
	if err != nil {
		t.Fatal(err)
	}
	if got := captureRegistry(derived).graphs; len(got) != 1 || got[0].Name() != "Organization" {
		t.Fatalf("graphs = %v", got)
	}
}
