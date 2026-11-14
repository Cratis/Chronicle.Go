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
)

type GvPersonView struct{ Name string }

// gvGraph declares a graph over the Gx nodes, with the views the caller builds.
func gvGraph(t *testing.T, name string, registered events.Type[GxPersonRegistered], build func(person *graphs.Node) []*graphs.View) (graphs.Declaration, error) {
	t.Helper()
	person := graphs.NewNode[GxPerson]().From(registered, graphs.UsingKey("personId"))
	return graphs.New(name, person).WithViews(build(person)...).Build()
}

func TestInvalidViewsDoNotBreakRegistrationOfOtherGraphs(t *testing.T) {
	registry := NewRegistry()
	registered, err := RegisterEvent[GxPersonRegistered](registry, events.WithID("gx-person-registered"))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := gvGraph(t, "People", registered, func(person *graphs.Node) []*graphs.View {
		return []*graphs.View{graphs.NewView[GvPersonView](person).Property("name")}
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gvGraph(t, "Broken", registered, func(person *graphs.Node) []*graphs.View {
		return []*graphs.View{graphs.NewView[GvPersonView](person).Property("missing")}
	})
	if !errors.Is(err, graphs.UnknownNodeProperty) || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("Build() = %v", err)
	}
	plain, err := gvGraph(t, "Plain", registered, func(*graphs.Node) []*graphs.View { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddGraph(valid); err != nil {
		t.Fatal(err)
	}
	if err := registry.AddGraph(plain); err != nil {
		t.Fatal(err)
	}
	var models, registeredProjections []string
	kernel := &supervisedKernel{}
	kernel.readModels = &readModelKernel{register: func(_ context.Context, r *modelcontracts.RegisterManyRequest) error {
		for _, model := range r.ReadModels {
			models = append(models, model.Type.Identifier)
		}
		return nil
	}}
	kernel.projections = &projectionKernel{register: func(_ context.Context, r *contracts.RegisterRequest) error {
		for _, projection := range r.Projections {
			registeredProjections = append(registeredProjections, projection.Identifier)
		}
		return nil
	}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), WithDefaultGraphSinkType(graphs.InMemoryGraph))
	if _, err := client.EventStore(ctx, "store"); err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 || len(registeredProjections) != 2 {
		t.Fatalf("read models = %v, projections = %v", models, registeredProjections)
	}
	artifacts, err := client.Artifacts("store")
	if err != nil || len(artifacts.Graphs) != 2 {
		t.Fatalf("artifacts = %v %v", artifacts.Graphs, err)
	}
	for _, definition := range artifacts.Graphs {
		views := definition.Views()
		switch definition.Name() {
		case "People":
			if len(views.Views) != 1 || views.Views[0].Name != "GvPersonView" || views.Views[0].Members[0].Source != "name" {
				t.Fatalf("People views = %+v", views)
			}
		case "Plain":
			if len(views.Views) != 0 {
				t.Fatalf("Plain views = %+v", views)
			}
		}
	}
}
