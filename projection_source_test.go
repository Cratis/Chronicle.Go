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
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

func TestProjectionSourceTemplateResolvesPerStore(t *testing.T) {
	r := NewRegistry()
	event, err := RegisterEvent[ProjectionOpened](r, events.WithSourceStore("origin"))
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[ProjectionModel](r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(model, projections.FromEvent(event))); err != nil {
		t.Fatal(err)
	}
	kernel := &supervisedKernel{
		readModels: &readModelKernel{register: func(context.Context, *modelcontracts.RegisterManyRequest) error { return nil }},
		projections: &projectionKernel{register: func(_ context.Context, r *contracts.RegisterRequest) error {
			want := "inbox-origin"
			if r.EventStore == "origin" {
				want = "event-log"
			}
			if len(r.Projections) != 1 || r.Projections[0].EventSequenceId != want {
				t.Errorf("wrong source template: %v", r)
			}
			return nil
		}},
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(r))
	for _, name := range []StoreName{"origin", "consumer"} {
		store, err := client.EventStore(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		want := events.SequenceID("inbox-origin")
		if name == "origin" {
			want = events.EventLog
		}
		if store.Projections()[0].EventSequence() != want || store.ReadModels().Catalog().Descriptors()[0].EventSequence() != want {
			t.Fatal("store model/definition sequence mismatch")
		}
	}
}

func TestGlobalHandlerCannotAlsoBeARegisteredReadModel(t *testing.T) {
	r := NewRegistry()
	if _, err := RegisterEvent[ProjectionOpened](r); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[ProjectionModel](r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(model, projections.GlobalFor[struct{}]())); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(r))
	if client != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("global model admitted: %v", err)
	}
}

func TestRecursiveReadModelIndexResolvesReferences(t *testing.T) {
	type Tree struct {
		Name     string
		Children []Tree
	}
	model, err := readmodels.Define[Tree](readmodels.WithIndexes("Children.Children.Name"))
	if err != nil {
		t.Fatal(err)
	}
	bound, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	if bound.Indexes()[0] != "children.children.name" {
		t.Fatal("recursive index naming lost")
	}
}
