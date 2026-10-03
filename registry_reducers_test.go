// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type FrozenFold struct{}

func (*FrozenFold) Fold(_ FoldChanged, current *FoldTotal) *FoldTotal { return current }

type WrongFold struct{}

func (WrongFold) Fold(FoldChanged, *FoldTotal, string) *FoldTotal { return nil }

func TestReducerDiscoveryValidatesAtNewClientWithoutConstruction(t *testing.T) {
	registry := NewRegistry()
	constructed := false
	model, err := RegisterReadModel[FoldTotal](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterReducer[*FrozenFold](registry, model, func() *FrozenFold { constructed = true; return &FrozenFold{} }); err != nil {
		t.Fatal(err)
	}
	// Forward event admission is legal until NewClient freezes the catalog.
	if _, err := RegisterEvent[FoldChanged](registry); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if constructed || len(client.reducers.defaults) != 1 {
		t.Fatal("constructor executed or no reducer plan")
	}
	d, _ := client.readModelCatalog.LookupIdentifier(model.Identifier())
	kind, id := d.Observer()
	if kind != readmodels.Reducer || id != "github.com/cratis/chronicle.go.FrozenFold" {
		t.Fatal(kind, id)
	}
	if kind, id := model.Descriptor().Observer(); kind != readmodels.Projection || id != "" {
		t.Fatal("authoring handle mutated")
	}
	// Client keeps a detached frozen catalog.
	registry.reducers = nil
	if len(client.reducers.defaults) != 1 {
		t.Fatal("snapshot mutated")
	}
}
func TestReducerProducerAndIdentityConflictsFailAtomically(t *testing.T) {
	for _, mode := range []string{"foreign model", "projection model", "reactor identity", "invalid fold", "sequence", "explicit projection", "duplicate model"} {
		t.Run(mode, func(t *testing.T) {
			registry := NewRegistry()
			ev, err := RegisterEvent[FoldChanged](registry)
			if err != nil {
				t.Fatal(err)
			}
			var options []readmodels.ModelOption
			if mode == "sequence" {
				options = append(options, readmodels.WithEventSequence("other"))
			}
			if mode == "explicit projection" {
				options = append(options, readmodels.WithObserver(readmodels.Projection, ""))
			}
			model, err := RegisterReadModel[FoldTotal](registry, options...)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "foreign model" {
				model, err = readmodels.Define[FoldTotal]()
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "projection model" {
				if err := registry.AddProjection(projections.ModelBound(model, projections.FromEvent(ev))); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "reactor identity" {
				if err := RegisterReactorHandler(registry, "fold", func(context.Context, FoldChanged) error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "invalid fold" {
				err = RegisterReducer[WrongFold](registry, model, nil, reducers.WithID("fold"))
			} else {
				err = RegisterReducer[*FrozenFold](registry, model, nil, reducers.WithID("fold"))
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "duplicate model" {
				err = RegisterReducerHandlers(registry, model, "other", []reducers.Handler{reducers.On(sumFold)})
				if !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatal(err)
				}
				return
			}
			client, err := NewClient(WithRegistry(registry))
			var detail *reducers.DeclarationError
			if client != nil || !errors.As(err, &detail) || !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("client %v err %v", client, err)
			}
		})
	}
}
func TestReducerPassiveAndInactiveBinding(t *testing.T) {
	for _, passive := range []bool{false, true} {
		registry := NewRegistry()
		if _, err := RegisterEvent[FoldChanged](registry); err != nil {
			t.Fatal(err)
		}
		var options []readmodels.ModelOption
		if passive {
			options = append(options, readmodels.Passive())
		}
		model, err := RegisterReadModel[FoldTotal](registry, options...)
		if err != nil {
			t.Fatal(err)
		}
		if err := RegisterReducer[*FrozenFold](registry, model, nil, reducers.WithActive(false), reducers.WithEventSequence(events.SequenceID("custom"))); err != nil {
			t.Fatal(err)
		}
		client, err := NewClient(WithRegistry(registry))
		if err != nil {
			t.Fatal(err)
		}
		plan := client.reducers.defaults[0]
		if plan.IsActive() || plan.IsPassive() != passive || plan.Model().EventSequence() != "custom" {
			t.Fatal("passive/inactive binding incorrect")
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
