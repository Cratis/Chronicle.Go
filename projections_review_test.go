// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestProjectionDiscoveryInheritsModelDefaults(t *testing.T) {
	for _, test := range []struct {
		name     string
		options  []readmodels.ModelOption
		id       string
		sequence events.SequenceID
	}{
		{"sequence", []readmodels.ModelOption{readmodels.WithEventSequence("orders")}, "github.com/cratis/chronicle.go.ProjectionModel", "orders"},
		{"observer", []readmodels.ModelOption{readmodels.WithObserver(readmodels.Projection, "x")}, "x", events.EventLog},
		{"both", []readmodels.ModelOption{readmodels.WithObserver(readmodels.Projection, "x"), readmodels.WithEventSequence("orders")}, "x", "orders"},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry()
			model, err := RegisterReadModel[ProjectionModel](registry, test.options...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = RegisterEvent[ProjectionOpened](registry); err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(WithRegistry(registry))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			definition := client.projections[0]
			bound, ok := client.readModelCatalog.LookupIdentifier(model.Identifier())
			_, observer := bound.Observer()
			if !ok || definition.Identifier() != test.id || definition.EventSequence() != test.sequence || observer != test.id || bound.EventSequence() != test.sequence {
				t.Fatalf("projection/model defaults differ: %s %s %s %s", definition.Identifier(), definition.EventSequence(), observer, bound.EventSequence())
			}
		})
	}
}

func TestProjectionExplicitModelConflictsFailAtNewClient(t *testing.T) {
	for _, test := range []struct {
		name       string
		model      readmodels.ModelOption
		projection projections.Option
	}{
		{"observer", readmodels.WithObserver(readmodels.Projection, "x"), projections.WithIdentifier("y")},
		{"sequence", readmodels.WithEventSequence("orders"), projections.WithEventSequence("other")},
		{"explicit event log", readmodels.WithEventSequence(events.EventLog), projections.WithEventSequence("orders")},
		{"projection event log", readmodels.WithEventSequence("orders"), projections.WithEventLog()},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry()
			model, err := RegisterReadModel[ProjectionModel](registry, test.model)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = RegisterEvent[ProjectionOpened](registry); err != nil {
				t.Fatal(err)
			}
			if err = registry.AddProjection(projections.ModelBound(model, test.projection)); err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(WithRegistry(registry))
			var declaration *projections.DeclarationError
			if client != nil || !errors.As(err, &declaration) {
				t.Fatalf("conflict accepted: %v", err)
			}
		})
	}
}

type SupplementaryLiteralModel struct {
	Name string `json:"name" chronicle:"value(ProjectionOpened,value=\"\U00010400\")"`
}

type BooleanPathEvent struct {
	LowerTrue  string `json:"true"`
	UpperTrue  string `json:"True"`
	LowerFalse string `json:"false"`
	UpperFalse string `json:"False"`
}

type BooleanPathModel struct {
	Value string `json:"True" chronicle:"set(BooleanPathEvent)"`
}

func TestKernelUnrepresentableModelMappingsFailAtNewClient(t *testing.T) {
	for _, name := range []string{"supplementary literal", "boolean property"} {
		t.Run(name, func(t *testing.T) {
			registry := NewRegistry()
			if _, err := RegisterEvent[ProjectionOpened](registry); err != nil {
				t.Fatal(err)
			}
			if _, err := RegisterEvent[BooleanPathEvent](registry); err != nil {
				t.Fatal(err)
			}
			var err error
			if name == "supplementary literal" {
				_, err = RegisterReadModel[SupplementaryLiteralModel](registry)
			} else {
				_, err = RegisterReadModel[BooleanPathModel](registry)
			}
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(WithRegistry(registry))
			var declaration *projections.DeclarationError
			if client != nil || !errors.As(err, &declaration) || declaration.Path == "" {
				t.Fatalf("invalid mapping accepted: %v", err)
			}
		})
	}
}

func TestKernelUnrepresentableKeysFailAtNewClient(t *testing.T) {
	for _, path := range []string{"true", "True", "false", "False", "\U00010400"} {
		t.Run(path, func(t *testing.T) {
			registry := NewRegistry()
			model, err := RegisterReadModel[ProjectionModel](registry)
			if err != nil {
				t.Fatal(err)
			}
			event, err := RegisterEvent[BooleanPathEvent](registry)
			if err != nil {
				t.Fatal(err)
			}
			option := projections.UsingKey(projections.Path[BooleanPathEvent, string](path))
			if path == "\U00010400" {
				option = projections.UsingConstantKey(path)
			}
			if err = registry.AddProjection(projections.ModelBound(model, projections.FromEvent(event, option))); err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(WithRegistry(registry))
			var declaration *projections.DeclarationError
			if client != nil || !errors.As(err, &declaration) || declaration.Directive != "FromEvent" {
				t.Fatalf("invalid key accepted: %v", err)
			}
		})
	}
}
