// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type snapshotReplacementPrevious struct{ OldValue string }

func TestAllSelectedSchemasFreezeBeforeDefinitionCallbacks(t *testing.T) {
	for _, phase := range []string{"factory", "composition"} {
		t.Run(phase, func(t *testing.T) {
			changed, calls := false, 0
			classifier := compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
				calls++
				return compliance.Classification{PII: changed && target.Field == "Title"}, nil
			})
			defaults, selected := NewRegistry(), NewRegistry()
			event, err := RegisterEvent[catalogEvent](selected, events.WithProtection(classifier), events.WithSourceStore("origin"))
			if err != nil {
				t.Fatal(err)
			}
			model, err := RegisterReadModel[catalogModel](selected, readmodels.WithProtection(classifier))
			if err != nil {
				t.Fatal(err)
			}
			if err := selected.AddProjection(projections.ModelBound(model, projections.FromEvent(event))); err != nil {
				t.Fatal(err)
			}
			if phase == "factory" {
				factoryEvent := declareEvent[catalogEvent](t, defaults)
				factoryModel, err := RegisterReadModel[catalogModel](defaults)
				if err != nil {
					t.Fatal(err)
				}
				if err := RegisterProjectionFactory(defaults, "prepared", factoryModel.Descriptor(), nil, func(context.Context, preparationDependency) (projections.Declaration, error) {
					changed = true
					return projections.ModelBound(factoryModel, projections.WithIdentifier("prepared"), projections.FromEvent(factoryEvent)), nil
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				declareEvent[DeclaredEmail](t, defaults)
				if err := defaults.ConfigureDeclaredConstraint("email", func(*constraints.Builder) { changed = true }); err != nil {
					t.Fatal(err)
				}
			}
			client, err := NewClient(WithRegistry(defaults), WithRegistryForStore("selected", selected), WithNamingPolicy(serialization.CamelCase))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			if !changed {
				t.Fatal("preparation callback did not run")
			}
			afterPrepare := calls
			for range 2 {
				artifacts, err := client.Artifacts("selected")
				if err != nil {
					t.Fatal(err)
				}
				for _, descriptor := range artifacts.Events.Descriptors() {
					if strings.Contains(descriptor.Schema(), `"PII"`) {
						t.Fatal("factory reinterpreted a captured event schema")
					}
					if _, err := descriptor.WithNamingPolicy(serialization.PreservePropertyNames); err != nil {
						t.Fatal(err)
					}
				}
				for _, descriptor := range artifacts.ReadModels.Descriptors() {
					if strings.Contains(descriptor.Schema(), `"PII"`) {
						t.Fatal("factory reinterpreted a captured model schema")
					}
					if _, err := descriptor.WithNamingPolicy(serialization.PreservePropertyNames); err != nil {
						t.Fatal(err)
					}
				}
				for _, projection := range artifacts.Projections {
					if _, err := projection.ForStore("origin"); err != nil {
						t.Fatal(err)
					}
				}
			}
			if calls != afterPrepare {
				t.Fatal("frozen naming/store binding reran a classifier")
			}
		})
	}
}

func TestSkipKeepAliveRejectsCapturedObserversBeforeAnyFactoryOrClassifier(t *testing.T) {
	registry := NewRegistry()
	armed, calls := false, 0
	classifier := compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		if armed {
			calls++
		}
		return compliance.Classification{}, nil
	})
	model, err := RegisterReadModel[catalogModel](registry, readmodels.WithProtection(classifier))
	if err != nil {
		t.Fatal(err)
	}
	event, err := RegisterEvent[catalogEvent](registry, events.WithProtection(classifier))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterProjectionFactory(registry, "prepared", model.Descriptor(), func() preparationDependency { calls++; return preparationDependency{} }, func(context.Context, preparationDependency) (projections.Declaration, error) {
		calls++
		return projections.ModelBound(model, projections.WithIdentifier("prepared"), projections.FromEvent(event)), nil
	}); err != nil {
		t.Fatal(err)
	}
	selected := NewRegistry()
	if _, err := RegisterEvent[catalogEvent](selected); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactorHandler(selected, "observer", func(context.Context, catalogEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	armed = true
	scopes := &snapshotPreparationScopes{}
	client, err := NewClient(WithRegistry(registry), WithRegistryForStore("selected", selected), WithServices(scopes), WithSkipKeepAlive())
	if client != nil || !errors.Is(err, ErrInvalidConfiguration) || calls != 0 || scopes.opened != 0 {
		t.Fatal(client, err, calls, scopes.opened)
	}
}

func TestFactoryMetadataInputIsCopiedAndCallbacksAreNeverCapturedByEvaluation(t *testing.T) {
	registry := NewRegistry()
	event := declareEvent[catalogEvent](t, registry)
	names := []string{"original"}
	if err := RegisterConstraintFactory(registry, names, nil, func(context.Context, preparationDependency) ([]constraints.Definition, error) {
		definition, err := constraints.UniqueValues("original").On(event.Descriptor(), "Title").Build()
		return []constraints.Definition{definition}, err
	}); err != nil {
		t.Fatal(err)
	}
	names[0] = "mutated"
	before := captureRegistry(registry)
	client, err := NewClient(WithRegistry(registry), WithRegistryForStore("shared", registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if !reflect.DeepEqual(before.constraintFactories[0].names, []string{"original"}) || client.constraints[0].Name() != "original" {
		t.Fatal("metadata aliased caller slice")
	}
}
