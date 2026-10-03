// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
	"google.golang.org/protobuf/proto"
)

type factoryCurrent struct{ Name string }
type factoryPrevious struct{ OldName string }
type factoryModel struct{ ID, Name string }
type factoryConfig struct{ label string }
type factoryArtifact struct {
	config factoryConfig
	closed *int
}

func (a *factoryArtifact) Close() error { (*a.closed)++; return nil }

type factoryDeclarations struct {
	registry *chronicle.Registry
	current  events.Type[factoryCurrent]
	previous events.Type[factoryPrevious]
	model    readmodels.Model[factoryModel]
}

func newFactoryDeclarations(t *testing.T) factoryDeclarations {
	t.Helper()
	r := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[factoryCurrent](r, events.WithID("factory-event"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := chronicle.RegisterEventGeneration[factoryPrevious](r, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[factoryModel](r)
	if err != nil {
		t.Fatal(err)
	}
	return factoryDeclarations{r, current, previous, model}
}
func (d factoryDeclarations) projection(_ context.Context, a *factoryArtifact) (projections.Declaration, error) {
	return projections.ModelBound(d.model, projections.WithIdentifier("factory-projection"), projections.FromEvent(d.current),
		projections.WithInitialValues(factoryModel{Name: a.config.label}), projections.WithLabels(a.config.label)), nil
}
func (d factoryDeclarations) constraint(_ context.Context, a *factoryArtifact) ([]constraints.Definition, error) {
	definition, err := constraints.UniqueValues("factory-name").On(d.current.Descriptor(), "Name").WithMessage(a.config.label + ": {PropertyValue}").Build()
	return []constraints.Definition{definition}, err
}
func (d factoryDeclarations) migration(_ context.Context, _ *factoryArtifact) (events.MigrationDeclaration, error) {
	return events.DefineMigration(d.current, d.previous, events.Migration[factoryCurrent, factoryPrevious]{
		Upcast:   func(b *events.MigrationBuilder[factoryCurrent, factoryPrevious]) { b.RenamedFrom("Name", "OldName") },
		Downcast: func(b *events.MigrationBuilder[factoryPrevious, factoryCurrent]) { b.RenamedFrom("OldName", "Name") },
	})
}
func (d factoryDeclarations) register(t *testing.T, factory any) {
	t.Helper()
	if err := chronicle.RegisterProjectionFactory(d.registry, "factory-projection", d.model.Descriptor(), factory, d.projection); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterConstraintFactory(d.registry, []string{"factory-name"}, factory, d.constraint); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterEventMigrationFactory(d.registry, d.current.Descriptor(), d.previous.Descriptor(), factory, d.migration); err != nil {
		t.Fatal(err)
	}
}

func TestDefinitionFactoriesPlainAndFundamentalsProduceIdenticalFrozenArtifacts(t *testing.T) {
	var baseline chronicle.Artifacts
	for _, mode := range []string{"plain", "dependencies", "scoped precedence", "scoped nil", "singleton precedence"} {
		t.Run(mode, func(t *testing.T) {
			d := newFactoryDeclarations(t)
			constructed, closed := 0, 0
			config := factoryConfig{label: "configured"}
			create := func(config factoryConfig) *factoryArtifact { constructed++; return &factoryArtifact{config, &closed} }
			var factory any = func() *factoryArtifact { return create(config) }
			options := []chronicle.ClientOption{chronicle.WithRegistry(d.registry), chronicle.WithRegistryForStore("shared", d.registry), chronicle.WithRegistryForStore("empty", nil), chronicle.WithNamingPolicy(serialization.CamelCase), chronicle.WithEventTypeGenerationValidation(true)}
			var provider dependencyinjection.Provider
			if mode != "plain" {
				var bindings container.Registry
				if err := dependencyinjection.BindValue(&bindings, config); err != nil {
					t.Fatal(err)
				}
				factory = func(_ context.Context, config factoryConfig) *factoryArtifact { return create(config) }
				if mode != "dependencies" {
					lifetime := dependencyinjection.Scoped
					if mode == "singleton precedence" {
						lifetime = dependencyinjection.Singleton
					}
					if err := dependencyinjection.BindFunc1(&bindings, lifetime, func(_ context.Context, config factoryConfig) (*factoryArtifact, error) { return create(config), nil }); err != nil {
						t.Fatal(err)
					}
					factory = func() *factoryArtifact { t.Fatal("registered service lost precedence"); return nil }
					if mode == "scoped nil" {
						factory = nil
					}
				}
				var err error
				provider, err = bindings.Build()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := provider.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				options = append(options, services.WithServices(provider))
			}
			d.register(t, factory)
			client, err := chronicle.NewClient(options...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			got, err := client.Artifacts("shared")
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Projections) != 1 || len(got.Constraints) != 1 || len(got.Events.Migrations()) != 1 {
				t.Fatal("factory outputs missing")
			}
			if mode == "plain" {
				baseline = got
			} else {
				if !proto.Equal(got.Projections[0].KernelDefinition(), baseline.Projections[0].KernelDefinition()) || !reflect.DeepEqual(got.Events.Migrations(), baseline.Events.Migrations()) || !reflect.DeepEqual(got.Constraints[0].Fields()[0].Properties, baseline.Constraints[0].Fields()[0].Properties) {
					t.Fatal("DI changed canonical output")
				}
			}
			if got.Projections[0].KernelDefinition().InitialModelState != `{"id":"","name":"configured"}` {
				t.Fatal("initial state not naming-bound")
			}
			violation := got.Constraints[0].ResolveMessage(constraints.Violation{ConstraintName: "factory-name", Details: map[string]string{constraints.PropertyValue: "taken"}})
			if violation.Message != "configured: taken" {
				t.Fatal(violation)
			}
			empty, err := client.Artifacts("empty")
			if err != nil || len(empty.Projections)+len(empty.Constraints)+len(empty.Events.Migrations()) != 0 {
				t.Fatal("nil registry inherited factories", err)
			}
			wantConstructed, wantClosed := 3, 3
			if mode == "singleton precedence" {
				wantConstructed, wantClosed = 1, 0
			}
			if constructed != wantConstructed || closed != wantClosed {
				t.Fatalf("constructed=%d closed=%d", constructed, closed)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if closed != wantClosed {
				t.Fatal("client closed borrowed services again")
			}
			if provider != nil {
				if err := provider.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				if closed != wantConstructed {
					t.Fatalf("provider cleanup count=%d", closed)
				}
			}
		})
	}
}

func TestDefinitionFactoriesRequireExactOutputIdentitiesAndNoRuntimeMessages(t *testing.T) {
	for _, scenario := range []string{"projection id", "projection foreign model", "constraint extra", "constraint missing", "constraint duplicate", "constraint runtime message", "migration endpoints"} {
		t.Run(scenario, func(t *testing.T) {
			d := newFactoryDeclarations(t)
			closed := 0
			factory := func() *factoryArtifact { return &factoryArtifact{factoryConfig{}, &closed} }
			var err error
			switch scenario {
			case "projection id", "projection foreign model":
				model := d.model
				id := "wrong"
				if scenario == "projection foreign model" {
					model, err = readmodels.Define[factoryModel]()
					id = "factory-projection"
				}
				if err != nil {
					t.Fatal(err)
				}
				err = chronicle.RegisterProjectionFactory(d.registry, "factory-projection", d.model.Descriptor(), factory, func(context.Context, *factoryArtifact) (projections.Declaration, error) {
					return projections.ModelBound(model, projections.WithIdentifier(id), projections.FromEvent(d.current)), nil
				})
			case "migration endpoints":
				foreign, defineErr := events.Define[factoryCurrent](events.WithID("factory-event"), events.WithGeneration(2))
				if defineErr != nil {
					t.Fatal(defineErr)
				}
				err = chronicle.RegisterEventMigrationFactory(d.registry, d.current.Descriptor(), d.previous.Descriptor(), factory, func(context.Context, *factoryArtifact) (events.MigrationDeclaration, error) {
					return events.DefineMigration(foreign, d.previous, events.Migration[factoryCurrent, factoryPrevious]{Upcast: func(*events.MigrationBuilder[factoryCurrent, factoryPrevious]) {}, Downcast: func(*events.MigrationBuilder[factoryPrevious, factoryCurrent]) {}})
				})
			default:
				err = chronicle.RegisterConstraintFactory(d.registry, []string{"factory-name"}, factory, func(context.Context, *factoryArtifact) ([]constraints.Definition, error) {
					builder := constraints.UniqueValues("factory-name").On(d.current.Descriptor(), "Name")
					if scenario == "constraint runtime message" {
						builder.WithMessageProvider(func(constraints.Violation) string { return "scoped" })
					}
					if scenario == "constraint extra" {
						builder.WithName("unexpected")
					}
					definition, err := builder.Build()
					if scenario == "constraint missing" {
						return nil, nil
					}
					if scenario == "constraint duplicate" {
						return []constraints.Definition{definition, definition}, err
					}
					return []constraints.Definition{definition}, err
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			client, err := chronicle.NewClient(chronicle.WithRegistry(d.registry))
			var preparation *chronicle.PreparationError
			if client != nil || !errors.Is(err, chronicle.ErrInvalidConfiguration) || !errors.As(err, &preparation) || closed != 1 {
				t.Fatalf("client=%v err=%v closed=%d", client, err, closed)
			}
		})
	}
}

func TestDefinitionFactoryDuplicateAdmissionIsAtomic(t *testing.T) {
	for _, factoryFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct first", true: "factory first"}[factoryFirst], func(t *testing.T) {
			d := newFactoryDeclarations(t)
			closed := 0
			a := &factoryArtifact{factoryConfig{label: "configured"}, &closed}
			registerFactory := func() { d.register(t, func() *factoryArtifact { return a }) }
			projection, _ := d.projection(t.Context(), a)
			definitions, _ := d.constraint(t.Context(), a)
			migration := events.Migration[factoryCurrent, factoryPrevious]{Upcast: func(*events.MigrationBuilder[factoryCurrent, factoryPrevious]) {}, Downcast: func(*events.MigrationBuilder[factoryPrevious, factoryCurrent]) {}}
			registerDirect := func(wantError bool) {
				for _, err := range []error{d.registry.AddProjection(projection), d.registry.AddConstraint(definitions[0]), chronicle.RegisterEventMigration(d.registry, d.current, d.previous, migration)} {
					if wantError && !errors.Is(err, chronicle.ErrInvalidConfiguration) || !wantError && err != nil {
						t.Fatal(err)
					}
				}
			}
			if factoryFirst {
				registerFactory()
				registerDirect(true)
			} else {
				registerDirect(false)
				for _, err := range []error{
					chronicle.RegisterProjectionFactory(d.registry, "factory-projection", d.model.Descriptor(), nil, d.projection),
					chronicle.RegisterConstraintFactory(d.registry, []string{"new-name", "factory-name"}, nil, d.constraint),
					chronicle.RegisterEventMigrationFactory(d.registry, d.current.Descriptor(), d.previous.Descriptor(), nil, d.migration),
				} {
					if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
						t.Fatal(err)
					}
				}
			}
			client, err := chronicle.NewClient(chronicle.WithRegistry(d.registry))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			got, err := client.Artifacts("store")
			if err != nil || len(got.Projections) != 1 || len(got.Constraints) != 1 || len(got.Events.Migrations()) != 1 {
				t.Fatal("failed registration changed catalog", err)
			}
		})
	}
}

type defaultDefinition struct{}

func TestDefinitionFactoryNilUsesDefaultConstruction(t *testing.T) {
	d := newFactoryDeclarations(t)
	if err := chronicle.RegisterProjectionFactory(d.registry, "factory-projection", d.model.Descriptor(), nil, func(_ context.Context, value *defaultDefinition) (projections.Declaration, error) {
		if value == nil {
			t.Fatal("nil default instance")
		}
		return projections.ModelBound(d.model, projections.WithIdentifier("factory-projection"), projections.FromEvent(d.current)), nil
	}); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(d.registry))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
