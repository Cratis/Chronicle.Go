// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
	"github.com/cratis/chronicle.go/serialization"
)

func TestCaptureRegistryClonesEveryDeclarationCollectionWithoutCallbacks(t *testing.T) {
	registry := &Registry{
		projectionFactories: []projectionFactoryDeclaration{{}}, constraintFactories: []constraintFactoryDeclaration{{}}, migrationFactories: []migrationFactoryDeclaration{{}},
		descriptors: []events.Descriptor{{}}, migrations: []events.MigrationDeclaration{{}},
		constraints: []constraints.Definition{{}}, constraintCompositions: []constraintComposition{{configure: func(*constraints.Builder) { t.Fatal("capture invoked composition") }}},
		readModels: []readmodels.Descriptor{{}}, projections: []projections.Declaration{{}},
		reactors: []reactorDeclaration{{}}, readModelReactors: []reactors.ReadModelDeclaration{{}},
		reducers: []reducers.Declaration{{}}, seeders: []seederDeclaration{{instance: seeding.Func(func(*seeding.Builder) error { t.Fatal("capture invoked seeder"); return nil })}},
		reactorMiddlewares: []any{"middleware"}, reactorSideEffects: []reactorSideEffectHandler{nil},
	}
	captured := captureRegistry(registry)
	original, detached := reflect.ValueOf(registry).Elem(), reflect.ValueOf(captured).Elem()
	for i := 0; i < original.NumField(); i++ {
		name := original.Type().Field(i).Name
		if name == "mu" {
			continue
		}
		field := detached.FieldByName(name)
		if !field.IsValid() || field.Type() != original.Field(i).Type() || field.Len() != 1 {
			t.Fatalf("snapshot must preserve registry collection %s", name)
		}
		if field.Pointer() == original.Field(i).Pointer() {
			t.Errorf("snapshot aliases registry collection %s", name)
		}
	}
	if detached.NumField() != original.NumField()-1 {
		t.Fatal("snapshot and registry declaration fields differ")
	}
	if captureRegistry(nil).hasObservers() {
		t.Fatal("nil registry contains observers")
	}
}

func TestCompileRegistryPreservesCapturedDeclarationsAndModelIdentity(t *testing.T) {
	registry, model, _ := artifactRegistry(t)
	captured, before := captureRegistry(registry), captureRegistry(registry)
	frozen, err := compileRegistry(t.Context(), captured, serialization.CamelCase, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(captured, before) {
		t.Fatal("compilation mutated captured authoring declarations")
	}
	service, err := readmodels.New("store", "tenant", frozen.models, &clientTransport{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := readmodels.For(service, model).Get(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("original model handle was not admitted before cancellation: %v", err)
	}
}

// Both the earliest composition callback and the later seeder callback must see
// all selected stores already captured. New declarations belong to the next client.
func TestClientCapturesAllRegistriesBeforeApplicationPreparation(t *testing.T) {
	for _, phase := range []string{"constraint composition", "seeder", "event classifier", "model classifier", "service catalog", "scope open", "constructor", "definition", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			defaults := NewRegistry()
			stores := []*Registry{catalogRegistry(t), catalogRegistry(t)}
			var capturedSeeds, lateSeeds int
			for _, registry := range stores {
				if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { capturedSeeds++; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			mutate := func() error {
				for _, registry := range stores {
					if err := RegisterReactorHandler(registry, "late-observer", func(context.Context, catalogEvent) error { return nil }); err != nil {
						return err
					}
					if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { lateSeeds++; return nil }); err != nil {
						return err
					}
					event, err := RegisterEvent[catalogReplacement](registry, events.WithGeneration(2))
					if err != nil {
						return err
					}
					model, err := RegisterReadModel[catalogReplacement](registry)
					if err != nil {
						return err
					}
					if err := RegisterProjectionFactory(registry, "late-factory", model.Descriptor(), nil, func(context.Context, preparationDependency) (projections.Declaration, error) {
						t.Fatal("late projection factory ran")
						return projections.ModelBound(model, projections.WithIdentifier("late-factory"), projections.FromEvent(event)), nil
					}); err != nil {
						return err
					}
					if err := RegisterConstraintFactory(registry, []string{"late-constraint"}, nil, func(context.Context, preparationDependency) ([]constraints.Definition, error) {
						t.Fatal("late constraint factory ran")
						return nil, nil
					}); err != nil {
						return err
					}
					previous, err := RegisterEventGeneration[snapshotReplacementPrevious](registry, event, 1)
					if err != nil {
						return err
					}
					if err := RegisterEventMigrationFactory(registry, event.Descriptor(), previous.Descriptor(), nil, func(context.Context, preparationDependency) (events.MigrationDeclaration, error) {
						t.Fatal("late migration factory ran")
						return events.MigrationDeclaration{}, nil
					}); err != nil {
						return err
					}
				}
				return nil
			}
			var mutationErr error
			mutated := false
			mutateOnce := func() {
				if !mutated {
					mutated = true
					mutationErr = mutate()
				}
			}
			var options []ClientOption
			switch phase {
			case "event classifier", "model classifier":
				armed := false
				classifier := compliance.Using(func(compliance.Target) (compliance.Classification, error) {
					if armed {
						mutateOnce()
					}
					return compliance.Classification{}, mutationErr
				})
				if phase == "event classifier" {
					if _, err := RegisterEvent[catalogEvent](defaults, events.WithProtection(classifier)); err != nil {
						t.Fatal(err)
					}
				} else if _, err := RegisterReadModel[catalogModel](defaults, readmodels.WithProtection(classifier)); err != nil {
					t.Fatal(err)
				}
				armed = true
			case "constraint composition":
				declareEvent[DeclaredEmail](t, defaults)
				if err := defaults.ConfigureDeclaredConstraint("email", func(*constraints.Builder) { mutationErr = mutate() }); err != nil {
					t.Fatal(err)
				}
			case "seeder":
				if err := RegisterSeederFunc(defaults, func(*seeding.Builder) error { mutationErr = mutate(); return mutationErr }); err != nil {
					t.Fatal(err)
				}
			default:
				event := declareEvent[catalogEvent](t, defaults)
				model, err := RegisterReadModel[catalogModel](defaults)
				if err != nil {
					t.Fatal(err)
				}
				services := preparationFactory{
					contains: func(reflect.Type) bool {
						if phase == "service catalog" {
							mutateOnce()
						}
						return false
					},
					open: func(context.Context) (reactors.Scope, error) {
						if phase == "scope open" {
							mutateOnce()
						}
						return &preparationScope{close: func(context.Context) error {
							if phase == "cleanup" {
								mutateOnce()
							}
							return mutationErr
						}}, nil
					},
				}
				options = append(options, WithServices(services))
				if err := RegisterProjectionFactory(defaults, "prepared", model.Descriptor(), func() *preparationArtifact {
					if phase == "constructor" {
						mutateOnce()
					}
					return &preparationArtifact{close: func(context.Context) error { return nil }}
				}, func(context.Context, *preparationArtifact) (projections.Declaration, error) {
					if phase == "definition" {
						mutateOnce()
					}
					return projections.ModelBound(model, projections.WithIdentifier("prepared"), projections.FromEvent(event)), mutationErr
				}); err != nil {
					t.Fatal(err)
				}
			}
			options = append(options, WithSkipKeepAlive(), WithRegistry(defaults), WithRegistryForStore("a", stores[0]), WithRegistryForStore("z", stores[1]), WithRegistryForStore("empty", nil))
			client, err := NewClient(options...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			if mutationErr != nil || capturedSeeds != 2 || lateSeeds != 0 {
				t.Fatalf("mutation error=%v captured seed calls=%d late seed calls=%d", mutationErr, capturedSeeds, lateSeeds)
			}
			for _, name := range []StoreName{"a", "z"} {
				eventCatalog, modelCatalog, err := client.Catalogs(name)
				if err != nil || len(eventCatalog.Descriptors()) != 1 || len(modelCatalog.Descriptors()) != 1 || len(client.reactors.stores[name]) != 0 || len(client.storeProjections[name]) != 1 {
					t.Fatalf("store %s did not retain captured declarations: %v", name, err)
				}
			}
			emptyEvents, emptyModels, err := client.Catalogs("empty")
			if err != nil || len(emptyEvents.Descriptors()) != 0 || len(emptyModels.Descriptors()) != 0 {
				t.Fatal("nil replacement inherited defaults", err)
			}
			// The caller-owned registries retain the new declarations. A future skip
			// client rejects them before any old or new preparation callback runs.
			for _, registry := range stores {
				if next, err := NewClient(WithSkipKeepAlive(), WithRegistry(registry)); next != nil || !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("future client ignored registered observer: %v", err)
				}
			}
			if capturedSeeds != 2 || lateSeeds != 0 {
				t.Fatal("rejected future snapshot executed preparation")
			}
		})
	}
}

func TestClientCaptureIgnoresConcurrentRegistryMutationDuringPreparation(t *testing.T) {
	defaults, selected := NewRegistry(), catalogRegistry(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var seeds atomic.Int32
	if err := RegisterSeederFunc(defaults, func(*seeding.Builder) error { close(entered); <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	type result struct {
		client *Client
		err    error
	}
	finished := make(chan result, 1)
	go func() {
		client, err := NewClient(WithSkipKeepAlive(), WithRegistry(defaults), WithRegistryForStore("selected", selected))
		finished <- result{client, err}
	}()
	// Cleanup always unblocks and joins the task-owned constructor, even on failure.
	t.Cleanup(func() {
		close(release)
		got := <-finished
		if got.err != nil {
			t.Error(got.err)
			return
		}
		defer func() {
			if err := got.client.Close(); err != nil {
				t.Error(err)
			}
		}()
		if len(got.client.reactors.stores["selected"]) != 0 || seeds.Load() != 0 {
			t.Error("concurrent mutation changed frozen admission or executed late seeder")
		}
	})
	<-entered
	if err := RegisterReactorHandler(selected, "concurrent-observer", func(context.Context, catalogEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSeederFunc(selected, func(*seeding.Builder) error { seeds.Add(1); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestSharedRegistryIsCapturedAndCompiledOncePerClient(t *testing.T) {
	registry := catalogRegistry(t)
	calls := 0
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error {
		calls++
		return RegisterReactorHandler(registry, "next-client-observer", func(context.Context, catalogEvent) error { return nil })
	}); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithSkipKeepAlive(), WithRegistry(registry), WithRegistryForStore("a", registry), WithRegistryForStore("z", registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	if calls != 1 || client.catalog != client.catalogs["a"] || client.catalog != client.catalogs["z"] || len(client.reactors.defaults) != 0 || len(client.reactors.stores["a"])+len(client.reactors.stores["z"]) != 0 {
		t.Fatal("shared registry was recaptured or compiled more than once")
	}
}

func TestInvalidCapturedObserverRejectsAllApplicationPreparation(t *testing.T) {
	defaults, selected := NewRegistry(), catalogRegistry(t)
	declareEvent[DeclaredEmail](t, defaults)
	calls := 0
	if err := defaults.ConfigureDeclaredConstraint("email", func(*constraints.Builder) { calls++ }); err != nil {
		t.Fatal(err)
	}
	for _, registry := range []*Registry{defaults, selected} {
		if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { calls++; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := RegisterSeederFactory[*declarationSeeder](registry, func() *declarationSeeder { calls++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := RegisterReactorHandler(selected, "invalid-observer", func(context.Context, catalogEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	factory := &snapshotPreparationScopes{}
	client, err := NewClient(WithSkipKeepAlive(), WithRegistry(defaults), WithRegistryForStore("selected", selected), WithServices(factory))
	if client != nil || !errors.Is(err, ErrInvalidConfiguration) || calls != 0 || factory.opened != 0 {
		t.Fatalf("client=%v error=%v preparation calls=%d opened scopes=%d", client, err, calls, factory.opened)
	}
}

type snapshotPreparationScopes struct{ opened int }

func (f *snapshotPreparationScopes) NewScope(context.Context) (reactors.Scope, error) {
	f.opened++
	return nil, errors.New("preparation scope must not be opened")
}
