// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

type preparationFactory struct {
	open     func(context.Context) (reactors.Scope, error)
	contains func(reflect.Type) bool
}

func (f preparationFactory) NewScope(ctx context.Context) (reactors.Scope, error) { return f.open(ctx) }
func (f preparationFactory) Contains(typ reflect.Type) bool {
	if f.contains != nil {
		return f.contains(typ)
	}
	return typ != reflect.TypeFor[*preparationArtifact]()
}

type preparationScope struct {
	resolve func(context.Context, reflect.Type) (any, error)
	close   func(context.Context) error
}

func (s *preparationScope) Resolve(ctx context.Context, typ reflect.Type) (any, error) {
	return s.resolve(ctx, typ)
}
func (s *preparationScope) Close(ctx context.Context) error { return s.close(ctx) }

type preparationArtifact struct{ close func(context.Context) error }

func (a *preparationArtifact) CloseContext(ctx context.Context) error { return a.close(ctx) }
func (*preparationArtifact) Close() error                             { panic("CloseContext must take precedence") }

type preparationDependency struct{}
type preparationMetadata struct{}
type sensitivePanic struct{ formatted *int }

func (p *sensitivePanic) String() string { (*p.formatted)++; return "secret panic payload" }

func TestDefinitionPreparationCleansPartialScopesAndConstructorResultsBeforeDependencies(t *testing.T) {
	for _, phase := range []string{"open error", "construct error", "define error", "cancel", "cleanup error", "success"} {
		t.Run(phase, func(t *testing.T) {
			registry := NewRegistry()
			model, err := RegisterReadModel[catalogModel](registry)
			if err != nil {
				t.Fatal(err)
			}
			event := declareEvent[catalogEvent](t, registry)
			failure := errors.New("sensitive dependency failure")
			var trace []string
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), preparationMetadata{}, "metadata"))
			defer cancel()
			checkCleanup := func(ctx context.Context) {
				if ctx.Err() != nil || ctx.Value(preparationMetadata{}) != "metadata" {
					t.Fatal("cleanup lost metadata or inherited cancellation")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("cleanup is unbounded")
				}
			}
			services := preparationFactory{open: func(context.Context) (reactors.Scope, error) {
				trace = append(trace, "open")
				scope := &preparationScope{resolve: func(context.Context, reflect.Type) (any, error) {
					trace = append(trace, "dependency")
					return preparationDependency{}, nil
				}, close: func(ctx context.Context) error { checkCleanup(ctx); trace = append(trace, "scope"); return nil }}
				if phase == "open error" {
					cancel()
					return scope, failure
				}
				return scope, nil
			}}
			factory := func(_ context.Context, _ preparationDependency) (*preparationArtifact, error) {
				trace = append(trace, "construct")
				a := &preparationArtifact{close: func(ctx context.Context) error {
					checkCleanup(ctx)
					trace = append(trace, "artifact")
					// Registry locks are not held while releasing an application result.
					if _, err := RegisterEvent[catalogReplacement](registry); err != nil {
						t.Error(err)
					}
					if phase == "cleanup error" {
						return failure
					}
					return nil
				}}
				if phase == "construct error" {
					cancel()
					return a, failure
				}
				return a, nil
			}
			err = RegisterProjectionFactory(registry, "prepared", model.Descriptor(), factory, func(context.Context, *preparationArtifact) (projections.Declaration, error) {
				trace = append(trace, "define")
				if phase == "define error" {
					return projections.Declaration{}, failure
				}
				if phase == "cancel" {
					cancel()
				}
				return projections.ModelBound(model, projections.WithIdentifier("prepared"), projections.FromEvent(event)), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClientContext(ctx, WithRegistry(registry), WithServices(services))
			want := []string{"open", "dependency", "construct", "define", "artifact", "scope"}
			if phase == "open error" {
				want = []string{"open", "scope"}
			}
			if phase == "construct error" {
				want = []string{"open", "dependency", "construct", "artifact", "scope"}
			}
			if !slices.Equal(trace, want) {
				t.Fatalf("trace=%v want=%v", trace, want)
			}
			if phase == "success" {
				if err != nil {
					t.Fatal(err)
				}
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(trace, want) {
					t.Fatal("client reclosed preparation resources")
				}
			} else {
				cause := failure
				if phase == "cancel" {
					cause = context.Canceled
				}
				var prepared *PreparationError
				if client != nil || !errors.Is(err, cause) || !errors.As(err, &prepared) {
					t.Fatalf("client=%v error=%v", client, err)
				}
				for _, text := range []string{fmt.Sprint(err), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
					if strings.Contains(text, "sensitive") {
						t.Fatal("failure leaked application payload")
					}
				}
			}
		})
	}
}

func TestDefinitionPreparationDiscardsPanicsWithoutEverFormattingTheirValues(t *testing.T) {
	for _, kind := range []string{"string", "object", "error"} {
		t.Run(kind, func(t *testing.T) {
			for _, phase := range []string{"validate", "open", "resolve", "construct", "define", "artifact close", "scope close"} {
				t.Run(phase, func(t *testing.T) {
					formatted, closed, scopeClosed := 0, 0, 0
					var payload any = "secret panic payload"
					switch kind {
					case "object":
						payload = &sensitivePanic{&formatted}
					case "error":
						payload = errors.New("secret panic payload")
					}
					registry := NewRegistry()
					model, err := RegisterReadModel[catalogModel](registry)
					if err != nil {
						t.Fatal(err)
					}
					services := preparationFactory{contains: func(typ reflect.Type) bool {
						if phase == "validate" {
							panic(payload)
						}
						return typ != reflect.TypeFor[*preparationArtifact]()
					}, open: func(context.Context) (reactors.Scope, error) {
						if phase == "open" {
							panic(payload)
						}
						return &preparationScope{resolve: func(context.Context, reflect.Type) (any, error) {
							if phase == "resolve" {
								panic(payload)
							}
							return preparationDependency{}, nil
						}, close: func(context.Context) error {
							scopeClosed++
							if phase == "scope close" {
								panic(payload)
							}
							return nil
						}}, nil
					}}
					err = RegisterProjectionFactory(registry, "prepared", model.Descriptor(), func(preparationDependency) *preparationArtifact {
						if phase == "construct" {
							panic(payload)
						}
						return &preparationArtifact{close: func(context.Context) error {
							closed++
							if phase == "artifact close" {
								panic(payload)
							}
							return nil
						}}
					}, func(context.Context, *preparationArtifact) (projections.Declaration, error) {
						if phase == "define" {
							panic(payload)
						}
						return projections.ModelBound(model, projections.WithIdentifier("prepared")), nil
					})
					if err != nil {
						t.Fatal(err)
					}
					client, err := NewClient(WithRegistry(registry), WithServices(services))
					var prepared *PreparationError
					if client != nil || !errors.As(err, &prepared) || !errors.Is(err, ErrInvalidConfiguration) {
						t.Fatalf("panic published a client or lost its category: %v", err)
					}
					if _, exists := reflect.TypeOf(prepared).MethodByName("Recovered"); exists {
						t.Fatal("preparation exposes recovered payloads")
					}
					assertDiscardedPreparationPanic(t, err, payload)
					if formatted != 0 {
						t.Fatal("panic payload was formatted")
					}
					wantScope, wantClosed := 1, 1
					if phase == "open" || phase == "validate" {
						wantScope, wantClosed = 0, 0
					}
					if phase == "construct" || phase == "resolve" {
						wantClosed = 0
					}
					if scopeClosed != wantScope || closed != wantClosed {
						t.Fatalf("scope=%d artifact=%d", scopeClosed, closed)
					}
				})
			}
		})
	}
}

// Inspect all retained fields as well as the public error chain: a payload must
// not merely be hidden by Error/Format, even behind an unexported cause field.
func assertDiscardedPreparationPanic(t *testing.T, err error, payload any) {
	t.Helper()
	original := reflect.ValueOf(payload)
	var inspect func(reflect.Value)
	inspect = func(value reflect.Value) {
		if !value.IsValid() {
			return
		}
		if value.Type() == original.Type() && value.Comparable() && value.Equal(original) {
			t.Fatal("diagnostic retains the panic payload")
		}
		if value.CanInterface() {
			if diagnostic, ok := value.Interface().(error); ok {
				if strings.Contains(fmt.Sprintf("%v %+v %#v %s", diagnostic, diagnostic, diagnostic, diagnostic), "secret panic payload") {
					t.Fatal("diagnostic formats the panic payload")
				}
			}
		}
		switch value.Kind() {
		case reflect.Pointer, reflect.Interface:
			if !value.IsNil() {
				inspect(value.Elem())
			}
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				inspect(value.Field(i))
			}
		case reflect.Slice:
			for i := 0; i < value.Len(); i++ {
				inspect(value.Index(i))
			}
		case reflect.String:
			if strings.Contains(value.String(), "secret panic payload") {
				t.Fatal("diagnostic retains secret text")
			}
		}
	}
	inspect(reflect.ValueOf(err))
}

func TestDefinitionPreparationResolverIsBorrowedGuardedAndExpires(t *testing.T) {
	registry := NewRegistry()
	model, err := RegisterReadModel[catalogModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	event := declareEvent[catalogEvent](t, registry)
	var retained reactors.Scope
	closed := 0
	err = RegisterProjectionFactory(registry, "prepared", model.Descriptor(), func(ctx context.Context, scope reactors.Scope) *preparationArtifact {
		retained = scope
		if err := scope.Close(ctx); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatal("borrower could close preparation scope", err)
		}
		self, err := scope.Resolve(ctx, reflect.TypeFor[reactors.Scope]())
		if err != nil || self != scope {
			t.Fatal("resolver exposed owned scope", err)
		}
		if _, err := scope.Resolve(ctx, reflect.TypeFor[*EventStore]()); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatal("prepared store resolved", err)
		}
		return &preparationArtifact{close: func(context.Context) error { closed++; return nil }}
	}, func(context.Context, *preparationArtifact) (projections.Declaration, error) {
		return projections.ModelBound(model, projections.WithIdentifier("prepared"), projections.FromEvent(event)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := retained.Resolve(t.Context(), reflect.TypeFor[preparationDependency]()); !errors.Is(err, ErrInvalidConfiguration) || closed != 1 {
		t.Fatal("resolver outlived operation", err)
	}
}

func TestDefinitionPreparationRejectsVisibleFacadeDependenciesBeforeOpeningScope(t *testing.T) {
	registry := NewRegistry()
	model, _ := RegisterReadModel[catalogModel](registry)
	if err := RegisterProjectionFactory(registry, "prepared", model.Descriptor(), func(*EventStore) *preparationArtifact { t.Fatal("constructor ran"); return nil }, func(context.Context, *preparationArtifact) (projections.Declaration, error) {
		t.Fatal("define ran")
		return projections.Declaration{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	services := preparationFactory{open: func(context.Context) (reactors.Scope, error) { t.Fatal("scope opened"); return nil, nil }}
	if client, err := NewClient(WithRegistry(registry), WithServices(services)); client != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(client, err)
	}
}

func TestDefinitionDependencyOnlyRejectsRuntimeFacades(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[*Client](), reflect.TypeFor[*EventStore](), reflect.TypeFor[*eventsequences.Sequence]()} {
		if err := definitionDependency(typ); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("facade %s admitted: %v", typ, err)
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[eventsequences.Scope](), reflect.TypeFor[StoreName](), reflect.TypeFor[preparationDependency]()} {
		if err := definitionDependency(typ); err != nil {
			t.Fatalf("configuration %s rejected: %v", typ, err)
		}
	}
}

func TestFactoryConstraintMetadataConflictsWithDeclaredConstraintBeforeActivation(t *testing.T) {
	registry := NewRegistry()
	declareEvent[DeclaredEmail](t, registry)
	if err := RegisterConstraintFactory(registry, []string{"email"}, func() preparationDependency { t.Fatal("constructor ran"); return preparationDependency{} }, func(context.Context, preparationDependency) ([]constraints.Definition, error) {
		t.Fatal("define ran")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if client, err := NewClient(WithRegistry(registry)); client != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(client, err)
	}
}

func TestWithProjectionRemovesFactoryProducerWithoutInvokingIt(t *testing.T) {
	registry := NewRegistry()
	event := declareEvent[catalogEvent](t, registry)
	model, _ := RegisterReadModel[catalogModel](registry)
	if err := RegisterProjectionFactory(registry, "original", model.Descriptor(), nil, func(context.Context, preparationDependency) (projections.Declaration, error) {
		t.Fatal("replaced factory ran")
		return projections.Declaration{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	replaced, err := registry.WithProjection(projections.ModelBound(model, projections.WithIdentifier("replacement"), projections.FromEvent(event)))
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(replaced))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if len(registry.projectionFactories) != 1 || len(replaced.projectionFactories) != 0 || client.projections[0].Identifier() != "replacement" {
		t.Fatal("incorrect producer replacement")
	}
}

// Keep original read-model handles accepted after all detached preparation plans.
func TestFactoryPreparationRetainsOriginalModelHandle(t *testing.T) {
	registry := NewRegistry()
	event := declareEvent[catalogEvent](t, registry)
	model, _ := RegisterReadModel[catalogModel](registry)
	if err := RegisterProjectionFactory(registry, "prepared", model.Descriptor(), nil, func(context.Context, preparationDependency) (projections.Declaration, error) {
		return projections.ModelBound(model, projections.WithIdentifier("prepared"), projections.FromEvent(event)), nil
	}); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, models, err := client.Catalogs("selected")
	if err != nil {
		t.Fatal(err)
	}
	service, err := readmodels.New("selected", "tenant", models, &clientTransport{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := readmodels.For(service, model).Get(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatal("original model declaration rejected", err)
	}
}
