// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	di "github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type preparedModel struct{ ID string }
type preparedArtifact struct{ close func() error }

func (a *preparedArtifact) Close() error { return a.close() }

type countedProvider struct {
	di.ScopeFactory
	di.Catalog
	opened, closed int
}

func (p *countedProvider) NewScope(ctx context.Context) (di.Scope, error) {
	scope, err := p.ScopeFactory.NewScope(ctx)
	if scope != nil {
		p.opened++
		return countedScope{scope, &p.closed}, err
	}
	return nil, err
}

type countedScope struct {
	di.Scope
	closed *int
}

func (s countedScope) Close(ctx context.Context) error { (*s.closed)++; return s.Scope.Close(ctx) }

func TestDefaultProviderPreparationDiscardsFactoryAndDisposalPanics(t *testing.T) {
	for _, kind := range []string{"string", "object", "error"} {
		for _, phase := range []string{"factory", "scope disposal", "partial result disposal", "partial result panic alias", "define and disposal"} {
			t.Run(kind+"/"+phase, func(t *testing.T) {
				formatted, constructed, closed, defined := 0, 0, 0, 0
				secret := &secretFailure{&formatted}
				var payload any = "secret panic payload"
				switch kind {
				case "object":
					payload = &struct{ Secret string }{"secret panic payload"}
				case "error":
					payload = secret
				}
				ordinary := &ordinaryFailure{}
				var bindings container.Registry
				if err := di.Bind(&bindings, di.Scoped, func(context.Context, di.Resolver) (*preparedArtifact, error) {
					constructed++
					if phase == "factory" {
						panic(payload)
					}
					artifact := &preparedArtifact{close: func() error { closed++; panic(payload) }}
					if phase == "partial result disposal" {
						return artifact, ordinary
					}
					if phase == "partial result panic alias" {
						artifact.close = func() error { closed++; panic(secret) }
						return artifact, errors.Join(secret, ordinary)
					}
					return artifact, nil
				}); err != nil {
					t.Fatal(err)
				}
				provider, err := bindings.Build()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := provider.Close(context.Background()); err != nil {
						t.Error("provider retained an already disposed scoped value")
					}
				})
				counted := &countedProvider{ScopeFactory: provider, Catalog: provider}
				registry := chronicle.NewRegistry()
				model, err := chronicle.RegisterReadModel[preparedModel](registry)
				if err != nil {
					t.Fatal(err)
				}
				if err := chronicle.RegisterProjectionFactory(registry, "prepared", model.Descriptor(), func() *preparedArtifact {
					t.Fatal("registered provider service did not take precedence")
					return nil
				}, func(context.Context, *preparedArtifact) (projections.Declaration, error) {
					defined++
					if phase == "define and disposal" {
						return projections.Declaration{}, ordinary
					}
					return projections.ModelBound(model, projections.WithIdentifier("prepared")), nil
				}); err != nil {
					t.Fatal(err)
				}
				client, err := chronicle.NewClient(chronicle.WithRegistry(registry), WithServices(counted))
				var preparation *chronicle.PreparationError
				if client != nil || !errors.As(err, &preparation) || !errors.Is(err, di.ErrCallbackPanicked) {
					t.Fatal("panic published a client or lost its category", err)
				}
				assertSafeProviderDiagnostics(t, err)
				var leaked *secretFailure
				if errors.Is(err, secret) || errors.As(err, &leaked) || formatted != 0 {
					t.Fatal("panic error is still reachable or was formatted")
				}
				if phase == "partial result disposal" || phase == "partial result panic alias" || phase == "define and disposal" {
					var cause *ordinaryFailure
					if !errors.Is(err, ordinary) || !errors.As(err, &cause) || cause != ordinary {
						t.Fatal("joined ordinary failure lost", err)
					}
				}
				wantClosed, wantDefined := 1, 1
				switch phase {
				case "factory":
					wantClosed, wantDefined = 0, 0
				case "partial result disposal", "partial result panic alias":
					wantDefined = 0
				}
				if constructed != 1 || closed != wantClosed || defined != wantDefined || counted.opened != 1 || counted.closed != 1 {
					t.Fatalf("constructed=%d closed=%d defined=%d scopes=%d/%d", constructed, closed, defined, counted.opened, counted.closed)
				}
			})
		}
	}
}

type panickingPartialScope struct{ closed int }

func (*panickingPartialScope) Resolve(context.Context, di.Key) (any, error) { return nil, nil }
func (s *panickingPartialScope) Close(context.Context) error {
	s.closed++
	panic("secret panic payload")
}

func TestPartialScopeCleanupPanicDoesNotHideOpenFailureOrPublishClient(t *testing.T) {
	scope := &panickingPartialScope{}
	ordinary := &ordinaryFailure{}
	registry := chronicle.NewRegistry()
	model, err := chronicle.RegisterReadModel[preparedModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterProjectionFactory(registry, "prepared", model.Descriptor(), func() *preparedArtifact {
		t.Fatal("constructor ran after failed open")
		return nil
	}, func(context.Context, *preparedArtifact) (projections.Declaration, error) {
		t.Fatal("define ran after failed open")
		return projections.Declaration{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), WithServices(partialScopeFactory{scope, ordinary}))
	if client != nil || scope.closed != 1 || !errors.Is(err, ordinary) || !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal("partial scope leaked or failure lost", err)
	}
	assertSafeProviderDiagnostics(t, err)
}

func TestPartialScopeIsClosedWhenOpenErrorInspectionPanics(t *testing.T) {
	for _, openErr := range []error{panickingUnwrap{}, &di.Error{Operation: "new-scope", Kind: di.ErrCallbackPanicked, Panic: "secret panic payload"}} {
		t.Run("partial open", func(t *testing.T) {
			scope := &partialScope{}
			registry := chronicle.NewRegistry()
			model, err := chronicle.RegisterReadModel[preparedModel](registry)
			if err != nil {
				t.Fatal(err)
			}
			if err := chronicle.RegisterProjectionFactory(registry, "prepared", model.Descriptor(), func() *preparedArtifact {
				t.Fatal("constructor ran after failed open")
				return nil
			}, func(context.Context, *preparedArtifact) (projections.Declaration, error) {
				t.Fatal("define ran after failed open")
				return projections.Declaration{}, nil
			}); err != nil {
				t.Fatal(err)
			}
			client, err := chronicle.NewClient(chronicle.WithRegistry(registry), WithServices(partialScopeFactory{scope, openErr}))
			if client != nil || scope.closed != 1 || !errors.Is(err, di.ErrCallbackPanicked) {
				t.Fatal("partial scope leaked or failure lost", err)
			}
			assertSafeProviderDiagnostics(t, err)
		})
	}
}
