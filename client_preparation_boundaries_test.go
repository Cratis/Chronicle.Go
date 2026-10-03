// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/seeding"
)

func TestClientValueAndSDKResultPreflightNeverActivatesOrCopiesClient(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[Client](), reflect.TypeFor[*eventsequences.Sequence](), reflect.TypeFor[*readmodels.Service](), reflect.TypeFor[*compliance.Manager]()} {
		t.Run(typ.String(), func(t *testing.T) {
			registry := NewRegistry()
			fn := reflect.MakeFunc(reflect.FuncOf([]reflect.Type{typ}, []reflect.Type{reflect.TypeFor[*clientDependentSeeder]()}, false), func([]reflect.Value) []reflect.Value {
				t.Error("invalid dependency constructor invoked")
				return []reflect.Value{reflect.ValueOf(&clientDependentSeeder{})}
			}).Interface()
			if typ == reflect.TypeFor[Client]() || typ == reflect.TypeFor[*eventsequences.Sequence]() {
				if err := RegisterSeederFactory[*clientDependentSeeder](registry, fn); err != nil {
					t.Fatal(err)
				}
			} else {
				registry.constraintFactories = []constraintFactoryDeclaration{{definitionFactory: definitionFactory{typ: typ}, names: []string{"owned"}}}
			}
			p := captureForTest(t, WithRegistry(registry))
			factory := preparationFactory{contains: func(typ reflect.Type) bool { return typ != reflect.TypeFor[*clientDependentSeeder]() }, open: func(context.Context) (reactors.Scope, error) { t.Error("scope opened"); return nil, nil }}
			if client, err := p.Prepare(t.Context(), factory); client != nil || !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatal(client, err)
			}
		})
	}
	registry := NewRegistry()
	var p *ClientPreparation
	if err := RegisterConstraintFactory(registry, []string{"owned"}, func() (*Client, error) {
		t.Error("owned client result invoked")
		return p.Client(), errors.New("partial failure")
	}, func(context.Context, *Client) ([]constraints.Definition, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	p = captureForTest(t, WithRegistry(registry))
	if _, err := p.Prepare(t.Context(), nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if p.Client().closed {
		t.Fatal("borrowed client was disposed as an artifact")
	}
}

func TestInterfaceArtifactResultCannotAcquireBorrowedClientOwnership(t *testing.T) {
	for _, partial := range []bool{false, true} {
		registry := NewRegistry()
		var p *ClientPreparation
		var cause error
		if partial {
			cause = errors.New("partial constructor failure")
		}
		if err := RegisterConstraintFactory[any](registry, []string{"owned"}, func() (any, error) {
			return p.Client(), cause
		}, func(context.Context, any) ([]constraints.Definition, error) {
			t.Error("SDK artifact reached define")
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		p = captureForTest(t, WithRegistry(registry))
		if client, err := p.Prepare(t.Context(), nil); client != nil || !errors.Is(err, ErrInvalidConfiguration) || (partial && !errors.Is(err, cause)) {
			t.Fatal(client, err)
		}
		if p.Client().closed {
			t.Fatal("interface result transferred borrowed client cleanup ownership")
		}
	}
}

func TestPreparationCleanupFailurePublishesNoDefaultOrStoreSnapshots(t *testing.T) {
	first, later := NewRegistry(), NewRegistry()
	declareEvent[catalogEvent](t, first)
	declareEvent[catalogReplacement](t, later)
	failure := errors.New("sensitive cleanup failure")
	closed := 0
	if err := RegisterSeederFactory[*clientDependentSeeder](later, func() *clientDependentSeeder { return &clientDependentSeeder{} }); err != nil {
		t.Fatal(err)
	}
	factory := preparationFactory{contains: func(reflect.Type) bool { return false }, open: func(context.Context) (reactors.Scope, error) {
		return &preparationScope{close: func(context.Context) error { closed++; return failure }}, nil
	}}
	p := captureForTest(t, WithRegistry(first), WithRegistryForStore("later", later))
	client, err := p.Prepare(t.Context(), factory)
	if client != nil || !errors.Is(err, failure) || closed != 1 {
		t.Fatal(client, err, closed)
	}
	if p.Client().catalog != nil || len(p.Client().catalogs) != 0 {
		t.Fatal("partially published catalogs")
	}
	for _, store := range []StoreName{"default", "later"} {
		if _, _, err := p.Client().Catalogs(store); !errors.Is(err, ErrNotPrepared) {
			t.Fatal(err)
		}
	}
	if _, again := p.Prepare(t.Context(), nil); again != err || closed != 1 {
		t.Fatal("cleanup attempt reran", again)
	}
}

func TestCaptureAndPreparationPerformNoSDKTransportIO(t *testing.T) {
	transport := &shutdownDispatchConn{}
	optionsCalled := 0
	p := captureForTest(t, func(*clientConfig) { optionsCalled++ }, func(c *clientConfig) { c.borrowed, c.borrowedSet = transport, true }, WithNoAuthentication())
	assertPreparationGuards(t, p.Client(), ErrNotPrepared)
	if optionsCalled != 1 {
		t.Fatal("ordinary client option not applied")
	}
	if _, err := p.Prepare(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.Client().Catalogs("store"); err != nil {
		t.Fatal(err)
	}
	if transport.calls.Load() != 0 {
		t.Fatal("preparation performed SDK transport IO")
	}
}

func TestPreparedIdentityChecksDirectRuntimeClientResolutions(t *testing.T) {
	p := captureForTest(t)
	other := captureForTest(t)
	factory := preparationFactory{open: func(context.Context) (reactors.Scope, error) {
		return &preparationScope{resolve: func(context.Context, reflect.Type) (any, error) { return other.Client(), nil }, close: func(context.Context) error { return nil }}, nil
	}}
	if _, err := p.Prepare(t.Context(), factory); err != nil {
		t.Fatal(err)
	}
	scope, err := p.Client().config.reactorServices.NewScope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scope.Close(context.Background()) }()
	if _, err := scope.Resolve(t.Context(), reflect.TypeFor[*Client]()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("runtime accepted foreign client", err)
	}
	if _, err := scope.Resolve(t.Context(), reflect.TypeFor[Client]()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("runtime allowed copying client", err)
	}
	if err := artifacts.ValidateService(artifacts.DefaultScopeFactory(), reflect.TypeFor[*Client]()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("default factory can fabricate client", err)
	}
}

func TestSuccessfulPreparationDoesNotRetainCallerCancellation(t *testing.T) {
	registry := NewRegistry()
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { return nil }); err != nil {
		t.Fatal(err)
	}
	p := captureForTest(t, WithRegistry(registry))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if _, err := p.Prepare(ctx, nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	if p.Client().life.Err() != nil {
		t.Fatal("caller cancellation became client lifetime")
	}
	if _, _, err := p.Client().Catalogs("store"); err != nil {
		t.Fatal(err)
	}
}
