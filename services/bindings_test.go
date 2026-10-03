// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type facadeMetadata struct {
	store     chronicle.StoreName
	namespace chronicle.Namespace
}
type facadeMetadataKey struct{}
type catalogReader interface{ Descriptors() []events.Descriptor }
type facadeApplication struct {
	store *chronicle.EventStore
	log   *eventsequences.Sequence
}

// Chronicle's own services consumer is the named proof; this is not Arc adoption.
func TestFacadeConsumerCaptureBindPrepareAndResolveCoordinates(t *testing.T) {
	ctx := facadeContext(t)
	kernel := &facadeKernel{}
	artifacts := chronicle.NewRegistry()
	if err := chronicle.RegisterSeederFactory[*preparationCollaborator](artifacts, nil); err != nil {
		t.Fatal(err)
	}
	p := captureFacadeClient(t, kernel, chronicle.WithRegistry(artifacts))
	var bindings container.Registry
	var selected atomic.Int32
	bindFacades(t, &bindings, p, func(ctx context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		selected.Add(1)
		m := ctx.Value(facadeMetadataKey{}).(facadeMetadata)
		return m.store, m.namespace, nil
	})
	closed := 0
	if err := dependencyinjection.BindFunc1(&bindings, dependencyinjection.Singleton, func(_ context.Context, client *chronicle.Client) (*preparationCollaborator, error) {
		if client != p.Client() {
			t.Fatal("not the captured client")
		}
		return &preparationCollaborator{client: client, closed: &closed}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := dependencyinjection.BindFunc2(&bindings, dependencyinjection.Scoped, func(_ context.Context, store *chronicle.EventStore, log *eventsequences.Sequence) (*facadeApplication, error) {
		return &facadeApplication{store, log}, nil
	}); err != nil {
		t.Fatal(err)
	}
	// Interface forwarding must resolve the concrete key, not construct again.
	if err := dependencyinjection.BindBorrowed[catalogReader](&bindings, dependencyinjection.Scoped,
		func(ctx context.Context, r dependencyinjection.Resolver) (catalogReader, error) {
			return dependencyinjection.Resolve[*events.Catalog](ctx, r)
		}, dependencyinjection.KeyFor[*events.Catalog]()); err != nil {
		t.Fatal(err)
	}
	provider := facadeProvider(t, &bindings, p.Client())
	if selected.Load() != 0 || kernel.calls.Load() != 0 {
		t.Fatal("build invoked application or SDK work")
	}
	client, err := services.PrepareClient(ctx, p, provider)
	if err != nil || client != p.Client() {
		t.Fatal("preparation identity", err)
	}
	seen := map[facadeMetadata]*chronicle.EventStore{}
	for range 2 {
		for _, storeName := range []chronicle.StoreName{"first", "second"} {
			for _, namespace := range []chronicle.Namespace{"north", "south"} {
				m := facadeMetadata{storeName, namespace}
				operation := context.WithValue(ctx, facadeMetadataKey{}, m)
				scope := facadeScope(t, operation, provider)
				app := resolveFacade[*facadeApplication](t, operation, scope)
				store := resolveFacade[*chronicle.EventStore](t, operation, scope)
				if app.store != store || app.log != store.EventLog() {
					t.Fatal("application received different facades")
				}
				if got := resolveFacade[*chronicle.Client](t, operation, scope); got != p.Client() {
					t.Fatal("client identity changed")
				}
				if store.Name() != storeName || store.Namespace() != namespace {
					t.Fatal("selection lost coordinates")
				}
				if got := resolveFacade[*events.Catalog](t, operation, scope); got != store.EventTypes() {
					t.Fatal("event catalog reconstructed")
				}
				if got := resolveFacade[catalogReader](t, operation, scope); got != store.EventTypes() {
					t.Fatal("interface forwarding reconstructed catalog")
				}
				if got := resolveFacade[*readmodels.Service](t, operation, scope); got != store.ReadModels() {
					t.Fatal("read models reconstructed")
				}
				if got := resolveFacade[*compliance.Manager](t, operation, scope); got != store.Compliance() {
					t.Fatal("compliance reconstructed")
				}
				plain, err := client.EventStore(operation, storeName, chronicle.WithNamespace(namespace))
				if err != nil || plain != store {
					t.Fatal("plain/provider store identity", err)
				}
				if prior := seen[m]; prior != nil && prior != store {
					t.Fatal("client cache not reused")
				}
				for other, prior := range seen {
					if other != m && prior == store {
						t.Fatal("coordinate aliasing")
					}
				}
				seen[m] = store
				if err := scope.Close(operation); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if selected.Load() != 8 {
		t.Fatalf("selector calls = %d, want 8", selected.Load())
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatal("provider collaborator was not closed once")
	}
	// Deliberately invert normal shutdown in this ownership test. The application
	// has joined scopes and still owns a live SDK after provider closure.
	if _, err := client.EventStore(ctx, "after-provider-close"); err != nil {
		t.Fatal("provider closed borrowed client", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EventStore(ctx, "after-client-close"); !errors.Is(err, chronicle.ErrClosed) {
		t.Fatal(err)
	}
}

func TestFacadeUnpreparedDenialFreezesSelectionWithoutSDKIOOrPoisoning(t *testing.T) {
	ctx := facadeContext(t)
	kernel := &facadeKernel{}
	p := captureFacadeClient(t, kernel)
	var bindings container.Registry
	calls := 0
	bindFacades(t, &bindings, p, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		calls++
		return "first", "north", nil
	})
	provider := facadeProvider(t, &bindings, p.Client())
	scope := facadeScope(t, ctx, provider)
	for range 2 {
		if _, err := dependencyinjection.Resolve[*eventsequences.Sequence](ctx, scope); !errors.Is(err, chronicle.ErrNotPrepared) {
			t.Fatal(err)
		}
	}
	if calls != 1 || kernel.calls.Load() != 0 {
		t.Fatal("denial performed SDK I/O or reselected", calls, kernel.calls.Load())
	}
	if _, err := services.PrepareClient(ctx, p, provider); err != nil {
		t.Fatal("denial poisoned Prepare", err)
	}
	store := resolveFacade[*chronicle.EventStore](t, ctx, scope)
	if calls != 1 || store.Name() != "first" {
		t.Fatal("preparation discarded selection")
	}
}

func TestFacadeFailedRegistrationRetriesFrozenCoordinates(t *testing.T) {
	ctx := facadeContext(t)
	kernel := &facadeKernel{}
	p := captureFacadeClient(t, kernel)
	var bindings container.Registry
	calls := 0
	bindFacades(t, &bindings, p, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		calls++
		return "first", "north", nil
	})
	provider := facadeProvider(t, &bindings, p.Client())
	if _, err := services.PrepareClient(ctx, p, provider); err != nil {
		t.Fatal(err)
	}
	scope := facadeScope(t, ctx, provider)
	kernel.failNamespace.Store(true)
	if _, err := dependencyinjection.Resolve[*chronicle.EventStore](ctx, scope); err == nil {
		t.Fatal("registration failure lost")
	}
	store := resolveFacade[*chronicle.EventStore](t, ctx, scope)
	if calls != 1 || store.Name() != "first" || store.Namespace() != "north" {
		t.Fatal("retried selector")
	}
	kernel.mu.Lock()
	defer kernel.mu.Unlock()
	if len(kernel.coordinates) != 2 {
		t.Fatal("namespace registration attempts", kernel.coordinates)
	}
	for _, m := range kernel.coordinates {
		if m != [2]string{"first", "north"} {
			t.Fatal("registration changed selection")
		}
	}
}

func TestFacadeConcurrentWaitersDoNotCancelSelectorOwner(t *testing.T) {
	ctx := facadeContext(t)
	p := captureFacadeClient(t, &facadeKernel{})
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var bindings container.Registry
	bindFacades(t, &bindings, p, func(owner context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		calls.Add(1)
		if owner.Value(facadeMetadataKey{}) != "owner" {
			t.Error("caller metadata discarded")
		}
		close(started)
		select {
		case <-release:
		case <-owner.Done():
			return "", "", owner.Err()
		}
		return "first", "north", nil
	})
	provider := facadeProvider(t, &bindings, p.Client())
	if _, err := services.PrepareClient(ctx, p, provider); err != nil {
		t.Fatal(err)
	}
	scope := facadeScope(t, ctx, provider)
	ownerResult := make(chan error, 1)
	go func() {
		_, err := dependencyinjection.Resolve[*chronicle.EventStore](context.WithValue(ctx, facadeMetadataKey{}, "owner"), scope)
		ownerResult <- err
	}()
	<-started
	waiter, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := dependencyinjection.Resolve[*chronicle.EventStore](waiter, scope); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	const waiters = 12
	var join sync.WaitGroup
	results := make(chan *chronicle.EventStore, waiters)
	for range waiters {
		join.Go(func() {
			store, err := dependencyinjection.Resolve[*chronicle.EventStore](ctx, scope)
			if err != nil {
				t.Error(err)
			}
			results <- store
		})
	}
	close(release)
	if err := <-ownerResult; err != nil {
		t.Fatal("waiter canceled owner", err)
	}
	join.Wait()
	close(results)
	store := resolveFacade[*chronicle.EventStore](t, ctx, scope)
	for got := range results {
		if got != store {
			t.Fatal("concurrent handles differ")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("selector reran", calls.Load())
	}
}

func TestFacadeClientClosedBeforeFirstScopeAccess(t *testing.T) {
	ctx := facadeContext(t)
	p := captureFacadeClient(t, &facadeKernel{})
	var bindings container.Registry
	bindFacades(t, &bindings, p, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) { return "first", "north", nil })
	provider := facadeProvider(t, &bindings, p.Client())
	if _, err := services.PrepareClient(ctx, p, provider); err != nil {
		t.Fatal(err)
	}
	if err := p.Client().Close(); err != nil {
		t.Fatal(err)
	}
	scope := facadeScope(t, ctx, provider)
	if _, err := dependencyinjection.Resolve[*eventsequences.Sequence](ctx, scope); !errors.Is(err, chronicle.ErrClosed) {
		t.Fatal(err)
	}
}

func TestFacadeSeparateProvidersDoNotShareStoreCache(t *testing.T) {
	ctx := facadeContext(t)
	stores := make([]*chronicle.EventStore, 2)
	for i := range stores {
		p := captureFacadeClient(t, &facadeKernel{})
		var bindings container.Registry
		bindFacades(t, &bindings, p, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) { return "first", "north", nil })
		provider := facadeProvider(t, &bindings, p.Client())
		if _, err := services.PrepareClient(ctx, p, provider); err != nil {
			t.Fatal(err)
		}
		stores[i] = resolveFacade[*chronicle.EventStore](t, ctx, facadeScope(t, ctx, provider))
	}
	if stores[0] == stores[1] || stores[0].EventLog() == stores[1].EventLog() {
		t.Fatal("adapter shared a global cache")
	}
}

// Declared self/captive dependencies fail without executing factories/selectors.
// Hidden captured resolvers are prohibited by StoreSelector's contract.
func TestFacadeDeclaredCyclesAndSingletonCapturesFailAtBuild(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		t.Run(map[bool]string{false: "singleton captures scope", true: "cycle"}[cycle], func(t *testing.T) {
			p, err := chronicle.CaptureClient()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Client().Close() }()
			var bindings container.Registry
			bindFacades(t, &bindings, p, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
				t.Fatal("selector ran at Build")
				return "", "", nil
			})
			deps := []dependencyinjection.Key{dependencyinjection.KeyFor[*chronicle.EventStore]()}
			want := dependencyinjection.ErrCaptiveLifetime
			if cycle {
				deps = []dependencyinjection.Key{dependencyinjection.KeyFor[*facadeApplication]()}
				want = dependencyinjection.ErrCycle
			}
			if err := dependencyinjection.Bind(&bindings, dependencyinjection.Singleton, func(context.Context, dependencyinjection.Resolver) (*facadeApplication, error) {
				t.Fatal("factory ran at Build")
				return nil, nil
			}, deps...); err != nil {
				t.Fatal(err)
			}
			if _, err := bindings.Build(); !errors.Is(err, want) {
				t.Fatal(err)
			}
		})
	}
}
