// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/cratis/chronicle.go/contracts/namespaces"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEvictionClientStatesWithoutPreparationCallbacks(t *testing.T) {
	p, err := CaptureClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Client().Close() }()
	for _, state := range []preparationState{preparationCaptured, preparationRunning, preparationFailed} {
		p.Client().preparation = state
		var stateError *ClientStateError
		if err := p.Client().EvictEventStores(); !errors.Is(err, ErrNotPrepared) || !errors.As(err, &stateError) {
			t.Fatal("unprepared state", state, err)
		}
	}
	p.Client().preparation = preparationCaptured
	client, err := p.Prepare(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	before := client.registryOutput
	for range 2 {
		evictStores(t, client)
	}
	if client.registryOutput != before || client.current != nil || len(client.storeOwners) != 0 {
		t.Fatal("empty eviction performed preparation or connection work")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.EvictEventStores(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	var store EventStore
	if _, err := store.RegisterProjection(t.Context(), projections.Declaration{}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("zero store admission changed", err)
	}
	if _, err := store.Identities().Rename(t.Context(), "subject", "name"); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("zero store identity admission changed", err)
	}
}

func TestEvictionKeepsAlreadyCapturedGenerationReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		calls := 0
		client, streams := memoryLifecycleClient(t, func(ctx context.Context, method string) error {
			if method == namespaces.Namespaces_EnsureNamespace_FullMethodName {
				calls++
				if calls == 2 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			return nil
		}, singleRegistrationAttempt())
		old, err := client.EventStore(t.Context(), "store")
		if err != nil {
			t.Fatal(err)
		}
		stream := <-streams
		stream.end <- status.Error(codes.Unavailable, "reconnect")
		<-entered
		evictStores(t, client)
		close(release)
		synctest.Wait()
		if calls != 2 || len(client.storeSnapshot()) != 0 {
			t.Fatal("captured replay canceled or recached")
		}
		client.mu.Lock()
		g := client.current
		client.mu.Unlock()
		if outcome := g.registrations.For(registrationKey(old.name, old.namespace, old.definitionRoot().revision)).Snapshot(); !outcome.IsSuccess() {
			t.Fatal("admitted replay did not finish", outcome)
		}
	})
}

func TestEvictionWhileRootRegistrationIsInFlight(t *testing.T) {
	registry, _ := projectionRegistry(t)
	kernel := runtimeKernel()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	kernel.readModels.(*readModelKernel).register = func(ctx context.Context, _ *modelcontracts.RegisterManyRequest) error {
		if calls.Add(1) == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	old, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	issued := old.ReadModels()
	_, addition := runtimeModel(t)
	done := make(chan error, 1)
	go func() { _, err := old.RegisterProjection(ctx, addition); done <- err }()
	awaitSignal(t, ctx, entered)
	evictStores(t, client)
	if old.ReadModels() == issued || len(old.ReadModels().Catalog().Descriptors()) != 2 {
		t.Fatal("eviction withdrew published root")
	}
	other, err := readmodels.Define[RuntimeOtherModel]()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := old.RegisterProjection(ctx, projections.ModelBound(other)); err != nil || !result.Published {
		t.Fatal(result, err)
	}
	fresh, err := client.EventStore(ctx, "store")
	if err != nil || fresh == old || fresh.definitions != old.definitions || fresh.ReadModels() != old.ReadModels() {
		t.Fatal("new facade lost current root", err)
	}
	close(release)
	if err := receiveOpening(t, ctx, done); err != nil {
		t.Fatal(err)
	}
	if len(fresh.ReadModels().Catalog().Descriptors()) != 3 || len(issued.Catalog().Descriptors()) != 1 {
		t.Fatal("old registration overwrote cumulative root")
	}
}
