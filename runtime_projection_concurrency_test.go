// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestRuntimeConcurrentAddCannotDispatchObsoleteSubset(t *testing.T) {
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
	requests := make(chan *contracts.RegisterRequest, 8)
	kernel.projections.(*projectionKernel).register = func(_ context.Context, r *contracts.RegisterRequest) error { requests <- r; return nil }
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	<-requests
	_, first := runtimeModel(t)
	type completion struct {
		result ProjectionRegistration
		err    error
	}
	done := make(chan completion, 1)
	go func() { result, err := store.RegisterProjection(ctx, first); done <- completion{result, err} }()
	awaitSignal(t, ctx, entered)
	second, err := readmodels.Define[RuntimeOtherModel]()
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.RegisterProjection(ctx, projections.ModelBound(second))
	if err != nil || !result.Published || !result.Outcome.IsSuccess() {
		t.Fatal(result, err)
	}
	close(release)
	finished := <-done
	if finished.err != nil || !finished.result.Published || !finished.result.Outcome.IsSuccess() {
		t.Fatal(finished)
	}
	request := <-requests
	if !request.FullSet || len(request.Projections) != 3 {
		t.Fatal("accumulated additions sent stale subset")
	}
	select {
	case request := <-requests:
		t.Fatalf("obsolete addition dispatched: %v", request)
	default:
	}
	if len(store.ReadModels().Catalog().Descriptors()) != 3 {
		t.Fatal("root lost concurrent addition")
	}
}

func TestRuntimePublishedCancellationRetainsRootAndReportsActualStage(t *testing.T) {
	for _, stage := range []string{"read-models", "projections"} {
		t.Run(stage, func(t *testing.T) {
			registry, _ := projectionRegistry(t)
			kernel := runtimeKernel()
			entered := make(chan struct{})
			var models, projectionsSent atomic.Int32
			kernel.readModels.(*readModelKernel).register = func(ctx context.Context, _ *modelcontracts.RegisterManyRequest) error {
				if models.Add(1) == 2 && stage == "read-models" {
					close(entered)
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			kernel.projections.(*projectionKernel).register = func(ctx context.Context, _ *contracts.RegisterRequest) error {
				if projectionsSent.Add(1) == 2 && stage == "projections" {
					close(entered)
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			client, ctx := supervisionClient(t, kernel, WithRegistry(registry), singleRegistrationAttempt())
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			callCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			_, declaration := runtimeModel(t)
			type completion struct {
				result ProjectionRegistration
				err    error
			}
			done := make(chan completion, 1)
			go func() { result, err := store.RegisterProjection(callCtx, declaration); done <- completion{result, err} }()
			awaitSignal(t, ctx, entered)
			cancel()
			finished := <-done
			if !finished.result.Published || !errors.Is(finished.err, context.Canceled) || errors.Is(finished.err, ErrDestructiveRegistrationUnknown) {
				t.Fatal(finished)
			}
			artifacts := finished.result.Outcome.Artifacts
			if len(artifacts) == 0 || artifacts[len(artifacts)-1].Name != stage || artifacts[len(artifacts)-1].Failure == nil {
				t.Fatal("stage failure hidden", artifacts)
			}
			if len(store.ReadModels().Catalog().Descriptors()) != 2 {
				t.Fatal("published addition rolled back")
			}
			if outcome, err := store.WaitForRegistration(ctx); err != nil || !outcome.IsSuccess() {
				t.Fatal(outcome, err)
			}
		})
	}
}
