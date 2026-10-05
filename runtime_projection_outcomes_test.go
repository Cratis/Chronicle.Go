// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimePublishedCumulativeUnknownReturnsRealAcknowledgedOutcomeAndSentinel(t *testing.T) {
	registry := NewRegistry()
	if _, err := RegisterEvent[ProjectionOpened](registry); err != nil {
		t.Fatal(err)
	}
	kernel := runtimeKernel()
	var calls atomic.Int32
	kernel.projections.(*projectionKernel).register = func(_ context.Context, r *contracts.RegisterRequest) error {
		if !r.FullSet {
			t.Error("no acknowledged predecessor must use cumulative set")
		}
		if calls.Add(1) == 1 {
			return status.Error(codes.DeadlineExceeded, "unknown")
		}
		return nil
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), WithRegistrationRetry(RegistrationRetry{MaxAttempts: 2, InitialDelay: time.Millisecond, MaximumDelay: time.Hour, AttemptTimeout: time.Second}))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	_, declaration := runtimeModel(t)
	result, err := store.RegisterProjection(ctx, declaration)
	if !result.Published || !result.Outcome.IsSuccess() || result.Outcome.RetryPending || !errors.Is(err, ErrDestructiveRegistrationUnknown) {
		t.Fatalf("truthful acknowledged unknown outcome: %+v %v", result, err)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeModelFailureRetriesCompleteModelsBeforeCumulativeProjections(t *testing.T) {
	registry, _ := projectionRegistry(t)
	kernel := runtimeKernel()
	var modelCalls atomic.Int32
	models := make(chan int, 5)
	kernel.readModels.(*readModelKernel).register = func(_ context.Context, r *modelcontracts.RegisterManyRequest) error {
		models <- len(r.ReadModels)
		if modelCalls.Add(1) == 2 {
			return status.Error(codes.DeadlineExceeded, "model unknown")
		}
		return nil
	}
	requests := make(chan *contracts.RegisterRequest, 5)
	kernel.projections.(*projectionKernel).register = func(_ context.Context, r *contracts.RegisterRequest) error { requests <- r; return nil }
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), singleRegistrationAttempt())
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	<-models
	<-requests
	_, declaration := runtimeModel(t)
	result, err := store.RegisterProjection(ctx, declaration)
	if !result.Published || err == nil || errors.Is(err, ErrDestructiveRegistrationUnknown) {
		t.Fatal(result, err)
	}
	if count := <-models; count != 1 {
		t.Fatal("first model delta", count)
	}
	outcome, err := store.WaitForRegistration(ctx)
	if err != nil || !outcome.IsSuccess() {
		t.Fatal(outcome, err)
	}
	if count := <-models; count != 2 {
		t.Fatal("retry did not register all models", count)
	}
	if request := <-requests; !request.FullSet || len(request.Projections) != 2 {
		t.Fatal("retry did not register cumulative projections")
	}
}

func TestRuntimeRegistrationAuthorizationCanCloseWithoutSelfJoining(t *testing.T) {
	registry, _ := projectionRegistry(t)
	var client *Client
	type closeKey struct{}
	source := decisionTokenSource(func(ctx context.Context) (Token, error) {
		if ctx.Value(closeKey{}) != nil {
			if err := client.Close(); err != nil {
				return Token{}, err
			}
		}
		return Token{AccessToken: "valid"}, nil
	})
	var ctx context.Context
	client, ctx = supervisionClient(t, runtimeKernel(), WithRegistry(registry), WithTokenSource(source))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	_, declaration := runtimeModel(t)
	result, err := store.RegisterProjection(context.WithValue(ctx, closeKey{}, true), declaration)
	if !result.Published || err == nil {
		t.Fatal(result, err)
	}
	_, models, catalogErr := client.Catalogs("store")
	if catalogErr != nil || len(models.Descriptors()) != 2 {
		t.Fatal("close discarded retained root", catalogErr)
	}
}

func TestRuntimeProjectionRefusesVariantsProtectionAndCollisionsWithoutPublication(t *testing.T) {
	registry, original := projectionRegistry(t)
	event, err := events.Define[ProjectionOpened]()
	if err != nil {
		t.Fatal(err)
	}
	kernel := runtimeKernel()
	var modelCalls, projectionCalls atomic.Int32
	kernel.readModels.(*readModelKernel).register = func(context.Context, *modelcontracts.RegisterManyRequest) error { modelCalls.Add(1); return nil }
	kernel.projections.(*projectionKernel).register = func(context.Context, *contracts.RegisterRequest) error { projectionCalls.Add(1); return nil }
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	model, _ := runtimeModel(t)
	protected, err := readmodels.Define[RuntimeOtherModel](readmodels.WithProtection(compliance.Property("name", compliance.Classification{PII: true})))
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range []projections.Declaration{
		projections.ModelBound(original),
		projections.ModelBound(model, projections.VariantOf[struct{}](), projections.EntersOn(event)),
		projections.ModelBound(model, projections.GlobalFor[struct{}]()),
		projections.ModelBound(protected),
		projections.ModelBound(model, projections.Passive()),
	} {
		result, err := store.RegisterProjection(ctx, declaration)
		if result.Published || err == nil {
			t.Fatal("unsupported shape admitted", result, err)
		}
	}
	if modelCalls.Load() != 1 || projectionCalls.Load() != 1 || store.definitionRoot().revision != 1 {
		t.Fatal("rejected declaration affected local or remote state")
	}
}
