// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type RuntimeProjectionModel struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(ProjectionOpened)"`
}
type RuntimeOtherModel struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(ProjectionOpened)"`
}

func runtimeModel(t *testing.T) (readmodels.Model[RuntimeProjectionModel], projections.Declaration) {
	t.Helper()
	model, err := readmodels.Define[RuntimeProjectionModel]()
	if err != nil {
		t.Fatal(err)
	}
	return model, projections.ModelBound(model)
}

func runtimeKernel() *supervisedKernel {
	return &supervisedKernel{readModels: &readModelKernel{
		register: func(context.Context, *modelcontracts.RegisterManyRequest) error { return nil },
		get: func(context.Context, *modelcontracts.GetInstanceByKeyRequest) (*modelcontracts.GetInstanceByKeyResponse, error) {
			return &modelcontracts.GetInstanceByKeyResponse{ReadModel: `{"id":"key","name":"read"}`}, nil
		},
	}, projections: &projectionKernel{register: func(context.Context, *contracts.RegisterRequest) error { return nil }}}
}

func singleRegistrationAttempt() ClientOption {
	return WithRegistrationRetry(RegistrationRetry{MaxAttempts: 1, InitialDelay: time.Millisecond, MaximumDelay: time.Hour, AttemptTimeout: time.Second})
}

func TestRuntimeProjectionPublishesStoreRootAndRetainsOldReaders(t *testing.T) {
	registry, original := projectionRegistry(t)
	kernel := runtimeKernel()
	models := make(chan *modelcontracts.RegisterManyRequest, 10)
	projectionsSent := make(chan *contracts.RegisterRequest, 10)
	kernel.readModels.(*readModelKernel).register = func(_ context.Context, request *modelcontracts.RegisterManyRequest) error {
		models <- proto.CloneOf(request)
		return nil
	}
	kernel.projections.(*projectionKernel).register = func(_ context.Context, request *contracts.RegisterRequest) error {
		projectionsSent <- proto.CloneOf(request)
		return nil
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.EventStore(ctx, "other")
	if err != nil {
		t.Fatal(err)
	}
	old := store.ReadModels()
	sequence := store.EventLog()
	<-models
	<-models
	<-projectionsSent
	<-projectionsSent
	model, declaration := runtimeModel(t)
	result, err := store.RegisterProjection(ctx, declaration)
	if err != nil || !result.Published || !result.Outcome.IsSuccess() {
		t.Fatalf("%+v %v", result, err)
	}
	modelRequest, projectionRequest := <-models, <-projectionsSent
	if len(modelRequest.ReadModels) != 1 || len(projectionRequest.Projections) != 1 || projectionRequest.FullSet {
		t.Fatal("addition was not model-first additive delta")
	}
	if old == store.ReadModels() || len(old.Catalog().Descriptors()) != 1 || len(other.ReadModels().Catalog().Descriptors()) != 1 || store.EventLog() != sequence {
		t.Fatal("old readers, other store or sequence mutated")
	}
	if _, err := readmodels.For(old, model).Get(ctx, "key"); !errors.Is(err, ErrNotRegistered) {
		t.Fatal(err)
	}
	if _, err := readmodels.For(old, original).Get(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := readmodels.For(store.ReadModels(), model).Get(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	_, latest, err := client.Catalogs("store")
	if err != nil || len(latest.Descriptors()) != 2 {
		t.Fatal("offline catalogs did not select current root", err)
	}
	artifacts, err := client.Artifacts("store")
	if err != nil {
		t.Fatal(err)
	}
	artifacts.Projections[0] = projections.Definition{}
	wire := store.Projections()[1].KernelDefinition()
	wire.From[0].Value.Properties["name"] = "mutated"
	if store.Projections()[0].Identifier() == "" || store.Projections()[1].KernelDefinition().From[0].Value.Properties["name"] == "mutated" {
		t.Fatal("publication view shared mutable slices or maps")
	}
	namespace, err := client.EventStore(ctx, "store", WithNamespace("new"))
	if err != nil || len(namespace.ReadModels().Catalog().Descriptors()) != 2 {
		t.Fatal("new namespace used initial registry", err)
	}
	result, err = store.RegisterProjection(ctx, declaration)
	if result.Published || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("replacement admitted", result, err)
	}
}

func TestRuntimeUnknownInitialFullSetPreservesReadyButRefusesAdd(t *testing.T) {
	registry, model := projectionRegistry(t)
	kernel := runtimeKernel()
	var calls atomic.Int32
	kernel.projections.(*projectionKernel).register = func(context.Context, *contracts.RegisterRequest) error {
		if calls.Add(1) == 1 {
			return status.Error(codes.DeadlineExceeded, "unknown")
		}
		return nil
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), singleRegistrationAttempt())
	if store, err := client.EventStore(ctx, "store"); store != nil || err == nil {
		t.Fatal("initial unknown succeeded")
	}
	store, err := client.EventStore(ctx, "store", WithNamespace("another"))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal("sticky mutation refusal changed Ready", err)
	}
	outcome, err := store.WaitForRegistration(ctx)
	if err != nil || !outcome.IsSuccess() || outcome.RetryPending {
		t.Fatalf("acknowledgement overwritten: %+v %v", outcome, err)
	}
	if _, err := readmodels.For(store.ReadModels(), model).Get(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EventLog().Append(ctx, "source", ProjectionOpened{Name: "name"}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
		t.Fatal(err)
	}
	_, declaration := runtimeModel(t)
	result, err := store.RegisterProjection(ctx, declaration)
	if result.Published || !errors.Is(err, ErrDestructiveRegistrationUnknown) {
		t.Fatalf("unknown forgotten: %+v %v", result, err)
	}
	separate, err := client.EventStore(ctx, "separate")
	if err != nil {
		t.Fatal(err)
	}
	if result, err := separate.RegisterProjection(ctx, declaration); err != nil || !result.Published {
		t.Fatal("another store poisoned", result, err)
	}
}

func TestRuntimeAdditiveUnknownThenNewAdditionRegistersCompleteRoot(t *testing.T) {
	registry, _ := projectionRegistry(t)
	kernel := runtimeKernel()
	requests := make(chan *contracts.RegisterRequest, 10)
	kernel.projections.(*projectionKernel).register = func(_ context.Context, request *contracts.RegisterRequest) error {
		requests <- proto.CloneOf(request)
		if !request.FullSet {
			return status.Error(codes.DeadlineExceeded, "additive unknown")
		}
		return nil
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), singleRegistrationAttempt())
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	<-requests
	_, declaration := runtimeModel(t)
	result, err := store.RegisterProjection(ctx, declaration)
	if !result.Published || err == nil || errors.Is(err, ErrDestructiveRegistrationUnknown) {
		t.Fatal(result, err)
	}
	if request := <-requests; request.FullSet || len(request.Projections) != 1 {
		t.Fatal("first addition not additive")
	}
	second, err := readmodels.Define[RuntimeOtherModel]()
	if err != nil {
		t.Fatal(err)
	}
	result, err = store.RegisterProjection(ctx, projections.ModelBound(second))
	if err != nil || !result.Published || !result.Outcome.IsSuccess() {
		t.Fatal(result, err)
	}
	if request := <-requests; !request.FullSet || len(request.Projections) != 3 {
		t.Fatal("cumulative retry omitted retained addition")
	}
}

func TestRuntimePublicationStalesAllNamespaceGuardsAndOldReaders(t *testing.T) {
	registry, model := projectionRegistry(t)
	client, ctx := supervisionClient(t, runtimeKernel(), WithRegistry(registry))
	one, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	two, err := client.EventStore(ctx, "store", WithNamespace("two"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.EventStore(ctx, "other")
	if err != nil {
		t.Fatal(err)
	}
	var tokens []decision.Token
	var guards []*decision.Guard
	for _, store := range []*EventStore{one, two, other} {
		catalog := store.definitionRoot().decisions
		token := decision.Issue(decision.Evidence{Target: decision.Target{Client: client, Store: string(store.name), Namespace: string(store.namespace), Sequence: string(events.EventLog)}, Model: string(model.Identifier()), Key: "key", Types: []events.TypeRef{{ID: "ProjectionOpened", Generation: 1}}, Catalog: catalog, Epoch: catalog.ExpectedEpoch, Generation: 1, Check: func() error { return nil }})
		tokens = append(tokens, token)
		if err := decision.Enroll(token, (&clientTransport{client: client, store: store}).DecisionTarget(events.EventLog), store, func(g *decision.Guard) error { guards = append(guards, g); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	old := readmodels.DecisionsFor(two.ReadModels(), model)
	if !old.Admit().IsAdmitted {
		t.Fatal("fixture not admitted")
	}
	_, declaration := runtimeModel(t)
	if _, err := one.RegisterProjection(ctx, declaration); err != nil {
		t.Fatal(err)
	}
	for i, store := range []*EventStore{one, two, other} {
		target := (&clientTransport{client: client, store: store}).DecisionTarget(events.EventLog)
		want := error(nil)
		if i < 2 {
			want = decision.ErrStale
		}
		if err := guards[i].Validate(target, 1); !errors.Is(err, want) {
			t.Fatal("enrolled guard", i, err)
		}
		if err := decision.Enroll(tokens[i], target, store, func(*decision.Guard) error { return nil }); !errors.Is(err, want) {
			t.Fatal("issued guard", i, err)
		}
	}
	if _, err := old.GetDetached(ctx, "key"); !errors.Is(err, decision.ErrStale) {
		t.Fatal("old reader minted fresh evidence", err)
	}
}
