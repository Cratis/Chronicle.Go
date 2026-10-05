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
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type projectionKernel struct {
	contracts.UnimplementedProjectionsServer
	register func(context.Context, *contracts.RegisterRequest) error
	preview  func(context.Context, *contracts.PreviewProjectionRequest) (*contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors, error)
}

func (k *projectionKernel) Register(ctx context.Context, r *contracts.RegisterRequest) (*emptypb.Empty, error) {
	if err := k.register(ctx, r); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (k *projectionKernel) Preview(ctx context.Context, r *contracts.PreviewProjectionRequest) (*contracts.OneOf_ProjectionPreview_ProjectionDeclarationParsingErrors, error) {
	return k.preview(ctx, r)
}

type ProjectionOpened struct {
	Name string `json:"name"`
}
type ProjectionModel struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(ProjectionOpened)"`
}

func projectionRegistry(t *testing.T) (*Registry, readmodels.Model[ProjectionModel]) {
	t.Helper()
	r := NewRegistry()
	// Forward reference: the model is declared before its contributing event.
	model, err := RegisterReadModel[ProjectionModel](r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RegisterEvent[ProjectionOpened](r); err != nil {
		t.Fatal(err)
	}
	return r, model
}
func TestProjectionDiscoveryRegistrationAndFrozenReplay(t *testing.T) {
	registry, model := projectionRegistry(t)
	labels := []string{"accounts", "accounts", "read"}
	initial := ProjectionModel{Name: "initial"}
	if err := registry.AddProjection(projections.ModelBound(model, projections.WithInitialValues(initial), projections.WithLabels(labels...))); err != nil {
		t.Fatal(err)
	}
	initial.Name = "mutated"
	labels[0] = "mutated"
	var modelCalls, projectionCalls, reads atomic.Int32
	requests := make(chan *contracts.RegisterRequest, 2)
	entered, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{}
	kernel.readModels = &readModelKernel{
		register: func(_ context.Context, r *modelcontracts.RegisterManyRequest) error {
			call := modelCalls.Add(1)
			if kernel.registrations.Load() != call {
				t.Error("model registration preceded events")
			}
			if r.ReadModels[0].ObserverIdentifier != "github.com/cratis/chronicle.go.ProjectionModel" {
				t.Error("producer was not bound to model")
			}
			return nil
		},
		get: func(_ context.Context, r *modelcontracts.GetInstanceByKeyRequest) (*modelcontracts.GetInstanceByKeyResponse, error) {
			reads.Add(1)
			return &modelcontracts.GetInstanceByKeyResponse{ReadModel: `{"id":"` + r.Namespace + `","name":"read"}`}, nil
		},
	}
	kernel.projections = &projectionKernel{register: func(ctx context.Context, r *contracts.RegisterRequest) error {
		call := projectionCalls.Add(1)
		if modelCalls.Load() != call {
			t.Error("projection registration preceded read models")
		}
		if !r.FullSet || r.Owner != contracts.ProjectionOwner_PROJECTION_OWNER_Client || len(r.Projections) != 1 {
			t.Error("invalid projection registration envelope")
		}
		requests <- proto.Clone(r).(*contracts.RegisterRequest)
		if call == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	one, err := client.EventStore(ctx, "store", WithNamespace("one"))
	if err != nil {
		t.Fatal(err)
	}
	two, err := client.EventStore(ctx, "store", WithNamespace("two"))
	if err != nil {
		t.Fatal(err)
	}
	if projectionCalls.Load() != 1 {
		t.Fatal("projection registered per namespace")
	}
	outcome, err := one.WaitForRegistration(ctx)
	if err != nil || len(outcome.Artifacts) != 6 || outcome.Artifacts[5].Name != "projections" {
		t.Fatalf("%+v %v", outcome, err)
	}
	first := <-requests
	if first.Projections[0].InitialModelState != `{"id":"","name":"initial"}` || len(first.Projections[0].Tags) != 2 || first.Projections[0].Tags[0] != "accounts" {
		t.Fatal("initial state/labels did not survive preparation")
	}
	exposed := one.Projections()
	hash, err := exposed[0].Hash()
	if err != nil {
		t.Fatal(err)
	}
	wire := exposed[0].KernelDefinition()
	wire.From[0].Value.Properties["name"] = "changed"
	wire.InitialModelState = "{}"
	wire.Tags[0] = "changed"
	// A registry extension cannot alter the snapshot replayed by the existing client.
	type LaterProjection struct {
		Name string `chronicle:"set(ProjectionOpened)"`
	}
	if _, err = RegisterReadModel[LaterProjection](registry); err != nil {
		t.Fatal(err)
	}
	kernel.endStream <- status.Error(codes.Unavailable, "lost")
	awaitSignal(t, ctx, entered)
	bounded, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	_, err = readmodels.For(one.ReadModels(), model).Get(bounded, "key")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || reads.Load() != 0 {
		t.Fatalf("read bypassed projection replay barrier: %v", err)
	}
	close(release)
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	second := <-requests
	if !proto.Equal(first, second) {
		t.Fatal("reconnect did not reuse the frozen definition")
	}
	if next, err := one.Projections()[0].Hash(); err != nil || next != hash {
		t.Fatal("reconnect changed definition identity")
	}
	for _, store := range []*EventStore{one, two} {
		instance, err := readmodels.For(store.ReadModels(), model).Get(ctx, "key")
		if err != nil || !instance.Exists || instance.Value.ID != string(store.Namespace()) {
			t.Fatalf("bound handle failed: %+v %v", instance, err)
		}
	}
}
func TestProjectionStartupValidationIsAtomicAndStoreLocal(t *testing.T) {
	valid, _ := projectionRegistry(t)
	invalid := NewRegistry()
	if _, err := RegisterReadModel[ProjectionModel](invalid); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(valid), WithRegistryForStore("broken", invalid))
	var declaration *projections.DeclarationError
	if client != nil || !errors.As(err, &declaration) || declaration.GoField != "Name" || declaration.Path != "name" || declaration.EventReference != "ProjectionOpened" {
		t.Fatalf("invalid graph was published: %v %v", client, err)
	}
	event, err := RegisterEvent[ProjectionOpened](invalid)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := readmodels.Define[ProjectionModel]()
	if err != nil {
		t.Fatal(err)
	}
	if err = invalid.AddProjection(projections.ModelBound(foreign, projections.FromEvent(event))); err != nil {
		t.Fatal(err)
	}
	if client, err = NewClient(WithRegistry(invalid)); client != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("foreign model admitted")
	}
}
func TestProjectionFailureCannotPublishReadiness(t *testing.T) {
	registry, _ := projectionRegistry(t)
	kernel := &supervisedKernel{readModels: &readModelKernel{register: func(context.Context, *modelcontracts.RegisterManyRequest) error { return nil }}, projections: &projectionKernel{register: func(context.Context, *contracts.RegisterRequest) error {
		return status.Error(codes.Unimplemented, "old kernel")
	}}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	store, err := client.EventStore(ctx, "store")
	var registration *RegistrationError
	if store != nil || !errors.Is(err, ErrUnsupported) || !errors.As(err, &registration) || registration.Outcome.IsSuccess() || registration.Outcome.Artifacts[5].Failure == nil {
		t.Fatalf("%v %v", store, err)
	}
}
func TestProjectionExplicitDeclarationsAndStoreOverrides(t *testing.T) {
	registry, model := projectionRegistry(t)
	declaration := projections.ModelBound(model, projections.Passive(), projections.WithEventSequence("custom"))
	if err := registry.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(declaration); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	selected := NewRegistry()
	if _, err := RegisterEvent[ProjectionOpened](selected, events.WithID("selected")); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterReadModel[ProjectionModel](selected); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry), WithRegistryForStore("other", selected))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	if !client.projections[0].IsPassive() || client.projections[0].EventSequence() != "custom" || client.storeProjections["other"][0].KernelDefinition().From[0].Key.Id != "selected" {
		t.Fatal("store override leaked")
	}
	bound, ok := client.readModelCatalog.LookupIdentifier(model.Identifier())
	if !ok || bound.Sink().Type != readmodels.NoSink || bound.EventSequence() != "custom" {
		t.Fatal("passive/sequence binding lost")
	}
}
