// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/constraints"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	reactorcontracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	reducercontracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type seedKernel struct {
	contracts.UnimplementedEventSeedingServer
	calls atomic.Int32
	send  func(context.Context, *contracts.SeedEventsRequest) (*contracts.CommandResult, error)
}

func (k *seedKernel) SeedEvents(ctx context.Context, request *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
	k.calls.Add(1)
	if k.send != nil {
		return k.send(ctx, request)
	}
	return &contracts.CommandResult{IsAuthorized: true}, nil
}
func seedRegistry(t *testing.T, seed func(*seeding.Builder) error) *Registry {
	t.Helper()
	r := NewRegistry()
	if _, err := RegisterEvent[lifecycleEvent](r, events.WithTags("seed")); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSeederFunc(r, seed); err != nil {
		t.Fatal(err)
	}
	return r
}
func seedPolicy() ClientOption {
	return WithRegistrationRetry(RegistrationRetry{MaxAttempts: 1, InitialDelay: time.Millisecond, MaximumDelay: 10 * time.Millisecond, AttemptTimeout: time.Second})
}

func TestSeedPreparationOnceAndReconnectReplaysFrozenRequest(t *testing.T) {
	var prepared atomic.Int32
	value := &lifecycleEvent{Name: "original"}
	registry := seedRegistry(t, func(b *seeding.Builder) error {
		prepared.Add(1)
		seeding.For(b, "one", value)
		seeding.For(b.ForNamespace("red"), "two", lifecycleEvent{Name: "local"})
		return nil
	})
	requests := make(chan *contracts.SeedEventsRequest, 8)
	seeds := &seedKernel{send: func(_ context.Context, request *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
		requests <- request
		return &contracts.CommandResult{IsAuthorized: true}, nil
	}}
	kernel := &supervisedKernel{seeding: seeds}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), seedPolicy())
	value.Name = "changed after preparation"
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { t.Error("late seeder ran"); return nil }); err != nil {
		t.Fatal(err)
	}
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveSeed(t, ctx, requests)
	if first.GlobalByEventType[0].Entries[0].Content != `{"Name":"original"}` {
		t.Fatal(first)
	}
	kernel.endStream <- status.Error(codes.Unavailable, "replace generation")
	second := receiveSeed(t, ctx, requests) // Automatic replay, without a readiness caller.
	if !proto.Equal(first, second) {
		t.Fatal("reconnect changed seed snapshot", second)
	}
	after, err := store.WaitForRegistration(ctx)
	if err != nil || after.Generation <= before.Generation {
		t.Fatal(after, err)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EventStore(ctx, "store", WithNamespace("red")); err != nil {
		t.Fatal(err)
	}
	if prepared.Load() != 1 || seeds.calls.Load() != 2 || kernel.appends.Load() != 0 {
		t.Fatal("callback rerun, duplicate send or append", prepared.Load(), seeds.calls.Load(), kernel.appends.Load())
	}
}

func receiveSeed(t *testing.T, ctx context.Context, requests <-chan *contracts.SeedEventsRequest) *contracts.SeedEventsRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

type seedModels struct {
	modelcontracts.UnimplementedReadModelsServer
	called *atomic.Bool
}

func (s *seedModels) RegisterMany(context.Context, *modelcontracts.RegisterManyRequest) (*emptypb.Empty, error) {
	s.called.Store(true)
	return &emptypb.Empty{}, nil
}

type seedProjections struct {
	projectioncontracts.UnimplementedProjectionsServer
	called *atomic.Bool
}

func (s *seedProjections) Register(context.Context, *projectioncontracts.RegisterRequest) (*emptypb.Empty, error) {
	s.called.Store(true)
	return &emptypb.Empty{}, nil
}

type seedReactors struct {
	reactorcontracts.UnimplementedReactorsServer
}

func (*seedReactors) Observe(stream grpc.BidiStreamingServer[reactorcontracts.ReactorMessage, reactorcontracts.EventsToObserve]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

type seedReducers struct {
	reducercontracts.UnimplementedReducersServer
}

func (*seedReducers) Observe(stream grpc.BidiStreamingServer[reducercontracts.ReducerMessage, reducercontracts.ReduceOperationMessage]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

type seedSendStream struct {
	grpc.ClientStream
	sent    *atomic.Int32
	entered chan<- struct{}
	release <-chan struct{}
}

func (s seedSendStream) SendMsg(value any) error {
	err := s.ClientStream.SendMsg(value)
	if err != nil {
		return err
	}
	s.entered <- struct{}{}
	select {
	case <-s.release:
		s.sent.Add(1)
		return nil
	case <-s.Context().Done():
		return s.Context().Err()
	}
}

type seededProjection struct{ Name string }
type seededFold struct{ Name string }

func TestSeedingIsLastAfterEveryObserverRegistrationSend(t *testing.T) {
	registry := seedRegistry(t, func(b *seeding.Builder) error { seeding.For(b, "one", lifecycleEvent{Name: "seed"}); return nil })
	if err := RegisterReactorHandler(registry, "seed-reactor", func(context.Context, lifecycleEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[seededFold](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err = RegisterReducerHandlers(registry, model, "seed-reducer", []reducers.Handler{reducers.On(func(_ context.Context, event lifecycleEvent, _ *seededFold, _ events.Context) (*seededFold, error) {
		return &seededFold{Name: event.Name}, nil
	})}); err != nil {
		t.Fatal(err)
	}
	projectionModel, err := RegisterReadModel[seededProjection](registry)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[lifecycleEvent]()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(projections.ModelBound(projectionModel, projections.FromEvent(event))); err != nil {
		t.Fatal(err)
	}
	constraint, err := constraints.UniqueEventTypes(event.Descriptor()).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddConstraint(constraint); err != nil {
		t.Fatal(err)
	}
	var models, projected, constraints atomic.Bool
	var sent atomic.Int32
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	seeds := &seedKernel{send: func(context.Context, *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
		if !models.Load() || !projected.Load() || !constraints.Load() || sent.Load() != 2 {
			t.Error("seed before observers", sent.Load())
		}
		return &contracts.CommandResult{IsAuthorized: true}, nil
	}}
	kernel := &supervisedKernel{seeding: seeds, readModels: &seedModels{called: &models}, projections: &seedProjections{called: &projected}, reactors: &seedReactors{}, reducers: &seedReducers{}, registerConstraints: func(context.Context, *constraintcontracts.RegisterConstraintsRequest) error {
		constraints.Store(true)
		return nil
	}}
	kernel.streamInterceptor = func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err == nil && (method == reactorcontracts.Reactors_Observe_FullMethodName || method == reducercontracts.Reducers_Observe_FullMethodName) {
			return seedSendStream{stream, &sent, entered, release}, nil
		}
		return stream, err
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	result := make(chan error, 1)
	go func() { _, err := client.EventStore(ctx, "store"); result <- err }()
	awaitSignal(t, ctx, entered)
	if seeds.calls.Load() != 0 {
		t.Fatal("seed dispatched while registration send was blocked")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if seeds.calls.Load() != 1 {
		t.Fatal(seeds.calls.Load())
	}
}

func TestSeedFailureRetainsSnapshotAndManualBatchConsumesOnlySuccess(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	seeds := &seedKernel{send: func(_ context.Context, _ *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
		if fail.Load() {
			return &contracts.CommandResult{IsAuthorized: true, ExceptionMessages: []string{"seed rejected"}}, nil
		}
		return &contracts.CommandResult{IsAuthorized: true}, nil
	}}
	registry := seedRegistry(t, func(b *seeding.Builder) error { seeding.For(b, "one", lifecycleEvent{}); return nil })
	client, ctx := supervisionClient(t, &supervisedKernel{seeding: seeds}, WithRegistry(registry), seedPolicy())
	_, err := client.EventStore(ctx, "store")
	var registrationError *RegistrationError
	var envelope *EnvelopeError
	if !errors.As(err, &registrationError) || !errors.As(err, &envelope) {
		t.Fatal(err)
	}
	last := registrationError.Outcome.Artifacts[len(registrationError.Outcome.Artifacts)-1]
	if last.Name != "seeding" || last.Failure == nil || registrationError.Outcome.IsSuccess() {
		t.Fatal(registrationError.Outcome)
	}
	fail.Store(false)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	var prepared int
	batch, err := store.PrepareSeeds(seeding.Func(func(b *seeding.Builder) error { prepared++; seeding.For(b, "manual", lifecycleEvent{}); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := batch.Register(ctx); !errors.As(err, &envelope) {
		t.Fatal(err)
	}
	fail.Store(false)
	if err := batch.Register(ctx); err != nil {
		t.Fatal(err)
	}
	calls := seeds.calls.Load()
	if err := batch.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if prepared != 1 || seeds.calls.Load() != calls {
		t.Fatal("manual batch reran consumed work")
	}
}

func TestSeedSendIsSingleFlightAndWaiterCancellationDoesNotConsumeWork(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	seeds := &seedKernel{send: func(ctx context.Context, _ *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return &contracts.CommandResult{IsAuthorized: true}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	registry := seedRegistry(t, func(b *seeding.Builder) error { seeding.For(b, "one", lifecycleEvent{}); return nil })
	client, ctx := supervisionClient(t, &supervisedKernel{seeding: seeds}, WithRegistry(registry))
	result := make(chan error, 1)
	go func() { _, err := client.EventStore(ctx, "store"); result <- err }()
	awaitSignal(t, ctx, entered)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := client.EventStore(canceled, "store"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if seeds.calls.Load() != 1 {
		t.Fatal(seeds.calls.Load())
	}
}
