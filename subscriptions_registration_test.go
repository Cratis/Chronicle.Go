// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/eventstoresubscriptions"
	reactorcontracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	reducercontracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	seedcontracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type automaticSubscriptionKernel struct {
	contracts.UnimplementedEventStoreSubscriptionsServer
	add   func(context.Context, *contracts.AddEventStoreSubscriptions) error
	calls atomic.Int32
}

func (k *automaticSubscriptionKernel) Add(ctx context.Context, r *contracts.AddEventStoreSubscriptions) (*emptypb.Empty, error) {
	k.calls.Add(1)
	if k.add != nil {
		if err := k.add(ctx, r); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

type OriginFirst struct{ Value string }
type OriginSecond struct{ Value string }
type OriginThird struct{ Value string }
type OriginFold struct{ Value string }
type OriginProjection struct{ Value string }

func externalRegistry(t *testing.T) *Registry {
	t.Helper()
	r := NewRegistry()
	first, err := RegisterEvent[OriginFirst](r, events.WithID("first"), events.WithSourceStore("origin"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RegisterEvent[OriginSecond](r, events.WithID("second"), events.WithSourceStore("origin")); err != nil {
		t.Fatal(err)
	}
	third, err := RegisterEvent[OriginThird](r, events.WithID("third"), events.WithSourceStore("origin"))
	if err != nil {
		t.Fatal(err)
	}
	if err = RegisterReactorHandler(r, "reactor", func(context.Context, OriginFirst) error { return nil }); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[OriginFold](r)
	if err != nil {
		t.Fatal(err)
	}
	if err = RegisterReducerHandlers(r, model, "reducer", []reducers.Handler{reducers.On(func(_ context.Context, e OriginSecond, _ *OriginFold, _ events.Context) (*OriginFold, error) {
		return &OriginFold{e.Value}, nil
	})}); err != nil {
		t.Fatal(err)
	}
	projectionModel, err := RegisterReadModel[OriginProjection](r)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.AddProjection(projections.ModelBound(projectionModel, projections.FromEvent(first), projections.FromEvent(third))); err != nil {
		t.Fatal(err)
	}
	return r
}
func TestObserverOriginsBindSequencesModelsAndSubscriptionUnion(t *testing.T) {
	client, err := NewClient(WithRegistry(externalRegistry(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, store := range []StoreName{"origin", "consumer"} {
		artifacts, err := client.Artifacts(store)
		if err != nil {
			t.Fatal(err)
		}
		want := events.EventLog
		if store != "origin" {
			want = "inbox-origin"
		}
		if artifacts.Reactors[0].EventSequence() != want || artifacts.Reducers[0].EventSequence() != want || artifacts.Reducers[0].Model().EventSequence() != want || artifacts.Projections[0].EventSequence() != want {
			t.Fatal("store binding lost")
		}
		for _, model := range artifacts.ReadModels.Descriptors() {
			if model.EventSequence() != want {
				t.Fatal("read model catalog not rebound")
			}
		}
		snapshot, err := client.selectedStoreSnapshot(store)
		if err != nil {
			t.Fatal(err)
		}
		handle := &EventStore{client: client, name: store, catalog: snapshot.events, reactorSnapshot: snapshot.reactors, reducerSnapshot: snapshot.reducers, projectionSnapshot: snapshot.projections}
		subscriptions := handle.externalSubscriptions()
		if store == "origin" {
			if len(subscriptions) != 0 {
				t.Fatal("self subscription inferred")
			}
			continue
		}
		if len(subscriptions) != 1 || subscriptions[0].Identifier() != "origin" || !reflect.DeepEqual(subscriptions[0].EventTypes(), []events.TypeID{"first", "second", "third"}) {
			t.Fatalf("union = %v", subscriptions)
		}
	}
}
func TestObserverExplicitSequencesOverrideInferenceAndMixedSourcesFail(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		r := NewRegistry()
		if _, err := RegisterEvent[OriginFirst](r, events.WithSourceStore("a")); err != nil {
			t.Fatal(err)
		}
		if _, err := RegisterEvent[OriginSecond](r, events.WithSourceStore("b")); err != nil {
			t.Fatal(err)
		}
		var options []reactors.Option
		if explicit {
			options = append(options, reactors.WithEventLog())
		}
		if err := RegisterReactorHandlers(r, "mixed", []reactors.Handler{reactors.On(func(context.Context, OriginFirst) error { return nil }), reactors.On(func(context.Context, OriginSecond) error { return nil })}, options...); err != nil {
			t.Fatal(err)
		}
		client, err := NewClient(WithRegistry(r))
		if !explicit {
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatal("mixed origins accepted", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		artifacts, err := client.Artifacts("target")
		if err != nil {
			t.Fatal(err)
		}
		if artifacts.Reactors[0].SourceStore() != "" || artifacts.Reactors[0].EventSequence() != events.EventLog {
			t.Fatal("explicit event-log not respected")
		}
		if err = client.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := reactors.DefineHandler("bad", func(context.Context, OriginFirst) error { return nil }, reactors.WithSourceStore("a"), reactors.WithEventLog()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("source/sequence conflict accepted")
	}
	model, err := readmodels.Define[OriginFold]()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reducers.DefineHandlers(model, "bad", []reducers.Handler{reducers.On(func(context.Context, OriginFirst, *OriginFold, events.Context) (*OriginFold, error) { return nil, nil })}, reducers.WithSourceStore("a"), reducers.WithEventLog()); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("reducer conflict accepted")
	}
}

func TestObserverSourceOverrideAndReducerMixedOriginEscape(t *testing.T) {
	for _, mode := range []string{"mixed", "explicit", "override"} {
		r := NewRegistry()
		if _, err := RegisterEvent[OriginFirst](r, events.WithSourceStore("a")); err != nil {
			t.Fatal(err)
		}
		if _, err := RegisterEvent[OriginSecond](r, events.WithSourceStore("b")); err != nil {
			t.Fatal(err)
		}
		model, err := RegisterReadModel[OriginFold](r)
		if err != nil {
			t.Fatal(err)
		}
		handlers := []reducers.Handler{
			reducers.On(func(context.Context, OriginFirst, *OriginFold, events.Context) (*OriginFold, error) { return nil, nil }),
			reducers.On(func(context.Context, OriginSecond, *OriginFold, events.Context) (*OriginFold, error) { return nil, nil }),
		}
		var options []reducers.Option
		if mode == "explicit" {
			options = append(options, reducers.WithEventLog())
		}
		if mode == "override" {
			options = append(options, reducers.WithSourceStore("selected"))
		}
		if err = RegisterReducerHandlers(r, model, "fold", handlers, options...); err != nil {
			t.Fatal(err)
		}
		if mode == "override" {
			if err = RegisterReactorHandler(r, "react", func(context.Context, OriginFirst) error { return nil }, reactors.WithSourceStore("selected")); err != nil {
				t.Fatal(err)
			}
		}
		client, err := NewClient(WithRegistry(r))
		if mode == "mixed" {
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatal("mixed reducer origins accepted", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, store := range []StoreName{"selected", "other"} {
			artifacts, err := client.Artifacts(store)
			if err != nil {
				t.Fatal(err)
			}
			want := events.EventLog
			if mode == "override" {
				want = "inbox-selected"
				if artifacts.Reactors[0].EventSequence() != want || artifacts.Reactors[0].SourceStore() != "selected" {
					t.Fatal("reactor source override lost")
				}
			}
			if artifacts.Reducers[0].EventSequence() != want || artifacts.Reducers[0].Model().EventSequence() != want {
				t.Fatal("reducer source override/escape lost")
			}
		}
		if err = client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

type subscriptionSendStream struct {
	grpc.ClientStream
	sent *atomic.Int32
}

func (s subscriptionSendStream) SendMsg(value any) error {
	err := s.ClientStream.SendMsg(value)
	if err == nil {
		s.sent.Add(1)
	}
	return err
}
func TestExternalSubscriptionsFollowObserversPrecedeSeedingAndReplayPerGeneration(t *testing.T) {
	r := externalRegistry(t)
	if err := RegisterSeederFunc(r, func(b *seeding.Builder) error { seeding.For(b, "seed", OriginFirst{Value: "seed"}); return nil }); err != nil {
		t.Fatal(err)
	}
	var models, projected atomic.Bool
	var sent atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	registered := make(chan struct{}, 8)
	subscriptions := &automaticSubscriptionKernel{add: func(_ context.Context, r *contracts.AddEventStoreSubscriptions) error {
		if !models.Load() || !projected.Load() || sent.Load() < 2 {
			t.Error("subscription before observers")
		}
		if r.TargetEventStore != "consumer" || len(r.Subscriptions) != 1 || r.Subscriptions[0].Identifier != "origin" || len(r.Subscriptions[0].EventTypes) != 3 {
			t.Error("incorrect automatic subscription wire")
		}
		if fail.Load() {
			return status.Error(codes.PermissionDenied, "denied")
		}
		registered <- struct{}{}
		return nil
	}}
	seeds := &seedKernel{send: func(context.Context, *seedcontracts.SeedEventsRequest) (*seedcontracts.CommandResult, error) {
		if fail.Load() || subscriptions.calls.Load() == 0 {
			t.Error("seeding before subscription")
		}
		return &seedcontracts.CommandResult{IsAuthorized: true}, nil
	}}
	kernel := &supervisedKernel{subscriptions: subscriptions, seeding: seeds, readModels: &seedModels{called: &models}, projections: &seedProjections{called: &projected}, reactors: &seedReactors{}, reducers: &seedReducers{}}
	kernel.streamInterceptor = func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err == nil && (method == reactorcontracts.Reactors_Observe_FullMethodName || method == reducercontracts.Reducers_Observe_FullMethodName) {
			return subscriptionSendStream{stream, &sent}, nil
		}
		return stream, err
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(r), seedPolicy())
	_, err := client.EventStore(ctx, "consumer")
	var registration *RegistrationError
	if !errors.As(err, &registration) || status.Code(err) != codes.PermissionDenied || seeds.calls.Load() != 0 {
		t.Fatalf("failed subscription published readiness: %v", err)
	}
	last := registration.Outcome.Artifacts[len(registration.Outcome.Artifacts)-1]
	if last.Name != "external-subscriptions" || last.Failure == nil {
		t.Fatal("missing stage result")
	}
	fail.Store(false)
	store, err := client.EventStore(ctx, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, registered)
	before := subscriptions.calls.Load()
	if _, err = client.EventStore(ctx, "consumer", WithNamespace("other")); err != nil {
		t.Fatal(err)
	}
	if subscriptions.calls.Load() != before {
		t.Fatal("store-wide subscription duplicated per namespace")
	}
	kernel.endStream <- status.Error(codes.Unavailable, "test disconnect")
	awaitSignal(t, ctx, registered)
	if _, err = store.WaitForRegistration(ctx); err != nil {
		t.Fatal(err)
	}
	if subscriptions.calls.Load() != before+1 {
		t.Fatal("subscription not replayed once per generation")
	}
}
