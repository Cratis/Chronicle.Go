// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reducers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type FoldChanged struct {
	Amount int `json:"amount"`
}
type FoldDeleted struct{}
type FoldTotal struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}
type foldScopes struct {
	opened, closed atomic.Int32
	failClose      bool
}

func (f *foldScopes) NewScope(ctx context.Context) (reducers.Scope, error) {
	if !reflect.DeepEqual(metadata.Identity(ctx), identities.System()) {
		return nil, errors.New("non-system constructor identity")
	}
	if batch, ok := reducers.BatchFromContext(ctx); !ok || batch.Store != "store" || batch.Namespace != DefaultNamespace {
		return nil, errors.New("missing batch coordinates")
	}
	f.opened.Add(1)
	return &foldScope{f}, nil
}

type foldScope struct{ owner *foldScopes }

func (*foldScope) Resolve(context.Context, reflect.Type) (any, error) {
	return nil, errors.New("unexpected resolution")
}
func (s *foldScope) Close(context.Context) error {
	s.owner.closed.Add(1)
	if s.owner.failClose {
		return errors.New("cleanup failed")
	}
	return nil
}

type reducerKernel struct {
	contracts.UnimplementedReducersServer
	registrations chan *contracts.RegisterReducer
	operations    chan *contracts.ReduceOperationMessage
	results       chan *contracts.ReducerResult
	end           chan error
	handlers      atomic.Int32
}

func (s *reducerKernel) Observe(stream grpc.BidiStreamingServer[contracts.ReducerMessage, contracts.ReduceOperationMessage]) error {
	s.handlers.Add(1)
	defer s.handlers.Add(-1)
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	select {
	case s.registrations <- request.GetContent().GetValue0():
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for {
		select {
		case err := <-s.end:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		case operation := <-s.operations:
			if err := stream.Send(operation); err != nil {
				return err
			}
			if operation.ReplayState != contracts.ReplayState_REPLAY_STATE_None {
				continue
			}
			request, err := stream.Recv()
			if err != nil {
				return err
			}
			select {
			case s.results <- request.GetContent().GetValue1():
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
	}
}
func reducerFixture(t *testing.T, registry *Registry, options ...ClientOption) (*Client, *EventStore, context.Context, *reducerKernel, *supervisedKernel) {
	t.Helper()
	server := &reducerKernel{registrations: make(chan *contracts.RegisterReducer, 8), operations: make(chan *contracts.ReduceOperationMessage, 8), results: make(chan *contracts.ReducerResult, 8), end: make(chan error, 1)}
	kernel := &supervisedKernel{reducers: server, readModels: &readModelKernel{register: func(_ context.Context, r *modelcontracts.RegisterManyRequest) error {
		for _, m := range r.ReadModels {
			if m.ObserverType != modelcontracts.ReadModelObserverType_Reducer || m.ObserverIdentifier != "fold" {
				return errors.New("wrong producer binding")
			}
		}
		return nil
	}}}
	fastRetry := func(c *clientConfig) {
		c.reactorRetryWait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	}
	client, ctx := supervisionClient(t, kernel, append([]ClientOption{WithRegistry(registry), fastRetry}, options...)...)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	return client, store, ctx, server, kernel
}
func foldRegistry(t *testing.T, on func(context.Context, FoldChanged, *FoldTotal, events.Context) (*FoldTotal, error), options ...reducers.Option) *Registry {
	t.Helper()
	registry := NewRegistry()
	if _, err := RegisterEvent[FoldChanged](registry, events.WithGeneration(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterEvent[FoldDeleted](registry); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[FoldTotal](registry)
	if err != nil {
		t.Fatal(err)
	}
	err = RegisterReducerHandlers(registry, model, "fold", []reducers.Handler{reducers.On(on), reducers.On(func(context.Context, FoldDeleted, *FoldTotal, events.Context) (*FoldTotal, error) { return nil, nil })}, options...)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
func foldEvent(id string, n uint64, content string) *contracts.AppendedEvent {
	return &contracts.AppendedEvent{Context: &contracts.EventContext{EventType: &contracts.EventType{Id: id, Generation: 1}, EventStore: "store", Namespace: string(DefaultNamespace), EventSourceId: "source", SequenceNumber: n, Occurred: &contracts.SerializableDateTimeOffset{Value: "2026-01-01T00:00:00Z"}, CausedBy: &contracts.Identity{Subject: "human"}}, Content: content}
}
func sumFold(_ context.Context, e FoldChanged, current *FoldTotal, _ events.Context) (*FoldTotal, error) {
	amount := e.Amount
	if current != nil {
		amount += current.Amount
	}
	return &FoldTotal{"source", amount}, nil
}
func TestReducerRuntimeFoldOrderDeletionGenerationAndCleanupBeforeAck(t *testing.T) {
	var order []int
	scopes := &foldScopes{}
	registry := foldRegistry(t, func(ctx context.Context, e FoldChanged, current *FoldTotal, ec events.Context) (*FoldTotal, error) {
		if !reflect.DeepEqual(metadata.Identity(ctx), identities.System()) || ec.CausedBy.Subject != "human" {
			return nil, errors.New("identity lost")
		}
		if ec.EventType.Generation != 2 {
			return nil, errors.New("generation not selected")
		}
		order = append(order, e.Amount)
		return sumFold(ctx, e, current, ec)
	}, reducers.WithVersion("logic-v3"), reducers.WithEventTagFilter("blue", "green"), reducers.WithEventSourceType("order"), reducers.WithEventStreamType("payments"), reducers.WithTags("finance"))
	client, store, ctx, server, _ := reducerFixture(t, registry, WithServices(scopes))
	registration := receiveOpening(t, ctx, server.registrations)
	if registration.Reducer.Hash == "" || registration.Reducer.ReadModel == "" || !registration.Reducer.IsActive || !slices.Equal(registration.Reducer.Filters.FilterTags, []string{"blue", "green"}) || registration.Reducer.Filters.EventSourceType != "order" || registration.Reducer.Filters.EventStreamType != "payments" || !slices.Equal(registration.Reducer.Tags, []string{"finance"}) {
		t.Fatal(registration)
	}
	change := foldEvent("FoldChanged", 0, `{"amount":99}`)
	change.GenerationalContent = map[int32]string{2: `{"amount":3}`}
	next := foldEvent("FoldChanged", 2, `{"amount":99}`)
	next.GenerationalContent = map[int32]string{2: `{"amount":5}`}
	server.operations <- &contracts.ReduceOperationMessage{Partition: "source", InitialState: `{"id":"source","amount":10}`, Events: []*contracts.AppendedEvent{change, foldEvent("FoldDeleted", 1, `{}`), next}}
	result := receiveOpening(t, ctx, server.results)
	var total FoldTotal
	if err := json.Unmarshal([]byte(result.ReadModelState), &total); err != nil {
		t.Fatal(err)
	}
	if result.State != contracts.ObservationState_Success || total.Amount != 5 || result.LastSuccessfulObservation != 2 || !slices.Equal(order, []int{3, 5}) || scopes.opened.Load() != 1 || scopes.closed.Load() != 1 {
		t.Fatalf("result %v total %+v order %v scope %d/%d", result, total, order, scopes.opened.Load(), scopes.closed.Load())
	}
	server.operations <- &contracts.ReduceOperationMessage{Partition: "source", InitialState: result.ReadModelState, Events: []*contracts.AppendedEvent{foldEvent("FoldDeleted", 3, `{}`)}}
	deleted := receiveOpening(t, ctx, server.results)
	if deleted.ReadModelState != "" || deleted.State != contracts.ObservationState_Success || scopes.opened.Load() != 2 || scopes.closed.Load() != 2 {
		t.Fatal(deleted)
	}
	if err := store.UnregisterReducer(ctx, "fold"); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestReducerRuntimeFailureNeverReportsPartialState(t *testing.T) {
	for _, mode := range []string{"fold", "panic", "cleanup", "decode", "initial", "order"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			scopes := &foldScopes{failClose: mode == "cleanup"}
			registry := foldRegistry(t, func(ctx context.Context, e FoldChanged, current *FoldTotal, ec events.Context) (*FoldTotal, error) {
				calls++
				if calls == 2 && mode == "fold" {
					return &FoldTotal{Amount: 999}, errors.New("fail second")
				}
				if calls == 2 && mode == "panic" {
					panic("fail second")
				}
				return sumFold(ctx, e, current, ec)
			})
			_, _, ctx, server, _ := reducerFixture(t, registry, WithServices(scopes))
			receiveOpening(t, ctx, server.registrations)
			batch := []*contracts.AppendedEvent{foldEvent("FoldChanged", 0, `{"amount":1}`), foldEvent("FoldChanged", 1, `{"amount":2}`), foldEvent("FoldChanged", 2, `{"amount":3}`)}
			initial := ""
			if mode == "decode" {
				batch[1].Content = "null"
			}
			if mode == "initial" {
				initial = "[]"
			}
			if mode == "order" {
				batch[1].Context.SequenceNumber = 0
			}
			server.operations <- &contracts.ReduceOperationMessage{Partition: "source", InitialState: initial, Events: batch}
			result := receiveOpening(t, ctx, server.results)
			if result.State != contracts.ObservationState_Failed || result.ReadModelState != "" || len(result.ExceptionMessages) == 0 {
				t.Fatal(result)
			}
			if (mode == "fold" || mode == "panic") && (calls != 2 || result.LastSuccessfulObservation != 0) {
				t.Fatal(calls, result)
			}
			if mode == "cleanup" && result.LastSuccessfulObservation != uint64(events.Unavailable) {
				t.Fatal(result)
			}
		})
	}
}
func TestReducerResubscribesAfterEOFAndGenerationReconnectWithoutRediscovery(t *testing.T) {
	registry := foldRegistry(t, sumFold)
	client, store, ctx, server, kernel := reducerFixture(t, registry)
	first := receiveOpening(t, ctx, server.registrations)
	server.end <- nil // Unlike C# normal completion, Go deliberately resubscribes on EOF too.
	second := receiveOpening(t, ctx, server.registrations)
	if !proto.Equal(first, second) {
		t.Fatal("same-generation registration changed")
	}
	kernel.endStream <- status.Error(codes.Unavailable, "restart")
	third := receiveOpening(t, ctx, server.registrations)
	if third.ConnectionId == first.ConnectionId || !proto.Equal(first.Reducer, third.Reducer) {
		t.Fatal("reconnect lost stable definition")
	}
	if err := store.UnregisterReducer(ctx, "fold"); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestReducerCancellationJoinsScopeAndNeverAcknowledges(t *testing.T) {
	started := make(chan struct{})
	scopes := &foldScopes{}
	registry := foldRegistry(t, func(ctx context.Context, _ FoldChanged, _ *FoldTotal, _ events.Context) (*FoldTotal, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	_, store, ctx, server, _ := reducerFixture(t, registry, WithServices(scopes))
	receiveOpening(t, ctx, server.registrations)
	server.operations <- &contracts.ReduceOperationMessage{Partition: "source", Events: []*contracts.AppendedEvent{foldEvent("FoldChanged", 0, `{}`)}}
	awaitSignal(t, ctx, started)
	if err := store.UnregisterReducer(ctx, "fold"); err != nil {
		t.Fatal(err)
	}
	if scopes.closed.Load() != 1 {
		t.Fatal("unregister returned before scope cleanup")
	}
	select {
	case result := <-server.results:
		t.Fatalf("acknowledged canceled operation: %v", result)
	default:
	}
}
