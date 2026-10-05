// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type replayReducerDependency struct{ value int }
type replayReducerScopes struct {
	opened, closed      atomic.Int32
	failOpen, failClose error
	artifact            *replayReducer
}

func (f *replayReducerScopes) Contains(typ reflect.Type) bool {
	return typ == reflect.TypeFor[*replayReducerDependency]() || (f.artifact != nil && typ == reflect.TypeFor[*replayReducer]())
}
func (f *replayReducerScopes) NewScope(context.Context) (reducers.Scope, error) {
	f.opened.Add(1)
	return &replayReducerScope{f}, f.failOpen
}

type replayReducerScope struct{ owner *replayReducerScopes }

func (s *replayReducerScope) Resolve(_ context.Context, typ reflect.Type) (any, error) {
	if typ == reflect.TypeFor[*replayReducer]() {
		return s.owner.artifact, nil
	}
	if typ == reflect.TypeFor[*replayReducerDependency]() {
		return &replayReducerDependency{7}, nil
	}
	return nil, errors.New("unexpected service")
}
func (s *replayReducerScope) Close(ctx context.Context) error {
	s.owner.closed.Add(1)
	if s.owner.artifact != nil {
		return s.owner.artifact.Close(ctx)
	}
	return s.owner.failClose
}

type replayReducer struct {
	callbacks reducers.ReplayCallbacks
	fold      func(context.Context, FoldChanged, *FoldTotal, events.Context) (*FoldTotal, error)
	closed    *atomic.Int32
}

func (r *replayReducer) Change(ctx context.Context, e FoldChanged, current *FoldTotal, ec events.Context) (*FoldTotal, error) {
	return r.fold(ctx, e, current, ec)
}
func (r *replayReducer) BeginReplay(ctx context.Context) error { return r.callbacks.BeginReplay(ctx) }
func (r *replayReducer) EndReplay(ctx context.Context) error   { return r.callbacks.EndReplay(ctx) }
func (r *replayReducer) BeginReplayPartition(ctx context.Context, key events.SourceID) error {
	return r.callbacks.BeginReplayPartition(ctx, key)
}
func (r *replayReducer) EndReplayPartition(ctx context.Context, key events.SourceID) error {
	return r.callbacks.EndReplayPartition(ctx, key)
}
func (r *replayReducer) Close(context.Context) error { r.closed.Add(1); return nil }

func replayRegistry(t *testing.T) (*Registry, readmodels.Model[FoldTotal]) {
	t.Helper()
	r := NewRegistry()
	if _, err := RegisterEvent[FoldChanged](r, events.WithGeneration(2)); err != nil {
		t.Fatal(err)
	}
	m, err := RegisterReadModel[FoldTotal](r)
	if err != nil {
		t.Fatal(err)
	}
	return r, m
}
func replayFoldOperation(n uint64) *contracts.ReduceOperationMessage {
	return &contracts.ReduceOperationMessage{Partition: "source", Events: []*contracts.AppendedEvent{foldEvent("FoldChanged", n, `{"amount":3}`)}}
}

func TestReducerReplayNotificationsShareProductionOrderAndFreshScopes(t *testing.T) {
	var fingerprint string
	for _, authoring := range []string{"manual", "reflective", "constructorDI", "scopeOwned"} {
		t.Run(authoring, func(t *testing.T) {
			r, m := replayRegistry(t)
			trace := make(chan string, 32)
			var created, closed atomic.Int32
			scopes := &replayReducerScopes{}
			check := func(ctx context.Context, system bool) {
				t.Helper()
				batch, ok := reducers.BatchFromContext(ctx)
				if !ok || batch != (reducers.Batch{Reducer: "fold", Store: "store", Namespace: DefaultNamespace, Sequence: events.EventLog}) {
					t.Errorf("batch = %+v, present=%v", batch, ok)
				}
				want := identities.NotSet()
				if system {
					want = identities.System()
				}
				if !reflect.DeepEqual(metadata.Identity(ctx), want) {
					t.Errorf("identity=%+v want=%+v", metadata.Identity(ctx), want)
				}
			}
			callbacks := reducers.ReplayCallbacks{
				BeginReplay: func(ctx context.Context) error { check(ctx, false); trace <- "begin"; return nil },
				EndReplay:   func(ctx context.Context) error { check(ctx, false); trace <- "end"; return nil },
				BeginReplayPartition: func(ctx context.Context, key events.SourceID) error {
					check(ctx, false)
					trace <- "begin:" + string(key)
					return nil
				},
				EndReplayPartition: func(ctx context.Context, key events.SourceID) error {
					check(ctx, false)
					trace <- "end:" + string(key)
					return nil
				},
			}
			fold := func(ctx context.Context, e FoldChanged, current *FoldTotal, ec events.Context) (*FoldTotal, error) {
				check(ctx, true)
				if ec.CausedBy.Subject != "human" || ec.EventType.Generation != 1 {
					t.Errorf("fold metadata=%+v", ec)
				}
				trace <- "fold"
				return sumFold(ctx, e, current, ec)
			}
			construct := func(ctx context.Context) *replayReducer {
				created.Add(1)
				// The first and last operations are folds; notifications are not
				// allowed to inherit the System identity or first event's metadata.
				check(ctx, reflect.DeepEqual(metadata.Identity(ctx), identities.System()))
				return &replayReducer{callbacks, fold, &closed}
			}
			var options []ClientOption
			if authoring == "manual" {
				if err := RegisterReducerHandlers(r, m, "fold", []reducers.Handler{reducers.On(fold)}, reducers.WithReplayCallbacks(callbacks)); err != nil {
					t.Fatal(err)
				}
				options = append(options, WithServices(scopes))
			} else {
				var factory any = construct
				if authoring == "constructorDI" {
					factory = func(ctx context.Context, dependency *replayReducerDependency) *replayReducer {
						if dependency.value != 7 {
							t.Error("constructor dependency lost")
						}
						return construct(ctx)
					}
					options = append(options, WithServices(scopes))
				}
				if authoring == "scopeOwned" {
					scopes.artifact = &replayReducer{callbacks, fold, &closed}
					options = append(options, WithServices(scopes))
				}
				if err := RegisterReducer[*replayReducer](r, m, factory, reducers.WithID("fold")); err != nil {
					t.Fatal(err)
				}
			}
			var server *reducerKernel
			t.Cleanup(func() {
				// Registered before the fixture: runs after WaitForHandlers(true)
				// Stop has joined the production gRPC handlers.
				if server != nil && server.handlers.Load() != 0 {
					t.Error("reducer stream handler leaked")
				}
			})
			client, store, ctx, openedServer, _ := reducerFixture(t, r, options...)
			server = openedServer
			registration := receiveOpening(t, ctx, server.registrations)
			if fingerprint == "" {
				fingerprint = registration.Reducer.Hash
			}
			if registration.Reducer.Hash != fingerprint {
				t.Error("manual and reflective lifecycle plans differ")
			}
			server.operations <- replayFoldOperation(0)
			if result := receiveOpening(t, ctx, server.results); result.State != contracts.ObservationState_Success || result.LastSuccessfulObservation != 0 {
				t.Fatal(result)
			}
			key := `partition/{"a":1}/ untouched`
			for _, state := range []contracts.ReplayState{contracts.ReplayState_BeginReplay, contracts.ReplayState_BeginReplayPartition, contracts.ReplayState_EndReplayPartition, contracts.ReplayState_EndReplay, contracts.ReplayState_BeginReplay} {
				server.operations <- &contracts.ReduceOperationMessage{ReplayState: state, Partition: key}
			}
			server.operations <- replayFoldOperation(1)
			if result := receiveOpening(t, ctx, server.results); result.State != contracts.ObservationState_Success || result.LastSuccessfulObservation != 1 || result.ReadModelState == "" {
				t.Fatal(result)
			}
			want := []string{"fold", "begin", "begin:" + key, "end:" + key, "end", "begin", "fold"}
			var got []string
			for range want {
				got = append(got, receiveOpening(t, ctx, trace))
			}
			if !slices.Equal(got, want) {
				t.Fatal(got)
			}
			if authoring != "reflective" && (scopes.opened.Load() != 7 || scopes.closed.Load() != 7) {
				t.Fatalf("scopes %d/%d", scopes.opened.Load(), scopes.closed.Load())
			}
			if authoring == "manual" && created.Load() != 0 {
				t.Fatal(created.Load())
			}
			if authoring != "manual" && closed.Load() != 7 {
				t.Fatal(closed.Load())
			}
			if authoring == "scopeOwned" && created.Load() != 0 {
				t.Fatal("borrowed service bypassed", created.Load())
			}
			if authoring == "reflective" || authoring == "constructorDI" {
				if created.Load() != 7 {
					t.Fatal(created.Load())
				}
			}
			if err := store.UnregisterReducer(ctx, "fold"); err != nil {
				t.Fatal(err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-server.results:
				t.Fatal("notification acknowledged", result)
			default:
			}
		})
	}
}

func TestReducerNotificationFailuresReconnectOnlyObserverAndNeverAcknowledge(t *testing.T) {
	for _, mode := range []string{"handler", "panic", "activation", "scope", "cleanup", "future"} {
		t.Run(mode, func(t *testing.T) {
			r, m := replayRegistry(t)
			var created, closed, calls atomic.Int32
			failure := errors.New("notification failure")
			scopes := &replayReducerScopes{}
			if mode == "scope" {
				scopes.failOpen = failure
			}
			if mode == "cleanup" {
				scopes.failClose = failure
			}
			callbacks := reducers.ReplayCallbacks{
				BeginReplay: func(context.Context) error {
					calls.Add(1)
					if mode == "panic" {
						panic(failure)
					}
					if mode == "handler" {
						return failure
					}
					return nil
				},
				EndReplay:            func(context.Context) error { t.Error("end dispatched without delivery"); return nil },
				BeginReplayPartition: func(context.Context, events.SourceID) error { return nil },
				EndReplayPartition:   func(context.Context, events.SourceID) error { return nil },
			}
			if err := RegisterReducer[*replayReducer](r, m, func() (*replayReducer, error) {
				created.Add(1)
				artifact := &replayReducer{callbacks, sumFold, &closed}
				if mode == "activation" {
					return artifact, failure
				}
				return artifact, nil
			}, reducers.WithID("fold")); err != nil {
				t.Fatal(err)
			}
			client, store, ctx, server, _ := reducerFixture(t, r, WithServices(scopes))
			first := receiveOpening(t, ctx, server.registrations)
			state := contracts.ReplayState_BeginReplay
			if mode == "future" {
				state = contracts.ReplayState(99)
			}
			server.operations <- &contracts.ReduceOperationMessage{ReplayState: state}
			next := receiveOpening(t, ctx, server.registrations)
			if first.ConnectionId != next.ConnectionId {
				t.Fatal("notification failure retired shared generation")
			}
			if mode == "future" {
				if scopes.opened.Load() != 0 || calls.Load() != 0 || created.Load() != 0 {
					t.Fatal("future state executed user code")
				}
			} else {
				if scopes.opened.Load() != 1 || scopes.closed.Load() != 1 {
					t.Fatalf("scopes=%d/%d", scopes.opened.Load(), scopes.closed.Load())
				}
				if mode != "scope" && closed.Load() != 1 {
					t.Fatal("artifact not cleaned", closed.Load())
				}
			}
			if err := store.UnregisterReducer(ctx, "fold"); err != nil {
				t.Fatal(err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-server.results:
				t.Fatal("notification failure acknowledged", result)
			default:
			}
		})
	}
}

func TestReducerNotificationCancellationUnregisterAndCallbackCloseContextJoin(t *testing.T) {
	for _, mode := range []string{"unregister", "generation", "callbackClose"} {
		t.Run(mode, func(t *testing.T) {
			r, m := replayRegistry(t)
			entered := make(chan struct{}, 1)
			finished := make(chan error, 1)
			scopes := &replayReducerScopes{}
			var client *Client
			callbacks := reducers.ReplayCallbacks{BeginReplay: func(ctx context.Context) error {
				entered <- struct{}{}
				if mode == "callbackClose" {
					finished <- client.CloseContext(ctx)
					return nil
				}
				<-ctx.Done()
				finished <- ctx.Err()
				return ctx.Err()
			}}
			if err := RegisterReducerHandlers(r, m, "fold", []reducers.Handler{reducers.On(sumFold)}, reducers.WithReplayCallbacks(callbacks)); err != nil {
				t.Fatal(err)
			}
			var store *EventStore
			var ctx context.Context
			var server *reducerKernel
			var kernel *supervisedKernel
			client, store, ctx, server, kernel = reducerFixture(t, r, WithServices(scopes))
			receiveOpening(t, ctx, server.registrations)
			server.operations <- &contracts.ReduceOperationMessage{ReplayState: contracts.ReplayState_BeginReplay}
			receiveOpening(t, ctx, entered)
			switch mode {
			case "generation":
				kernel.endStream <- errors.New("replace generation")
				receiveOpening(t, ctx, server.registrations)
			case "unregister":
				if err := store.UnregisterReducer(ctx, "fold"); err != nil {
					t.Fatal(err)
				}
			}
			if err := receiveOpening(t, ctx, finished); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if scopes.opened.Load() != 1 || scopes.closed.Load() != 1 {
				t.Fatalf("scopes=%d/%d", scopes.opened.Load(), scopes.closed.Load())
			}
			select {
			case result := <-server.results:
				t.Fatal("canceled notification acknowledged", result)
			default:
			}
		})
	}
}

type invalidReplayResult struct{}

func (*invalidReplayResult) Fold(FoldChanged, *FoldTotal) *FoldTotal { return nil }
func (*invalidReplayResult) BeginReplay(context.Context)             {}
func (*invalidReplayResult) EndReplay(context.Context) error         { return nil }

type invalidReplayPartition struct{}

func (*invalidReplayPartition) Fold(FoldChanged, *FoldTotal) *FoldTotal                   { return nil }
func (*invalidReplayPartition) BeginReplayPartition(context.Context, string) error        { return nil }
func (*invalidReplayPartition) EndReplayPartition(context.Context, events.SourceID) error { return nil }

type incompleteReplay struct{}

func (*incompleteReplay) Fold(FoldChanged, *FoldTotal) *FoldTotal { return nil }
func (*incompleteReplay) BeginReplay(context.Context) error       { return nil }

type replayNamedFold struct{}

func (*replayNamedFold) BeginReplay(FoldChanged, *FoldTotal) *FoldTotal { return &FoldTotal{Amount: 9} }

func TestReducerFoldNamesAreNotLifecycleSelectors(t *testing.T) {
	r, m := replayRegistry(t)
	if err := RegisterReducer[*replayNamedFold](r, m, nil); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(r))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	plan := client.reducers.defaults[0]
	if err := plan.NotifyReplay(t.Context(), reducers.BeginReplay, ""); err != nil {
		t.Fatal(err)
	}
	result := plan.Reduce(t.Context(), []reducers.Event{{Content: FoldChanged{}, Context: events.Context{EventType: events.TypeRef{ID: "FoldChanged", Generation: 2}, SequenceNumber: 0}}}, nil)
	if result.Err != nil || result.State.(*FoldTotal).Amount != 9 || result.LastSuccessful != 0 {
		t.Fatal(result)
	}
}

type replayPrefixHelper struct{}

func (*replayPrefixHelper) Fold(FoldChanged, *FoldTotal) *FoldTotal { return nil }
func (*replayPrefixHelper) BeginReplaySomething(int)                { panic("prefix helper invoked") }

func TestReducerNotificationSignaturesValidateAtNewClient(t *testing.T) {
	for _, mode := range []string{"result", "partition", "incomplete", "conflict", "prefix"} {
		t.Run(mode, func(t *testing.T) {
			r, m := replayRegistry(t)
			var err error
			switch mode {
			case "result":
				err = RegisterReducer[*invalidReplayResult](r, m, nil)
			case "partition":
				err = RegisterReducer[*invalidReplayPartition](r, m, nil)
			case "incomplete":
				err = RegisterReducer[*incompleteReplay](r, m, nil)
			case "conflict":
				err = RegisterReducer[*replayReducer](r, m, nil, reducers.WithReplayCallbacks(reducers.ReplayCallbacks{BeginReplay: func(context.Context) error { return nil }}))
			case "prefix":
				err = RegisterReducer[*replayPrefixHelper](r, m, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(WithRegistry(r))
			if mode == "prefix" {
				if err != nil {
					t.Fatal(err)
				}
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
				return
			}
			var detail *reducers.DeclarationError
			if client != nil || !errors.Is(err, ErrInvalidConfiguration) || !errors.As(err, &detail) || detail.Method == "" {
				t.Fatal(client, err)
			}
		})
	}
}
