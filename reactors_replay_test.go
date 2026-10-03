// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
)

type ReplayScopeFactory struct{ opened, closed atomic.Int32 }

func (f *ReplayScopeFactory) NewScope(context.Context) (reactors.Scope, error) {
	f.opened.Add(1)
	return &ReplayScope{factory: f}, nil
}

type ReplayScope struct{ factory *ReplayScopeFactory }

func (*ReplayScope) Resolve(context.Context, reflect.Type) (any, error) {
	return nil, errors.New("unexpected resolution")
}
func (s *ReplayScope) Close(context.Context) error { s.factory.closed.Add(1); return nil }

type ReplayRuntimeArtifact struct {
	id     int32
	trace  chan string
	closed *atomic.Int32
	fail   bool
}

func (r *ReplayRuntimeArtifact) Live(ReactorInput)    { r.trace <- "live" }
func (r *ReplayRuntimeArtifact) Rebuild(ReactorInput) { r.trace <- "replay" }
func (r *ReplayRuntimeArtifact) Only(ReactorOutput)   { r.trace <- "only" }
func (r *ReplayRuntimeArtifact) BeginReplay(context.Context) error {
	r.trace <- "begin"
	if r.fail {
		return errors.New("notification failed")
	}
	return nil
}
func (r *ReplayRuntimeArtifact) EndReplay(context.Context) error { r.trace <- "end"; return nil }
func (r *ReplayRuntimeArtifact) BeginReplayPartition(_ context.Context, p events.SourceID) error {
	r.trace <- "begin-" + string(p)
	return nil
}
func (r *ReplayRuntimeArtifact) EndReplayPartition(_ context.Context, p events.SourceID) error {
	r.trace <- "end-" + string(p)
	return nil
}
func (r *ReplayRuntimeArtifact) Close() error { r.closed.Add(1); return nil }

func TestReactorReplayDeliveryAndSeparateNotificationScopes(t *testing.T) {
	r := reactorRegistry(t)
	trace := make(chan string, 16)
	var created, closed, middleware atomic.Int32
	factory := &ReplayScopeFactory{}
	if err := chronicle.RegisterReactor[*ReplayRuntimeArtifact](r, func(ctx context.Context) *ReplayRuntimeArtifact {
		if _, ok := reactors.BatchFromContext(ctx); !ok {
			t.Error("notification missing batch coordinates")
		}
		return &ReplayRuntimeArtifact{id: created.Add(1), trace: trace, closed: &closed}
	}, reactors.Replay("Rebuild", "Only"), reactors.OnceOnly("Live"), reactors.WithMiddleware(func() *ScopeMiddleware { middleware.Add(1); return &ScopeMiddleware{} })); err != nil {
		t.Fatal(err)
	}
	k := &reactorKernel{}
	_, _, ctx := reactorClient(t, k, r, chronicle.WithServices(factory))
	s := receive(t, ctx, k.sessions)
	if len(s.registration.Reactor.EventTypes) != 2 {
		t.Fatal("replay-only subscription missing", s.registration)
	}
	for _, state := range []contracts.ReplayState{contracts.ReplayState_BeginReplay, contracts.ReplayState_BeginReplayPartition, contracts.ReplayState_EndReplayPartition, contracts.ReplayState_EndReplay} {
		s.batches <- &contracts.EventsToObserve{ReplayState: state, Partition: "source"}
	}
	live := batch(0)
	s.batches <- live
	if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success {
		t.Fatal(result)
	}
	replay := batch(1)
	replay.Events[0].Context.ObservationState = contracts.EventObservationState(events.ObservationReplay | events.ObservationInitial)
	s.batches <- replay
	if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success {
		t.Fatal(result)
	}
	// Same real runtime, no live handler for a replay-only subscription.
	only := batch(2)
	only.Events[0].Context.EventType = &contracts.EventType{Id: "ReactorOutput", Generation: 1}
	only.Events[0].Context.ObservationState = contracts.EventObservationState(events.ObservationReplay)
	s.batches <- only
	if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success {
		t.Fatal(result)
	}
	want := []string{"begin", "begin-source", "end-source", "end", "live", "replay", "only"}
	var got []string
	for range want {
		got = append(got, receive(t, ctx, trace))
	}
	if !slices.Equal(got, want) || created.Load() != 7 || closed.Load() != 7 || factory.opened.Load() != 7 || factory.closed.Load() != 7 || middleware.Load() != 3 {
		t.Fatalf("trace %v artifacts %d/%d scopes %d/%d middleware %d", got, created.Load(), closed.Load(), factory.opened.Load(), factory.closed.Load(), middleware.Load())
	}
}

type ScopeMiddleware struct{}

func (*ScopeMiddleware) Before(context.Context, reactors.Invocation) error { return nil }
func (*ScopeMiddleware) After(context.Context, reactors.Invocation) error  { return nil }

func TestReactorOnceOnlySkipsReplayButNotRecovery(t *testing.T) {
	r := reactorRegistry(t)
	var calls atomic.Int32
	if err := chronicle.RegisterReactorHandlers(r, "once", []reactors.Handler{reactors.On(func(context.Context, ReactorInput) error { calls.Add(1); return nil }).OnceOnly()}); err != nil {
		t.Fatal(err)
	}
	k := &reactorKernel{}
	_, _, ctx := reactorClient(t, k, r)
	s := receive(t, ctx, k.sessions)
	for _, state := range []events.ObservationState{events.ObservationInitial, events.ObservationReplay, events.ObservationInitial} {
		b := batch(0)
		b.Events[0].Context.ObservationState = contracts.EventObservationState(state)
		s.batches <- b
		if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success || result.LastSuccessfulObservation != 0 {
			t.Fatal(result)
		}
	}
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
}

func TestReactorNotificationFailureReleasesScopeAndReconnects(t *testing.T) {
	r := reactorRegistry(t)
	trace := make(chan string, 8)
	var closed atomic.Int32
	factory := &ReplayScopeFactory{}
	if err := chronicle.RegisterReactor[*ReplayRuntimeArtifact](r, func() *ReplayRuntimeArtifact {
		return &ReplayRuntimeArtifact{trace: trace, closed: &closed, fail: true}
	}, reactors.Replay("Rebuild", "Only")); err != nil {
		t.Fatal(err)
	}
	k := &reactorKernel{}
	_, _, ctx := reactorClient(t, k, r, chronicle.WithServices(factory))
	s := receive(t, ctx, k.sessions)
	s.batches <- &contracts.EventsToObserve{ReplayState: contracts.ReplayState_BeginReplay}
	next := receive(t, ctx, k.sessions)
	if next == s || closed.Load() != 1 || factory.closed.Load() != 1 {
		t.Fatal("notification failure leaked scope or did not reconnect")
	}
}

func TestConcreteHistoricalGenerationHandlerIsNotLost(t *testing.T) {
	// Today's catalog can select one generation for an ID, but cannot register
	// current and historical codecs together (#32). A historical concrete type
	// explicitly registered with that ID must still contribute its own handler.
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ReactorInput](r, events.WithID("evolved"), events.WithGeneration(1)); err != nil {
		t.Fatal(err)
	}
	observed := make(chan events.Context, 1)
	if err := chronicle.RegisterReactor[*HistoricalReactor](r, func() *HistoricalReactor { return &HistoricalReactor{observed} }); err != nil {
		t.Fatal(err)
	}
	k := &reactorKernel{}
	k.constraintsReady.Store(true) // This catalog intentionally has no constraints to register.
	_, _, ctx := reactorClient(t, k, r)
	s := receive(t, ctx, k.sessions)
	if refs := s.registration.Reactor.EventTypes; len(refs) != 1 || refs[0].EventType.Id != "evolved" || refs[0].EventType.Generation != 1 {
		t.Fatal(refs)
	}
	b := batch(5)
	b.Events[0].Context.EventType = &contracts.EventType{Id: "evolved", Generation: 3}
	b.Events[0].GenerationalContent = map[int32]string{1: `{"number":42}`}
	s.batches <- b
	if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success {
		t.Fatal(result)
	}
	if ec := receive(t, ctx, observed); ec.EventType.Generation != 1 {
		t.Fatal(ec)
	}
}

type HistoricalReactor struct{ observed chan events.Context }

func (r *HistoricalReactor) Handle(e ReactorInput, ec events.Context) error {
	if e.Number != 42 {
		return errors.New("wrong historical payload")
	}
	r.observed <- ec
	return nil
}
