// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type modelWatchSession struct {
	request  *contracts.WatchRequest
	messages chan *contracts.ReadModelChangeset
	end      chan error
	done     chan struct{}
}
type modelReactorKernel struct {
	readModelKernel
	contracts.UnimplementedMaterializedReadModelsServer
	sessions chan *modelWatchSession
	windows  chan *contracts.ObserveInstancesResponse
	observes atomic.Int32
	active   atomic.Int32
}

func (k *modelReactorKernel) Watch(request *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
	k.active.Add(1)
	defer k.active.Add(-1)
	s := &modelWatchSession{request: request, messages: make(chan *contracts.ReadModelChangeset, 8), end: make(chan error, 1), done: make(chan struct{})}
	defer close(s.done)
	select {
	case k.sessions <- s:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-s.end:
			return err
		case message := <-s.messages:
			if err := stream.Send(message); err != nil {
				return err
			}
		}
	}
}
func (k *modelReactorKernel) ObserveInstances(r *contracts.ObserveInstancesRequest, stream grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
	k.active.Add(1)
	defer k.active.Add(-1)
	k.observes.Add(1)
	if r.Page != 0 || r.PageSize != 50 {
		return status.Error(codes.InvalidArgument, "expected default window")
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case window := <-k.windows:
			if err := stream.Send(window); err != nil {
				return err
			}
		}
	}
}
func newModelReactorKernel() (*supervisedKernel, *modelReactorKernel) {
	k := &modelReactorKernel{sessions: make(chan *modelWatchSession, 8), windows: make(chan *contracts.ObserveInstancesResponse, 8)}
	k.register = func(context.Context, *contracts.RegisterManyRequest) error { return nil }
	outer := &supervisedKernel{readModels: k, projections: &projectionKernel{register: func(context.Context, *projectioncontracts.RegisterRequest) error { return nil }}}
	return outer, k
}
func modelMessage(ns string, kind contracts.ReadModelChangeType, value string) *contracts.ReadModelChangeset {
	return &contracts.ReadModelChangeset{Namespace: ns, ModelKey: "person", ReadModel: value, ChangeType: kind, Removed: kind == contracts.ReadModelChangeType_Removed}
}

type readModelEffectScope struct{}

func (readModelEffectScope) GetScope(context.Context, *eventsequences.Sequence, eventsequences.ScopeFilter) (eventsequences.Scope, error) {
	return eventsequences.Scope{Expectation: eventsequences.NoCheck()}, nil
}

type modelScopes struct{ opened, closed atomic.Int32 }

func (s *modelScopes) NewScope(ctx context.Context) (reactors.Scope, error) {
	scope, err := reactors.DefaultScopeFactory().NewScope(ctx)
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return &modelScope{Scope: scope, owner: s}, nil
}

type modelScope struct {
	reactors.Scope
	owner *modelScopes
}

func (s *modelScope) Close(ctx context.Context) error {
	s.owner.closed.Add(1)
	return s.Scope.Close(ctx)
}

type modelDispatch struct {
	kind  string
	ns    string
	count int
}
type ConventionModelReactor struct {
	calls  chan modelDispatch
	closed *atomic.Int32
}

func (r *ConventionModelReactor) Added(_ context.Context, m *ProjectionModel, ec events.Context) ProjectionOpened {
	r.calls <- modelDispatch{"added", string(ec.Namespace), 1}
	return ProjectionOpened{Name: m.Name}
}
func (r *ConventionModelReactor) Modified(models []ProjectionModel, ec events.Context) error {
	r.calls <- modelDispatch{"modified", string(ec.Namespace), len(models)}
	return errors.New("dispatch failure")
}
func (r *ConventionModelReactor) Removed(models []*ProjectionModel, ec events.Context) {
	r.calls <- modelDispatch{"removed", string(ec.Namespace), len(models)}
}
func (r *ConventionModelReactor) Close() error { r.closed.Add(1); return nil }

func TestReadModelReactorConventionsReadyEffectsScopesAndNoRetry(t *testing.T) {
	registry, model := projectionRegistry(t)
	calls := make(chan modelDispatch, 16)
	reported := make(chan error, 16)
	var constructed, closed, appended atomic.Int32
	scopes := &modelScopes{}
	if err := RegisterReadModelReactor[*ConventionModelReactor](registry, model, func() *ConventionModelReactor { constructed.Add(1); return &ConventionModelReactor{calls, &closed} }, reactors.WithReadModelErrorHandler(func(_ context.Context, err error) { reported <- err }), reactors.WithReadModelHandler(reactors.ReadModelOn(readmodels.Modified, func(_ context.Context, m *ProjectionModel) { calls <- modelDispatch{"extra", "", 1} }))); err != nil {
		t.Fatal(err)
	}
	outer, k := newModelReactorKernel()
	outer.appendCall = func(context.Context) error { appended.Add(1); return nil }
	client, ctx := supervisionClient(t, outer, WithRegistry(registry), WithDefaultConcurrencyStrategy(readModelEffectScope{}), WithServices(scopes))
	opened := make(chan *EventStore, 1)
	openErr := make(chan error, 1)
	go func() {
		s, err := client.EventStore(ctx, "store", WithNamespace("one"))
		if err != nil {
			openErr <- err
		} else {
			opened <- s
		}
	}()
	var session *modelWatchSession
	select {
	case session = <-k.sessions:
	case err := <-openErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-opened:
		t.Fatal("registration returned before Subscribed")
	default:
	}
	if constructed.Load() != 0 {
		t.Fatal("constructed before dispatch")
	}
	session.messages <- &contracts.ReadModelChangeset{Subscribed: true}
	var store *EventStore
	select {
	case store = <-opened:
	case err := <-openErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, m := range []*contracts.ReadModelChangeset{modelMessage("one", contracts.ReadModelChangeType_Added, `{"id":"person","name":"Ada"}`), modelMessage("one", contracts.ReadModelChangeType_Modified, `{"id":"person","name":"Grace"}`), modelMessage("one", contracts.ReadModelChangeType_Removed, `null`)} {
		session.messages <- m
	}
	for _, want := range []modelDispatch{{"added", "one", 1}, {"modified", "one", 1}, {"extra", "", 1}, {"removed", "one", 0}} {
		select {
		case got := <-calls:
			if got != want {
				t.Fatalf("call = %+v want %+v", got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-reported:
	case <-ctx.Done():
		t.Fatal("missing dispatch failure")
	}
	session.end <- status.Error(codes.Unavailable, "watch lost")
	select {
	case err := <-reported:
		if !errors.Is(err, readmodels.ErrInterrupted) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("missing terminal error")
	}
	// Joining via unregister proves no callback/activation outlives the handle.
	id := reactors.ID(reflect.TypeFor[ConventionModelReactor]().PkgPath() + ".ConventionModelReactor")
	if err := store.UnregisterReadModelReactor(ctx, id); err != nil {
		t.Fatal(err)
	}
	if scopes.opened.Load() != 4 || scopes.closed.Load() != 4 {
		t.Fatalf("scopes: opened=%d closed=%d", scopes.opened.Load(), scopes.closed.Load())
	}
	if constructed.Load() != 4 || closed.Load() != 4 || appended.Load() != 1 {
		t.Fatalf("lifecycle constructors=%d close=%d effects=%d", constructed.Load(), closed.Load(), appended.Load())
	}
	select {
	case <-k.sessions:
		t.Fatal("silently resumed interrupted feed")
	default:
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if k.active.Load() != 0 {
		select {
		case <-session.done:
		case <-ctx.Done():
			t.Fatal("server handler leaked")
		}
	}
}

func TestReadModelReactorMaterializedUsesWindowsAndJoinsCancellation(t *testing.T) {
	registry, model := projectionRegistry(t)
	calls := make(chan modelDispatch, 8)
	reported := make(chan error, 8)
	handlers := []reactors.ReadModelHandler{
		reactors.ReadModelOn(readmodels.Added, func(m *ProjectionModel, ec events.Context) {
			if ec.SequenceNumber != events.Unavailable {
				t.Error("invented position")
			}
			calls <- modelDispatch{"added", string(ec.SourceID), 1}
		}),
		reactors.ReadModelOn(readmodels.Modified, func(m *ProjectionModel) { calls <- modelDispatch{"modified", m.ID, 1} }),
		reactors.ReadModelOn(readmodels.Removed, func(m *ProjectionModel, ec events.Context) {
			if m != nil {
				t.Error("removed window value not nil")
			}
			calls <- modelDispatch{"removed", string(ec.SourceID), 0}
		}),
	}
	if err := RegisterReadModelReactorHandlers(registry, "window", model, handlers, reactors.Materialized(nil), reactors.WithReadModelErrorHandler(func(_ context.Context, e error) { reported <- e })); err != nil {
		t.Fatal(err)
	}
	outer, k := newModelReactorKernel()
	k.windows <- &contracts.ObserveInstancesResponse{Instances: []string{`{"id":"person","name":"Ada"}`}}
	client, ctx := supervisionClient(t, outer, WithRegistry(registry))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	k.windows <- &contracts.ObserveInstancesResponse{Instances: []string{`{"id":"person","name":"Grace"}`}}
	k.windows <- &contracts.ObserveInstancesResponse{}
	for _, kind := range []string{"added", "modified", "removed"} {
		select {
		case got := <-calls:
			if got.kind != kind || got.ns != "person" {
				t.Fatal(got)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := store.UnregisterReadModelReactor(ctx, "window"); err != nil {
		t.Fatal(err)
	}
	if k.observes.Load() != 1 || len(k.sessions) != 0 {
		t.Fatal("materialized reactor used Watch or restarted")
	}
	select {
	case err := <-reported:
		t.Fatal(err)
	default:
	}
}

func TestReadModelReactorStartupReporterCanReenterSameStore(t *testing.T) {
	registry, model := projectionRegistry(t)
	var client *Client
	reported := make(chan error, 1)
	if err := RegisterReadModelReactorHandlers(registry, "startup", model,
		[]reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(*ProjectionModel) {})},
		reactors.WithReadModelErrorHandler(func(ctx context.Context, err error) {
			if status.Code(err) != codes.Unavailable {
				t.Errorf("startup error = %v", err)
			}
			_, err = client.EventStore(ctx, "store")
			reported <- err
		})); err != nil {
		t.Fatal(err)
	}
	outer, k := newModelReactorKernel()
	var ctx context.Context
	client, ctx = supervisionClient(t, outer, WithRegistry(registry))
	registered := make(chan error, 1)
	go func() { _, err := client.EventStore(ctx, "store"); registered <- err }()
	select {
	case session := <-k.sessions:
		session.end <- status.Error(codes.Unavailable, "startup failed")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, result := range []<-chan error{registered, reported} {
		select {
		case err := <-result:
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("registration/reentrant call = %v", err)
			}
		case <-ctx.Done():
			t.Fatal("startup reporter blocked registration", ctx.Err())
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

type plannedReactorModel struct {
	ID         string
	ExternalID string `json:"ID"`
}

func TestReadModelReactorDispatchUsesRegisteredNamingPlan(t *testing.T) {
	for _, materialized := range []bool{false, true} {
		t.Run(map[bool]string{false: "watch", true: "materialized"}[materialized], func(t *testing.T) {
			registry := NewRegistry()
			model, err := RegisterReadModel[plannedReactorModel](registry, readmodels.WithObserver(readmodels.Projection, "planned"))
			if err != nil {
				t.Fatal(err)
			}
			want := plannedReactorModel{ID: "person", ExternalID: "external"}
			data, err := model.Descriptor().Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			calls := make(chan plannedReactorModel, 1)
			options := []reactors.ReadModelOption{reactors.WithReadModelErrorHandler(func(_ context.Context, err error) { t.Error(err) })}
			if materialized {
				options = append(options, reactors.Materialized(nil))
			}
			if err := RegisterReadModelReactorHandlers(registry, "planned", model,
				[]reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(m *plannedReactorModel) { calls <- *m })}, options...); err != nil {
				t.Fatal(err)
			}
			outer, k := newModelReactorKernel()
			client, ctx := supervisionClient(t, outer, WithRegistry(registry))
			registered := make(chan error, 1)
			go func() { _, err := client.EventStore(ctx, "store"); registered <- err }()
			if materialized {
				k.windows <- &contracts.ObserveInstancesResponse{Instances: []string{string(data)}}
			} else {
				select {
				case session := <-k.sessions:
					session.messages <- &contracts.ReadModelChangeset{Subscribed: true}
					session.messages <- modelMessage(string(DefaultNamespace), contracts.ReadModelChangeType_Added, string(data))
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case err := <-registered:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case got := <-calls:
				if got != want {
					t.Fatalf("model = %+v, want %+v", got, want)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

type InvalidModelReactor struct{}

func (*InvalidModelReactor) Added(string) {}
func TestReadModelReactorValidationAtNewClientAndFrozenRegistries(t *testing.T) {
	registry, model := projectionRegistry(t)
	if err := RegisterReadModelReactor[*InvalidModelReactor](registry, model, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(WithRegistry(registry)); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("signature accepted: %v", err)
	}
	registry, model = projectionRegistry(t)
	if err := RegisterReadModelReactorHandlers(registry, "same", model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(*ProjectionModel) {})}); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := RegisterReadModelReactorHandlers(registry, "same", model, nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("duplicate: %v", err)
	}
	if err := RegisterReadModelReactorHandlers(registry, "later", model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(*ProjectionModel) {})}); err != nil {
		t.Fatal(err)
	}
	if len(client.readModelReactors.defaults) != 1 {
		t.Fatal("client registry mutated")
	}
}
func TestReadModelReactorGenerationRetiresAndNamespacesStayIsolated(t *testing.T) {
	registry, model := projectionRegistry(t)
	calls := make(chan string, 8)
	entered, release := make(chan struct{}), make(chan struct{})
	if err := RegisterReadModelReactorHandlers(registry, "generation", model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(ctx context.Context, _ *ProjectionModel, ec events.Context) {
		calls <- string(ec.Namespace)
		if ec.Namespace == "one" {
			close(entered)
			<-release
		}
	})}); err != nil {
		t.Fatal(err)
	}
	outer, k := newModelReactorKernel()
	client, ctx := supervisionClient(t, outer, WithRegistry(registry))
	open := func(namespace Namespace) *modelWatchSession {
		done := make(chan error, 1)
		go func() { _, err := client.EventStore(ctx, "store", WithNamespace(namespace)); done <- err }()
		var session *modelWatchSession
		select {
		case session = <-k.sessions:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		session.messages <- &contracts.ReadModelChangeset{Subscribed: true}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		return session
	}
	one, two := open("one"), open("two")
	one.messages <- modelMessage("one", contracts.ReadModelChangeType_Added, `{"id":"person"}`)
	<-entered
	two.messages <- modelMessage("two", contracts.ReadModelChangeType_Added, `{"id":"person"}`)
	if first, second := <-calls, <-calls; first != "one" || second != "two" {
		t.Fatalf("namespace calls: %s %s", first, second)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := client.CloseContext(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close did not wait: %v", err)
	}
	close(release)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*modelWatchSession{one, two} {
		select {
		case <-s.done:
		case <-ctx.Done():
			t.Fatal("watch leaked")
		}
	}
}
