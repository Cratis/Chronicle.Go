// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	constraintdefs "github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/contracts/clients"
	constraints "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ReactorInput struct {
	Number int `json:"number"`
}
type ReactorOutput struct {
	Number int `json:"number"`
}
type observerSession struct {
	registration *contracts.RegisterReactor
	batches      chan *contracts.EventsToObserve
	results      chan *contracts.ReactorResult
	end          chan error
	done         chan struct{}
}
type reactorKernel struct {
	fakeKernel
	contracts.UnimplementedReactorsServer
	modelcontracts.UnimplementedReadModelsServer
	sessions         chan *observerSession
	active           atomic.Int32
	constraintsReady atomic.Bool
	readKey          chan string
	modelJSON        string
	endConnection    chan error
}

func (k *reactorKernel) Connect(request *clients.ConnectRequest, stream grpc.ServerStreamingServer[clients.ConnectionKeepAlive]) error {
	k.connectCalls.Add(1)
	if err := stream.Send(&clients.ConnectionKeepAlive{ConnectionId: request.ConnectionId}); err != nil {
		return err
	}
	select {
	case err := <-k.endConnection:
		return err
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func (k *reactorKernel) Register(context.Context, *constraints.RegisterConstraintsRequest) (*emptypb.Empty, error) {
	k.constraintsReady.Store(true)
	return &emptypb.Empty{}, nil
}
func (k *reactorKernel) Observe(stream grpc.BidiStreamingServer[contracts.ReactorMessage, contracts.EventsToObserve]) error {
	k.active.Add(1)
	defer k.active.Add(-1)
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if !k.constraintsReady.Load() || k.registrations.Load() == 0 {
		return errors.New("observer started before definitions")
	}
	s := &observerSession{registration: first.GetContent().GetValue0(), batches: make(chan *contracts.EventsToObserve, 1), results: make(chan *contracts.ReactorResult, 1), end: make(chan error, 1), done: make(chan struct{})}
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
		case batch := <-s.batches:
			if err := stream.Send(batch); err != nil {
				return err
			}
			if batch.ReplayState != contracts.ReplayState_REPLAY_STATE_None {
				continue
			}
			response, err := stream.Recv()
			if err != nil {
				return err
			}
			select {
			case s.results <- response.GetContent().GetValue1():
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
	}
}
func (k *reactorKernel) RegisterMany(context.Context, *modelcontracts.RegisterManyRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (k *reactorKernel) GetInstanceByKey(_ context.Context, r *modelcontracts.GetInstanceByKeyRequest) (*modelcontracts.GetInstanceByKeyResponse, error) {
	k.readKey <- r.ReadModelKey
	document := k.modelJSON
	if document == "" {
		document = `{"id":"customer","name":"Ada"}`
	}
	return &modelcontracts.GetInstanceByKeyResponse{ReadModel: document, LastHandledEventSequenceNumber: 1}, nil
}
func reactorClient(t *testing.T, k *reactorKernel, registry *chronicle.Registry, options ...chronicle.ClientOption) (*chronicle.Client, *chronicle.EventStore, context.Context) {
	t.Helper()
	k.sessions = make(chan *observerSession, 8)
	k.readKey = make(chan string, 8)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.WaitForHandlers(true)) // Stop joins handlers before the leak check.
	clients.RegisterConnectionServiceServer(server, k)
	eventstores.RegisterEventStoresServer(server, k)
	eventtypes.RegisterEventTypesServer(server, k)
	namespaces.RegisterNamespacesServer(server, k)
	constraints.RegisterConstraintsServer(server, k)
	sequences.RegisterEventSequencesServer(server, k)
	contracts.RegisterReactorsServer(server, k)
	modelcontracts.RegisterReadModelsServer(server, k)
	done := make(chan struct{})
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-done
		if k.active.Load() != 0 {
			t.Error("observer server leaked")
		}
	})
	go serveKernelFixture(server, listener, done, t.Error)
	conn, err := grpc.NewClient("passthrough:///reactors", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	base := []chronicle.ClientOption{chronicle.WithRegistry(registry), chronicle.WithGRPCConnection(conn), chronicle.WithNoAuthentication(), chronicle.WithKeepAliveTimeout(time.Minute)}
	client, err := chronicle.NewClient(append(base, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := client.EventStore(ctx, "reactors")
	if err != nil {
		t.Fatal(err)
	}
	return client, store, ctx
}
func receive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}
func reactorRegistry(t *testing.T) *chronicle.Registry {
	t.Helper()
	r := chronicle.NewRegistry()
	input, err := chronicle.RegisterEvent[ReactorInput](r, events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	constraint, err := constraintdefs.UniqueValues("numbers").On(input.Descriptor(), "number").Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = r.AddConstraint(constraint); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ReactorOutput](r); err != nil {
		t.Fatal(err)
	}
	return r
}
func batch(numbers ...int) *contracts.EventsToObserve {
	b := &contracts.EventsToObserve{Partition: "source"}
	for _, n := range numbers {
		b.Events = append(b.Events, &contracts.AppendedEvent{Context: &contracts.EventContext{EventType: &contracts.EventType{Id: "ReactorInput", Generation: 1}, EventSourceId: "source", EventStore: "reactors", Namespace: "Default", SequenceNumber: uint64(n), Occurred: &contracts.SerializableDateTimeOffset{Value: "2026-01-01T00:00:00Z"}}, Content: `{"number":-1}`, GenerationalContent: map[int32]string{2: fmt.Sprintf(`{"number":%d}`, n)}})
	}
	return b
}

type BatchReactor struct {
	handled  *[]int
	releases *atomic.Int32
	failAt   int
}

func (r *BatchReactor) Anything(ctx context.Context, event *ReactorInput, ec events.Context, delivery reactors.Delivery) (ReactorOutput, error) {
	if event.Number != int(ec.SequenceNumber) || ec.EventType.Generation != 2 || delivery.Partition != "source" || metadata.Identity(ctx).Subject != "5d032c92-9d5e-41eb-947a-ee5314ed0032" || metadata.CausationChain(ctx)[0].Properties["ReactorId"] != "batch" {
		return ReactorOutput{}, errors.New("metadata/generation mismatch")
	}
	*r.handled = append(*r.handled, event.Number)
	if event.Number == r.failAt {
		return ReactorOutput{}, errors.New("handler failure")
	}
	return ReactorOutput{event.Number}, nil
}
func (r *BatchReactor) Close() error { r.releases.Add(1); return nil }

type countingScopes struct {
	opened, closed atomic.Int32
	closeError     error
}

func (f *countingScopes) NewScope(ctx context.Context) (reactors.Scope, error) {
	if batch, ok := reactors.BatchFromContext(ctx); !ok || batch.Store != "reactors" || batch.Namespace != "Default" {
		return nil, errors.New("scope missing batch coordinates")
	}
	f.opened.Add(1)
	return &countingScope{owner: f}, nil
}

type countingScope struct{ owner *countingScopes }

func (*countingScope) Resolve(context.Context, reflect.Type) (any, error) {
	return nil, errors.New("unexpected resolve")
}
func (s *countingScope) Close(context.Context) error {
	s.owner.closed.Add(1)
	return s.owner.closeError
}

func TestReactorOrderedBatchPartialFailureAndCleanupBeforeAck(t *testing.T) {
	registry := reactorRegistry(t)
	var handled []int
	var releases atomic.Int32
	scopes := &countingScopes{}
	constructed := 0
	if err := chronicle.RegisterReactor[*BatchReactor](registry, func() *BatchReactor { constructed++; return &BatchReactor{&handled, &releases, 2} }, reactors.WithID("batch")); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	_, _, ctx := reactorClient(t, kernel, registry, chronicle.WithServices(scopes))
	session := receive(t, ctx, kernel.sessions)
	definition := session.registration.Reactor
	if !definition.IsReplayable || definition.Filters.EventStreamType != "All" || definition.Filters.EventSourceType != "" || len(definition.Tags) != 0 || len(definition.Filters.FilterTags) != 0 {
		t.Fatal("incorrect default reactor metadata", definition)
	}
	if definition.ReactorId != "batch" || definition.EventTypes[0].Key != "$eventSourceId" || definition.EventTypes[0].EventType.Generation != 2 {
		t.Fatal(session.registration)
	}
	session.batches <- batch(0, 1, 2, 3)
	result := receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Failed || result.LastSuccessfulObservation != 1 || len(result.ExceptionMessages) == 0 {
		t.Fatal(result)
	}
	if !slices.Equal(handled, []int{0, 1, 2}) || kernel.appendCalls.Load() != 2 || scopes.opened.Load() != 1 || scopes.closed.Load() != 1 || releases.Load() != 1 || constructed != 1 {
		t.Fatalf("handled %v scopes %d/%d releases %d constructors %d", handled, scopes.opened.Load(), scopes.closed.Load(), releases.Load(), constructed)
	}
	session.batches <- batch(4)
	result = receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Success || result.LastSuccessfulObservation != 4 || constructed != 2 {
		t.Fatal(result, constructed)
	}
}
func TestReactorPerEventScopeAndAppendRejection(t *testing.T) {
	registry := reactorRegistry(t)
	scopes := &countingScopes{}
	var handled []int
	var releases atomic.Int32
	if err := chronicle.RegisterReactor[*BatchReactor](registry, func() *BatchReactor { return &BatchReactor{&handled, &releases, -1} }, reactors.WithID("batch"), reactors.PerEvent()); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	kernel.append = func(context.Context, *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{HasErrors: true, Errors: []string{"failed"}}}, nil
	}
	_, _, ctx := reactorClient(t, kernel, registry, chronicle.WithServices(scopes))
	session := receive(t, ctx, kernel.sessions)
	session.batches <- batch(0, 1)
	result := receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Failed || result.LastSuccessfulObservation != uint64(events.Unavailable) || kernel.appendCalls.Load() != 1 || scopes.closed.Load() != 1 {
		t.Fatal(result, scopes.closed.Load())
	}
}
func TestReactorCleanupFailureDoesNotAcknowledgeCompletedEffects(t *testing.T) {
	registry := reactorRegistry(t)
	scopes := &countingScopes{closeError: errors.New("cleanup")}
	if err := chronicle.RegisterReactorHandler(registry, "callback", func(context.Context, ReactorInput) error { return nil }); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	_, _, ctx := reactorClient(t, kernel, registry, chronicle.WithServices(scopes))
	session := receive(t, ctx, kernel.sessions)
	session.batches <- batch(0, 1)
	result := receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Failed || result.LastSuccessfulObservation != uint64(events.Unavailable) {
		t.Fatal(result)
	}
}

type hook struct {
	name                string
	trace               *[]string
	beforeErr, afterErr error
}

func (m *hook) Before(_ context.Context, i reactors.Invocation) error {
	*m.trace = append(*m.trace, "before "+m.name)
	if i.Scope == nil || i.Delivery.Reactor != "hooks" {
		return errors.New("bad middleware context")
	}
	return m.beforeErr
}
func (m *hook) After(context.Context, reactors.Invocation) error {
	*m.trace = append(*m.trace, "after "+m.name)
	return m.afterErr
}
func TestReactorMiddlewareOrderAndFailurePolicies(t *testing.T) {
	for _, beforeFails := range []bool{false, true} {
		t.Run(fmt.Sprint(beforeFails), func(t *testing.T) {
			registry := reactorRegistry(t)
			var trace []string
			var beforeErr error
			if beforeFails {
				beforeErr = errors.New("before")
			}
			err := chronicle.RegisterReactorHandler(registry, "hooks", func(context.Context, ReactorInput) error { trace = append(trace, "handler"); return nil }, reactors.WithMiddleware(func() *hook { return &hook{"a", &trace, beforeErr, nil} }), reactors.WithMiddleware(func() *hook { return &hook{"b", &trace, nil, errors.New("after")} }))
			if err != nil {
				t.Fatal(err)
			}
			kernel := &reactorKernel{}
			_, _, ctx := reactorClient(t, kernel, registry)
			session := receive(t, ctx, kernel.sessions)
			session.batches <- batch(0)
			result := receive(t, ctx, session.results)
			expected := []string{"before a", "before b", "handler", "after a", "after b"}
			state := contracts.ObservationState_Success
			if beforeFails {
				expected = []string{"before a", "before b", "after a", "after b"}
				state = contracts.ObservationState_Failed
			}
			if !slices.Equal(trace, expected) || result.State != state {
				t.Fatal(trace, result)
			}
		})
	}
}
func TestReactorReconnectUnregisterAndWorkerJoining(t *testing.T) {
	for _, streamError := range []error{nil, status.Error(codes.InvalidArgument, "bad definition")} {
		t.Run(fmt.Sprint(streamError), func(t *testing.T) {
			testReactorResubscription(t, streamError)
		})
	}
}

func testReactorResubscription(t *testing.T, streamError error) {
	registry := reactorRegistry(t)
	var handled atomic.Int32
	if err := chronicle.RegisterReactorHandler(registry, "callback", func(context.Context, ReactorInput) error { handled.Add(1); return nil }); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	kernel := &reactorKernel{}
	kernel.append = func(ctx context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		close(entered)
		select {
		case <-release:
			return success(request, 0), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	retrying, resume, wait := reactorRetryClock(t)
	client, store, ctx := reactorClient(t, kernel, registry, wait)
	first := receive(t, ctx, kernel.sessions)
	first.batches <- batch(0)
	receive(t, ctx, first.results)
	appendDone := make(chan error, 1)
	go func() {
		result, err := store.EventLog().Append(ctx, "other", ReactorOutput{42})
		appendDone <- errors.Join(err, result.Err())
	}()
	receive(t, ctx, entered)
	first.end <- streamError
	receive(t, ctx, first.done)
	receive(t, ctx, retrying)
	resume <- struct{}{}
	second := receive(t, ctx, kernel.sessions)
	if first.registration.ConnectionId != second.registration.ConnectionId || kernel.registrations.Load() != 1 || kernel.active.Load() != 1 {
		t.Fatal("observer resubscription replaced the shared generation")
	}
	select {
	case err := <-appendDone:
		t.Fatalf("unrelated append interrupted: %v", err)
	default:
	}
	second.batches <- batch(1)
	receive(t, ctx, second.results)
	if err := store.UnregisterReactor(ctx, "callback"); err != nil {
		t.Fatal(err)
	}
	receive(t, ctx, second.done)
	// Release the append normally before shutdown; it must not have an unknown outcome.
	release <- struct{}{}
	if err := receive(t, ctx, appendDone); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if handled.Load() != 2 {
		t.Fatal(handled.Load())
	}
}
func TestReactorCancellationJoinsActiveHandlerAndClosesScope(t *testing.T) {
	registry := reactorRegistry(t)
	entered := make(chan struct{})
	left := make(chan struct{})
	scopes := &countingScopes{}
	if err := chronicle.RegisterReactorHandler(registry, "cancel", func(ctx context.Context, _ ReactorInput) error {
		close(entered)
		<-ctx.Done()
		close(left)
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	client, _, ctx := reactorClient(t, kernel, registry, chronicle.WithServices(scopes))
	session := receive(t, ctx, kernel.sessions)
	session.batches <- batch(0)
	receive(t, ctx, entered)
	if err := client.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	receive(t, ctx, left)
	receive(t, ctx, session.done)
	if scopes.closed.Load() != 1 {
		t.Fatal("scope leaked")
	}
	select {
	case result := <-session.results:
		t.Fatalf("acknowledged canceled delivery: %v", result)
	default:
	}
}

type ReactorModel struct {
	ID   string   `json:"id"`
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}
type ModelReactor struct{ names chan string }

func (r *ModelReactor) React(_ ReactorInput, m *ReactorModel) error {
	if m == nil {
		r.names <- "absent"
		return nil
	}
	if m.Tags == nil {
		return errors.New("model collections were not normalized")
	}
	r.names <- m.Name
	return nil
}
func TestReactorReadModelInjectionUsesCustomKey(t *testing.T) {
	for _, tc := range []struct{ name, key, document, want string }{{"source key", "source", "", "Ada"}, {"custom key", "customer", "", "Ada"}, {"absent pointer", "source", "null", "absent"}} {
		t.Run(tc.name, func(t *testing.T) {
			registry := reactorRegistry(t)
			if _, err := chronicle.RegisterReadModel[ReactorModel](registry); err != nil {
				t.Fatal(err)
			}
			names := make(chan string, 1)
			var options []reactors.Option
			if tc.key == "customer" {
				options = append(options, reactors.WithReadModelKey(func(context.Context, any, events.Context) (readmodels.Key, error) { return "customer", nil }))
			}
			if err := chronicle.RegisterReactor[*ModelReactor](registry, func() *ModelReactor { return &ModelReactor{names} }, options...); err != nil {
				t.Fatal(err)
			}
			kernel := &reactorKernel{modelJSON: tc.document}
			_, _, ctx := reactorClient(t, kernel, registry)
			session := receive(t, ctx, kernel.sessions)
			session.batches <- batch(0)
			result := receive(t, ctx, session.results)
			if result.State != contracts.ObservationState_Success || receive(t, ctx, kernel.readKey) != tc.key || receive(t, ctx, names) != tc.want {
				t.Fatal(result)
			}
		})
	}
}
