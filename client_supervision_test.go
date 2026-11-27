// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	subscriptioncontracts "github.com/cratis/chronicle.go/contracts/observation/eventstoresubscriptions"
	reactorcontracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	reducercontracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	readmodelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	seedcontracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type supervisedKernel struct {
	lifecycleKernel
	eventstores.UnimplementedEventStoresServer
	eventtypes.UnimplementedEventTypesServer
	namespaces.UnimplementedNamespacesServer
	constraintcontracts.UnimplementedConstraintsServer
	registerConstraints func(context.Context, *constraintcontracts.RegisterConstraintsRequest) error
	registrations       atomic.Int32
	registered          chan struct{}
	register            func(context.Context) error
	registerEventTypes  func(context.Context, *eventtypes.RegisterEventTypesRequest) error
	appendCall          func(context.Context) error
	mu                  sync.Mutex
	namespaceCounts     map[string]int
	readModels          readmodelcontracts.ReadModelsServer
	projections         projectioncontracts.ProjectionsServer
	reactors            reactorcontracts.ReactorsServer
	reducers            reducercontracts.ReducersServer
	seeding             seedcontracts.EventSeedingServer
	subscriptions       subscriptioncontracts.EventStoreSubscriptionsServer
	streamInterceptor   grpc.StreamClientInterceptor
}

func (*supervisedKernel) EnsureEventStore(context.Context, *eventstores.EnsureEventStoreRequest) (*eventstores.CommandResult, error) {
	return &eventstores.CommandResult{IsAuthorized: true}, nil
}
func (k *supervisedKernel) EnsureNamespace(_ context.Context, r *namespaces.EnsureNamespaceRequest) (*namespaces.CommandResult, error) {
	k.mu.Lock()
	k.namespaceCounts[r.Namespace]++
	k.mu.Unlock()
	return &namespaces.CommandResult{IsAuthorized: true}, nil
}
func (k *supervisedKernel) RegisterEventTypes(ctx context.Context, request *eventtypes.RegisterEventTypesRequest) (*eventtypes.CommandResult, error) {
	k.registrations.Add(1)
	if k.registerEventTypes != nil {
		if err := k.registerEventTypes(ctx, request); err != nil {
			return nil, err
		}
	}
	if k.register != nil {
		if err := k.register(ctx); err != nil {
			return nil, err
		}
	}
	k.registered <- struct{}{}
	return &eventtypes.CommandResult{IsAuthorized: true}, nil
}
func (k *supervisedKernel) Register(ctx context.Context, request *constraintcontracts.RegisterConstraintsRequest) (*emptypb.Empty, error) {
	if k.registerConstraints != nil {
		if err := k.registerConstraints(ctx, request); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}
func (k *supervisedKernel) Append(ctx context.Context, r *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
	k.appends.Add(1)
	if k.appendCall != nil {
		if err := k.appendCall(ctx); err != nil {
			return nil, err
		}
	}
	return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, CorrelationId: r.CorrelationId}}, nil
}

func supervisionClient(t *testing.T, k *supervisedKernel, options ...ClientOption) (*Client, context.Context) {
	t.Helper()
	k.endStream = make(chan error, 1)
	k.registered = make(chan struct{}, 20)
	k.namespaceCounts = make(map[string]int)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	clients.RegisterConnectionServiceServer(server, k)
	eventstores.RegisterEventStoresServer(server, k)
	eventtypes.RegisterEventTypesServer(server, k)
	constraintcontracts.RegisterConstraintsServer(server, k)
	namespaces.RegisterNamespacesServer(server, k)
	sequences.RegisterEventSequencesServer(server, k)
	if k.readModels != nil {
		readmodelcontracts.RegisterReadModelsServer(server, k.readModels)
		if explorer, ok := k.readModels.(readmodelexplorer.ReadModelExplorerServer); ok {
			readmodelexplorer.RegisterReadModelExplorerServer(server, explorer)
		}
		if materialized, ok := k.readModels.(readmodelcontracts.MaterializedReadModelsServer); ok {
			readmodelcontracts.RegisterMaterializedReadModelsServer(server, materialized)
		}
	}
	if k.projections != nil {
		projectioncontracts.RegisterProjectionsServer(server, k.projections)
	}
	if k.reactors != nil {
		reactorcontracts.RegisterReactorsServer(server, k.reactors)
	}
	if k.reducers != nil {
		reducercontracts.RegisterReducersServer(server, k.reducers)
	}
	if k.subscriptions != nil {
		subscriptioncontracts.RegisterEventStoreSubscriptionsServer(server, k.subscriptions)
	}
	if k.seeding != nil {
		seedcontracts.RegisterEventSeedingServer(server, k.seeding)
	}
	served := make(chan struct{})
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-served
	})
	go serveSupervisionFixture(server, listener, served, t.Error)
	conn, err := grpc.NewClient("passthrough:///supervision", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithStreamInterceptor(k.streamInterceptor))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	registry := NewRegistry()
	if _, err = RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	base := []ClientOption{WithGRPCConnection(conn), WithConnectionString("chronicle://test"), WithRegistry(registry), WithTokenSource(&invalidatingSource{})}
	client, err := NewClient(append(base, options...)...)
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
	return client, ctx
}

func serveSupervisionFixture(server *grpc.Server, listener net.Listener, served chan struct{}, report func(...any)) {
	defer close(served)
	// Cleanup can stop the server before this goroutine starts serving.
	if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
		report(err)
	}
}

func awaitSignal(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestAutomaticReplaySharesStoreDefinitionsAndSeparatesNamespaces(t *testing.T) {
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel)
	first, err := client.EventStore(ctx, "store", WithNamespace("one"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.EventStore(ctx, "store", WithNamespace("two"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := first.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, kernel.registered)
	if kernel.registrations.Load() != 1 {
		t.Fatal("definitions registered once per namespace")
	}
	client.mu.Lock()
	old := client.current
	client.mu.Unlock()
	kernel.endStream <- status.Error(codes.Unavailable, "restart")
	awaitSignal(t, ctx, old.ctx.Done())
	// No Connect/Ready call drives this replay: the supervisor owns it.
	awaitSignal(t, ctx, kernel.registered)
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := second.WaitForRegistration(ctx)
	if err != nil || !after.IsSuccess() || after.Generation <= before.Generation || kernel.registrations.Load() != 2 {
		t.Fatalf("replay: %+v %v", after, err)
	}
	kernel.mu.Lock()
	defer kernel.mu.Unlock()
	if kernel.namespaceCounts["one"] != 2 || kernel.namespaceCounts["two"] != 2 {
		t.Fatal(kernel.namespaceCounts)
	}
}

func TestCachedHandleRejectsOutageBeforeDispatch(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{streamInterceptor: gateReplacementConnect(entered, release)}
	client, ctx := supervisionClient(t, kernel, WithConnectTimeout(30*time.Second))
	releaseConnect := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseConnect)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	log := store.EventLog()
	if got := kernel.registrations.Load(); got != 1 {
		t.Fatalf("initial registration count = %d, want 1", got)
	}
	kernel.endStream <- status.Error(codes.Unavailable, "lost before dispatch")
	awaitSignal(t, ctx, entered)
	client.mu.Lock()
	current, cause := client.current, client.connectionError
	client.mu.Unlock()
	if current != nil || status.Code(cause) != codes.Unavailable {
		t.Fatalf("replacement handshake entered with current installed = %t, connection error = %v", current != nil, cause)
	}

	scope := eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})
	appendCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err = log.Append(appendCtx, "source", lifecycleEvent{}, scope)
	callerErr, appends := appendCtx.Err(), kernel.appends.Load()
	t.Logf("outage append: error = %v, caller error = %v, RPCs = %d", err, callerErr, appends)
	if !errors.Is(err, cause) {
		t.Errorf("append error = %v, want recorded connection error %v", err, cause)
	}
	var unknown *eventsequences.OutcomeUnknownError
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
		t.Errorf("append should fail before dispatch without waiting for the caller deadline, got %v", err)
	}
	if callerErr != nil {
		t.Errorf("append returned with expired or canceled caller context: %v", callerErr)
	}
	if appends != 0 {
		t.Errorf("append RPCs during outage = %d, want 0", appends)
	}

	releaseConnect()
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if got := kernel.registrations.Load(); got != 2 {
		t.Fatalf("registration count after Ready = %d, want 2", got)
	}
	result, err := log.Append(ctx, "source", lifecycleEvent{}, scope)
	if err != nil || result.Disposition != eventsequences.Committed {
		t.Fatalf("new append after Ready: disposition = %v, error = %v", result.Disposition, err)
	}
	if got := kernel.appends.Load(); got != 1 {
		t.Fatalf("total append RPCs = %d, want 1", got)
	}
}

func gateReplacementConnect(entered chan<- struct{}, release <-chan struct{}) grpc.StreamClientInterceptor {
	var connects atomic.Int32
	return func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method == clients.ConnectionService_Connect_FullMethodName {
			attempt := connects.Add(1)
			if attempt == 2 {
				close(entered)
			}
			if attempt > 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		return streamer(ctx, desc, conn, method, opts...)
	}
}

func TestCachedHandleFailsWhenRegistrationGenerationDies(t *testing.T) {
	registrationStarted := make(chan struct{})
	drainEntered, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{
		registerEventTypes: holdSecondRegistration(registrationStarted),
		streamInterceptor:  gateSecondConnectDrain(drainEntered, release),
	}
	client, ctx := supervisionClient(t, kernel)
	releaseDrain := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseDrain)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	log := store.EventLog()
	reconnectWithoutStoreReplay(t, ctx, client, kernel)

	scope := eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})
	appendCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	result := make(chan error, 1)
	t.Cleanup(func() {
		releaseDrain()
		cancel()
		for range result {
		}
	})
	go func() {
		defer close(result)
		_, err := log.Append(appendCtx, "interrupted", lifecycleEvent{}, scope)
		result <- err
	}()
	awaitSignal(t, appendCtx, registrationStarted)
	kernel.endStream <- status.Error(codes.Unavailable, "lost during registration")
	awaitSignal(t, appendCtx, drainEntered)
	select {
	case err := <-result:
		requireGenerationLossBeforeAppend(t, err, appendCtx.Err(), kernel.appends.Load())
	case <-appendCtx.Done():
		t.Fatalf("append did not fail while the canceled generation was draining: %v", appendCtx.Err())
	}

	releaseDrain()
	requireAppendAfterRegistrationRecovery(t, ctx, kernel, recoveryAppendOptions{
		store: store,
		log:   log,
		scope: scope,
	})
}

func holdSecondRegistration(started chan<- struct{}) func(context.Context, *eventtypes.RegisterEventTypesRequest) error {
	var registrations atomic.Int32
	return func(ctx context.Context, _ *eventtypes.RegisterEventTypesRequest) error {
		if registrations.Add(1) != 2 {
			return nil
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
}

func reconnectWithoutStoreReplay(t *testing.T, ctx context.Context, client *Client, kernel *supervisedKernel) {
	t.Helper()
	evictStores(t, client)
	client.mu.Lock()
	first := client.current
	client.mu.Unlock()
	kernel.endStream <- status.Error(codes.Unavailable, "replace initial generation")
	awaitSignal(t, ctx, first.ctx.Done())
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if got := kernel.registrations.Load(); got != 1 {
		t.Fatalf("registrations after reconnect = %d, want 1", got)
	}
}

type recoveryAppendOptions struct {
	store *EventStore
	log   *eventsequences.Sequence
	scope eventsequences.AppendOption
}

func requireAppendAfterRegistrationRecovery(t *testing.T, ctx context.Context, kernel *supervisedKernel, options recoveryAppendOptions) {
	t.Helper()
	outcome, err := options.store.WaitForRegistration(ctx)
	if err != nil || !outcome.IsSuccess() {
		t.Fatalf("registration after generation loss: outcome = %+v, error = %v", outcome, err)
	}
	if got := kernel.registrations.Load(); got != 3 {
		t.Fatalf("registrations after recovery = %d, want 3", got)
	}
	fresh, err := options.log.Append(ctx, "fresh", lifecycleEvent{}, options.scope)
	if err != nil || fresh.Disposition != eventsequences.Committed {
		t.Fatalf("fresh append: disposition = %v, error = %v", fresh.Disposition, err)
	}
	if got := kernel.appends.Load(); got != 1 {
		t.Fatalf("total append RPCs = %d, want 1", got)
	}
}

func requireGenerationLossBeforeAppend(t *testing.T, err, callerErr error, appends int32) {
	t.Helper()
	if callerErr != nil || appends != 0 {
		t.Fatalf("append returned with caller error = %v, append RPCs = %d; want a live caller and no RPCs", callerErr, appends)
	}
	if err == nil {
		t.Fatal("append returned no error after losing its registration generation")
	}
	var registration *RegistrationError
	var unknown *eventsequences.OutcomeUnknownError
	code := status.Code(err)
	switch {
	case errors.As(err, &registration), errors.As(err, &unknown),
		errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		code == codes.Canceled, code == codes.DeadlineExceeded:
		t.Fatalf("append exposed a registration, cancellation, or unknown-outcome error: %v", err)
	}
}

func gateSecondConnectDrain(entered chan<- struct{}, release <-chan struct{}) grpc.StreamClientInterceptor {
	var connects atomic.Int32
	return func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, conn, method, opts...)
		if err != nil || method != clients.ConnectionService_Connect_FullMethodName {
			return stream, err
		}
		if connects.Add(1) == 2 {
			stream = &connectDrainGate{ClientStream: stream, entered: entered, release: release}
		}
		return stream, nil
	}
}

type connectDrainGate struct {
	grpc.ClientStream
	entered  chan<- struct{}
	release  <-chan struct{}
	terminal bool
}

func (s *connectDrainGate) RecvMsg(message any) error {
	if s.terminal && s.entered != nil {
		// The supervisor cancels the generation before draining, and joins this
		// receive before replacing it. Hold that interval open for the append.
		close(s.entered)
		s.entered = nil
		<-s.release
	}
	err := s.ClientStream.RecvMsg(message)
	s.terminal = err != nil
	return err
}

func TestLostGenerationDoesNotReplayInflightAppend(t *testing.T) {
	entered := make(chan struct{})
	kernel := &supervisedKernel{appendCall: func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }}
	client, ctx := supervisionClient(t, kernel)
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := store.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		result <- err
	}()
	awaitSignal(t, ctx, entered)
	kernel.endStream <- status.Error(codes.Unavailable, "lost")
	var unknown *eventsequences.OutcomeUnknownError
	select {
	case err = <-result:
		if !errors.As(err, &unknown) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if kernel.appends.Load() != 1 {
		t.Fatal("ambiguous append replayed")
	}
}
