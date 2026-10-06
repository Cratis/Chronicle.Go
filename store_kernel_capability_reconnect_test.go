// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	readmodelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// kernelSwitchConn reports a switchable kernel version on every compatibility
// check, simulating a reconnect to another wire-compatible kernel, and counts
// every RPC method actually sent.
type kernelSwitchConn struct {
	grpc.ClientConnInterface
	version atomic.Value
	mu      sync.Mutex
	calls   map[string]int
}

func newKernelSwitchConn(inner grpc.ClientConnInterface, version string) *kernelSwitchConn {
	c := &kernelSwitchConn{ClientConnInterface: inner, calls: map[string]int{}}
	c.version.Store(version)
	return c
}

func (c *kernelSwitchConn) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	c.mu.Lock()
	c.calls[method[strings.LastIndex(method, "/")+1:]]++
	c.mu.Unlock()
	if err := c.ClientConnInterface.Invoke(ctx, method, args, reply, options...); err != nil {
		return err
	}
	if response, ok := reply.(*clients.CompatibilityResponse); ok {
		response.ServerVersion, response.ServerProtocolVersion = c.version.Load().(string), "19.32.3"
	}
	return nil
}

func (c *kernelSwitchConn) count(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[method]
}

// reconnectFixture is a supervised client whose kernel version can change on
// reconnect. connected receives every successfully established generation.
type reconnectFixture struct {
	kernel    *supervisedKernel
	client    *Client
	conn      *kernelSwitchConn
	connected chan ConnectionEvent
	ctx       context.Context
}

func newReconnectFixture(t *testing.T, kernel *supervisedKernel, version string, options ...ClientOption) *reconnectFixture {
	t.Helper()
	f := &reconnectFixture{kernel: kernel, connected: make(chan ConnectionEvent, 8)}
	options = append(options, WithOnConnected(func(_ context.Context, e ConnectionEvent) { f.connected <- e }))
	f.client, f.ctx = supervisionClient(t, kernel, options...)
	f.conn = newKernelSwitchConn(f.client.config.borrowed, version)
	f.client.config.borrowed = f.conn
	return f
}

func (f *reconnectFixture) awaitConnected(t *testing.T) ConnectionEvent {
	t.Helper()
	select {
	case e := <-f.connected:
		return e
	case <-f.ctx.Done():
		t.Fatal("no connection", f.ctx.Err())
		return ConnectionEvent{}
	}
}

// reconnectTo ends the current session and returns once the next generation,
// reporting version, is established.
func (f *reconnectFixture) reconnectTo(t *testing.T, version string, previous uint64) {
	t.Helper()
	f.conn.version.Store(version)
	f.kernel.endStream <- status.Error(codes.Unavailable, "restart on another kernel")
	for {
		if e := f.awaitConnected(t); e.Err == nil && e.Generation > previous {
			return
		}
	}
}

type capabilityRevised struct {
	Name string `chronicle:"pii"`
}

// A protected Revise admitted against 19.32.2 that reconnects to 19.32.1 while
// an enricher runs must not dispatch: 19.32.1 stores revisions unprotected.
func TestProtectedReviseRefusesWhenReconnectedToIncapableKernelBeforeDispatch(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	enricher := func(context.Context, events.TypeRef, *events.EventContent) error {
		if blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}
	registry := NewRegistry()
	if _, err := RegisterEvent[capabilityRevised](registry); err != nil {
		t.Fatal(err)
	}
	f := newReconnectFixture(t, &supervisedKernel{}, "19.32.2", WithRegistry(registry), WithEventEnrichers(enricher))
	store, err := f.client.EventStore(f.ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	first := f.awaitConnected(t)
	done := make(chan error, 1)
	go func() { done <- store.EventLog().Revise(f.ctx, 0, capabilityRevised{Name: "synthetic-private"}) }()
	awaitSignal(t, f.ctx, entered) // Admitted on 19.32.2.
	f.reconnectTo(t, "19.32.1", first.Generation)
	close(release)
	var err2 error
	select {
	case err2 = <-done:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	var unknown *eventsequences.MutationOutcomeUnknownError
	if !errors.Is(err2, ErrUnsupported) || errors.As(err2, &unknown) || strings.Contains(err2.Error(), "synthetic-private") {
		t.Fatalf("revise after reconnect = %v", err2)
	}
	if calls := f.conn.count("Revise"); calls != 0 {
		t.Fatalf("Revise RPCs = %d", calls)
	}
}

// Reverse: an incapable kernel refuses at the pre-check, before providers run,
// even though a later reconnect could reach a capable kernel.
func TestProtectedReviseOnIncapableKernelRefusesBeforeProviders(t *testing.T) {
	var enriched atomic.Int32
	registry := NewRegistry()
	if _, err := RegisterEvent[capabilityRevised](registry); err != nil {
		t.Fatal(err)
	}
	f := newReconnectFixture(t, &supervisedKernel{}, "19.32.1", WithRegistry(registry), WithEventEnrichers(func(context.Context, events.TypeRef, *events.EventContent) error {
		enriched.Add(1)
		return nil
	}))
	store, err := f.client.EventStore(f.ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	first := f.awaitConnected(t)
	if err := store.EventLog().Revise(f.ctx, 0, capabilityRevised{Name: "synthetic-private"}); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	f.reconnectTo(t, "19.32.2", first.Generation)
	if _, err := store.WaitForRegistration(f.ctx); err != nil {
		t.Fatal(err)
	}
	// After reconnecting to a capable kernel the revision is admitted and sent.
	_ = store.EventLog().Revise(f.ctx, 0, capabilityRevised{Name: "synthetic-private"})
	if f.conn.count("Revise") != 1 || enriched.Load() != 1 {
		t.Fatalf("Revise RPCs = %d, enrichments = %d", f.conn.count("Revise"), enriched.Load())
	}
}

type capabilityClassifiedModel struct {
	ID   string
	Name string `chronicle:"pii"`
}

// blockingValidator runs inner, then signals and waits, so the read is admitted
// on one generation and dispatched after a reconnect. The inner validator must
// admit the read: a refusal before the reconnect is not the behavior under test.
func blockingValidator(t *testing.T, inner readmodels.ProjectionReplayValidator, entered, release chan struct{}) readmodels.ProjectionReplayValidator {
	return func(ctx context.Context, d readmodels.Descriptor) (bool, error) {
		known, err := inner(ctx, d)
		if err != nil {
			t.Errorf("replay refused at admission, before the reconnect: %v", err)
		}
		close(entered)
		<-release
		return known, err
	}
}

func readModelServers() (*readModelKernel, *projectionKernel) {
	return &readModelKernel{register: func(context.Context, *readmodelcontracts.RegisterManyRequest) error { return nil }},
		&projectionKernel{register: func(context.Context, *projectioncontracts.RegisterRequest) error { return nil }}
}

func runBlockedReplay(t *testing.T, f *reconnectFixture, store *EventStore, model readmodels.Identifier, catalog *readmodels.Catalog, validator readmodels.ProjectionReplayValidator, previous uint64) error {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	service, err := readmodels.New(store.name, store.namespace, catalog, &clientTransport{client: f.client, store: store}, readmodels.WithProjectionReplayValidator(blockingValidator(t, validator, entered, release)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := service.GetAll(f.ctx, model, new(events.Count(1)))
		done <- err
	}()
	awaitSignal(t, f.ctx, entered)
	f.reconnectTo(t, "19.32.0", previous)
	close(release)
	select {
	case err := <-done:
		return err
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
		return nil
	}
}

// assertDispatchRefusal requires the dispatch-time capability refusal, not an
// admission pre-check refusal with the same message. The read error hides its
// cause's text, so the message is checked on the unwrapped BeforeDispatch.
func assertDispatchRefusal(t *testing.T, err error, message string) {
	t.Helper()
	var before *faults.BeforeDispatch
	if !errors.As(err, &before) || !errors.Is(before, ErrUnsupported) || !strings.Contains(before.Error(), message) {
		t.Fatalf("replay after reconnect = %v; want dispatch-time refusal %q", err, message)
	}
}

// A classified projection replay admitted against 19.32.2 that reconnects to
// an older kernel while admission completes must not dispatch.
func TestClassifiedReplayRefusesWhenReconnectedToIncapableKernelBeforeDispatch(t *testing.T) {
	registry := NewRegistry()
	model, err := RegisterReadModel[capabilityClassifiedModel](registry, readmodels.WithObserver(readmodels.Projection, "classified"))
	if err != nil {
		t.Fatal(err)
	}
	models, projections := readModelServers()
	f := newReconnectFixture(t, &supervisedKernel{readModels: models, projections: projections}, "19.32.2", WithRegistry(registry))
	store, err := f.client.EventStore(f.ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	first := f.awaitConnected(t)
	err = runBlockedReplay(t, f, store, model.Identifier(), store.ReadModels().Catalog(), func(context.Context, readmodels.Descriptor) (bool, error) { return true, nil }, first.Generation)
	assertDispatchRefusal(t, err, "protected projection replay release requires Chronicle 19.32.2")
	if calls := f.conn.count("GetAllInstances"); calls != 0 {
		t.Fatalf("GetAllInstances RPCs = %d", calls)
	}
}

// A mixed all-event replay admitted against 19.32.1 that reconnects to 19.32.0
// must not dispatch: 19.32.0 drops events handled only through ALL.
func TestMixedAllReplayRefusesWhenReconnectedToIncapableKernelBeforeDispatch(t *testing.T) {
	registry := NewRegistry()
	if _, err := RegisterEvent[historyAdmissionEvent](registry); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[historyMixedAllModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	models, projections := readModelServers()
	f := newReconnectFixture(t, &supervisedKernel{readModels: models, projections: projections}, "19.32.1", WithRegistry(registry))
	store, err := f.client.EventStore(f.ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	first := f.awaitConnected(t)
	snapshot, err := f.client.selectedStoreSnapshot("store")
	if err != nil {
		t.Fatal(err)
	}
	transport := &clientTransport{client: f.client, store: store}
	validator, err := projectionReplayValidatorFor(snapshot, transport)
	if err != nil {
		t.Fatal(err)
	}
	err = runBlockedReplay(t, f, store, model.Identifier(), snapshot.models, validator, first.Generation)
	assertDispatchRefusal(t, err, "mixed all-event projection replay requires Chronicle 19.32.1")
	if calls := f.conn.count("GetAllInstances"); calls != 0 {
		t.Fatalf("GetAllInstances RPCs = %d", calls)
	}
}

// Nested map/array-element protection registers on the generation it is
// admitted on. While the 19.32.2 registration RPC is in flight the session moves
// to 19.32.1; that generation re-registers and refuses without an RPC.
func TestNestedProtectionReRegistrationRefusesAfterReconnectToIncapableKernel(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	kernel := &supervisedKernel{registerEventTypes: func(context.Context, *eventtypes.RegisterEventTypesRequest) error {
		if blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}}
	registry := NewRegistry()
	if _, err := RegisterEvent[nestedProtectionEvent](registry); err != nil {
		t.Fatal(err)
	}
	f := newReconnectFixture(t, kernel, "19.32.2", WithRegistry(registry))
	stores := make(chan error, 1)
	go func() {
		_, err := f.client.EventStore(f.ctx, "store")
		stores <- err
	}()
	first := f.awaitConnected(t)
	awaitSignal(t, f.ctx, entered) // Admitted and dispatched on 19.32.2.
	f.reconnectTo(t, "19.32.1", first.Generation)
	close(release)
	var err error
	select {
	case err = <-stores:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "requires Chronicle 19.32.2") {
		t.Fatalf("registration after reconnect = %v", err)
	}
	if calls := f.conn.count("RegisterEventTypes"); calls != 1 {
		t.Fatalf("RegisterEventTypes RPCs = %d, want only the 19.32.2 attempt", calls)
	}
}

// Reverse: refused on 19.32.1, registered after reconnecting to 19.32.2.
func TestNestedProtectionRegistersAfterReconnectToCapableKernel(t *testing.T) {
	registry := NewRegistry()
	if _, err := RegisterEvent[nestedProtectionEvent](registry); err != nil {
		t.Fatal(err)
	}
	f := newReconnectFixture(t, &supervisedKernel{}, "19.32.1", WithRegistry(registry))
	if _, err := f.client.EventStore(f.ctx, "store"); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	first := f.awaitConnected(t)
	if f.conn.count("RegisterEventTypes") != 0 {
		t.Fatal("refused registration reached the kernel")
	}
	f.reconnectTo(t, "19.32.2", first.Generation)
	store, err := f.client.EventStore(f.ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WaitForRegistration(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.conn.count("RegisterEventTypes") != 1 {
		t.Fatalf("RegisterEventTypes RPCs = %d", f.conn.count("RegisterEventTypes"))
	}
}
