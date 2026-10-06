//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/observation"
	reactorcontracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type evictionHeartbeatStream struct {
	grpc.ClientStream
	once      sync.Once
	connected chan<- string
}

func (s *evictionHeartbeatStream) RecvMsg(value any) error {
	err := s.ClientStream.RecvMsg(value)
	if err == nil {
		s.once.Do(func() {
			var id string
			if alive, ok := value.(*clients.ConnectionKeepAlive); ok {
				id = alive.GetConnectionId()
			}
			select {
			case s.connected <- id:
			case <-s.Context().Done():
			}
		})
	}
	return err
}

func TestKernelCacheEvictionRetainsWatchAndExplicitReconnect(t *testing.T) {
	f := newKernelFixture(t)
	f.storeName = chronicle.StoreName("go-evict-" + uuid.NewString())
	uri, err := chronicle.ParseConnectionString(f.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	relay := newRelay(t, uri.Addresses()[0].String())
	connected := make(chan string, 4)
	var streams atomic.Int32
	conn, err := grpc.NewClient(relay.listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true})), grpc.WithDisableRetry(), // Test-owned local TLS endpoint.
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
			stream, err := streamer(ctx, desc, cc, method, options...)
			if err == nil && method == reactorcontracts.Reactors_Observe_FullMethodName {
				streams.Add(1)
			}
			if err == nil && method == clients.ConnectionService_Connect_FullMethodName {
				return &evictionHeartbeatStream{ClientStream: stream, connected: connected}, nil
			}
			return stream, err
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[WatchedPersonNamed](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[WatchedPerson](registry)
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan string, 8)
	if err := chronicle.RegisterReactorHandler(registry, "eviction", func(ctx context.Context, value WatchedPersonNamed) error {
		select {
		case handled <- value.Name:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	client := f.client(registry, chronicle.WithGRPCConnection(conn), chronicle.WithConnectionString("chronicle://"+relay.listener.Addr().String()))
	select {
	case <-connected:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	old, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	before, err := old.WaitForRegistration(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The reactor stream is open, but the kernel subscribes it asynchronously.
	// Chronicle#4558 (dropped live delivery) is fixed in 19.32.3, but without
	// this wait 19.32.3 redelivered "after-eviction" to the reactor after the
	// explicit reconnect below. Keep both waits until that is understood.
	awaitObserversObserving(t, f, old.Namespace(), append([]string{"eviction", string(model.Identifier())}, eventLogStatisticsObservers...)...)
	reader := readmodels.For(old.ReadModels(), model)
	watch, err := reader.Watch(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = watch.Close() }()
	observe := func(store *chronicle.EventStore, subscription *readmodels.Subscription[readmodels.Change[WatchedPerson]], name string) {
		t.Helper()
		appendSuccessfully(t, f.ctx, store, "person", WatchedPersonNamed{Name: name})
		change, err := subscription.Recv()
		if err != nil || change.Value.Name != name || change.Key != "person" {
			t.Fatal("watch delivery", change, err)
		}
		select {
		case got := <-handled:
			if got != name {
				t.Fatal("reactor delivery", got, name)
			}
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
	}
	observe(old, watch, "before")
	if err := client.EvictEventStores(); err != nil {
		t.Fatal(err)
	}
	fresh, err := client.EventStore(f.ctx, f.storeName)
	if err != nil || fresh == old || fresh.Name() != old.Name() || fresh.Namespace() != old.Namespace() {
		t.Fatal("cache facade", err)
	}
	observe(fresh, watch, "after-eviction")
	if streams.Load() != 1 {
		t.Fatal("eviction duplicated observer streams", streams.Load())
	}
	// Detach again before generation loss: do not depend on automatic membership.
	if err := client.EvictEventStores(); err != nil {
		t.Fatal(err)
	}
	relay.cut()
	if _, err := watch.Recv(); !errors.Is(err, readmodels.ErrInterrupted) {
		t.Fatal("old watch did not terminate on generation loss", err)
	}
	var reconnected string
	select {
	case reconnected = <-connected:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	after, err := old.WaitForRegistration(f.ctx)
	if err != nil || !after.IsSuccess() || after.Generation <= before.Generation {
		t.Fatal("explicit retained registration", before, after, err)
	}
	// The new generation resubscribes the reactor asynchronously; wait until it
	// observes before appending (see the first wait above).
	awaitReactorConnection(t, f, old.Namespace(), "eviction", reconnected)
	awaitObserversObserving(t, f, old.Namespace(), append([]string{"eviction", string(model.Identifier())}, eventLogStatisticsObservers...)...)
	next, err := reader.Watch(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = next.Close() }()
	observe(old, next, "after-explicit-reconnect")
	if streams.Load() != 2 {
		t.Fatal("expected one observer stream per generation", streams.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("cache eviction: old watch delivered; facades distinct; reactor streams=2 across generations %d -> %d; explicit retained registration and rewatch delivered", before.Generation, after.Generation)
}

// awaitReactorConnection waits until the kernel's reactor subscription targets
// exactly the given client connection, so a stale previous-generation
// subscription cannot satisfy the following observing-state check.
func awaitReactorConnection(t *testing.T, f *kernelFixture, namespace chronicle.Namespace, id, connectionID string) {
	t.Helper()
	if connectionID == "" {
		t.Fatal("reconnect heartbeat carried no connection id")
	}
	client := observation.NewObserversClient(f.conn)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		connected, err := client.GetConnectedClientsForObserver(f.ctx, &observation.GetConnectedClientsForObserverRequest{EventStore: string(f.storeName), Namespace: string(namespace), ObserverId: id, EventSequenceId: "event-log"})
		if err != nil {
			t.Fatalf("observer %s connected clients: %v", id, err)
		}
		if items := connected.GetItems(); len(items) == 1 && items[0].GetConnectionId() == connectionID {
			return
		}
		select {
		case <-f.ctx.Done():
			t.Fatalf("observer %s not subscribed for connection %s (last %v): %v", id, connectionID, connected, f.ctx.Err())
		case <-ticker.C:
		}
	}
}
