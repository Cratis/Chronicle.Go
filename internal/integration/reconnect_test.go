//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

// A test-owned TCP relay cuts real TLS/HTTP2 connections without operating on
// someone else's kernel. Both OAuth and gRPC still terminate at the real kernel.
type cuttableRelay struct {
	listener    net.Listener
	target      string
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	accepted    chan struct{}
	workers     sync.WaitGroup
}

func newRelay(t *testing.T, target string) *cuttableRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	relay := &cuttableRelay{listener: listener, target: target, ctx: ctx, cancel: cancel, connections: make(map[net.Conn]struct{}), accepted: make(chan struct{})}
	go relay.accept()
	t.Cleanup(func() { cancel(); _ = listener.Close(); <-relay.accepted; relay.cut(); relay.workers.Wait() })
	return relay
}
func (r *cuttableRelay) accept() {
	defer close(r.accepted)
	for {
		incoming, err := r.listener.Accept()
		if err != nil {
			return
		}
		r.mu.Lock()
		r.connections[incoming] = struct{}{}
		r.mu.Unlock()
		r.workers.Go(func() { r.forward(incoming) })
	}
}
func (r *cuttableRelay) forward(incoming net.Conn) {
	defer func() { _ = incoming.Close(); r.mu.Lock(); delete(r.connections, incoming); r.mu.Unlock() }()
	outgoing, err := (&net.Dialer{}).DialContext(r.ctx, "tcp", r.target)
	if err != nil {
		return
	}
	defer func() { _ = outgoing.Close() }()
	copied := make(chan struct{})
	go func() { defer close(copied); _, _ = io.Copy(outgoing, incoming); _ = outgoing.Close() }()
	_, _ = io.Copy(incoming, outgoing)
	_ = incoming.Close()
	<-copied
}
func (r *cuttableRelay) cut() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for conn := range r.connections {
		_ = conn.Close()
	}
}

func TestKernelReconnectReplaysRegistrationAndPreservesHandles(t *testing.T) {
	fixture := newKernelFixture(t)
	uri, err := chronicle.ParseConnectionString(fixture.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	relay := newRelay(t, uri.Addresses()[0].String())
	registry := integrationRegistry[CustomerRegistered](t, events.WithID("go-reconnect-customer"))
	client := fixture.client(registry, chronicle.WithConnectionString("chronicle://"+relay.listener.Addr().String()))
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	appendEvent := func(name string) {
		t.Helper()
		result, err := store.EventLog().Append(fixture.ctx, "customer", CustomerRegistered{Name: name}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		if err != nil || result.Err() != nil {
			t.Fatalf("append: %v %v", err, result.Err())
		}
	}
	appendEvent("before")
	relay.cut()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var after chronicle.RegistrationOutcome
	for {
		select {
		case <-fixture.ctx.Done():
			t.Fatal("reconnect failed", fixture.ctx.Err())
		case <-ticker.C:
		}
		after, err = store.WaitForRegistration(fixture.ctx)
		if err == nil && after.IsSuccess() && after.Generation > before.Generation {
			break
		}
	}
	cached, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil || cached != store {
		t.Fatalf("handle identity changed: %v", err)
	}
	appendEvent("after")
	if actual := fixture.read("customer"); len(actual) != 2 {
		t.Fatalf("persisted events=%d", len(actual))
	}
	if err = client.Shutdown(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("real kernel recovered generation %d -> %d, replayed registration and retained store handle", before.Generation, after.Generation)
}
