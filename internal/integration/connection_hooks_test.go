//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
)

func nextConnectionEvent(t *testing.T, ctx context.Context, events <-chan chronicle.ConnectionEvent) chronicle.ConnectionEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return chronicle.ConnectionEvent{}
	}
}

func TestKernelConnectionHooksPairRealReconnectAndClose(t *testing.T) {
	fixture := newKernelFixture(t)
	uri, err := chronicle.ParseConnectionString(fixture.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	relay := newRelay(t, uri.Addresses()[0].String())
	events := make(chan chronicle.ConnectionEvent, 8)
	hook := func(_ context.Context, event chronicle.ConnectionEvent) { events <- event }
	client := fixture.client(chronicle.NewRegistry(), chronicle.WithConnectionString("chronicle://"+relay.listener.Addr().String()), chronicle.WithKeepAliveTimeout(time.Minute), chronicle.WithOnConnected(hook), chronicle.WithOnDisconnected(hook))
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := nextConnectionEvent(t, fixture.ctx, events)
	if first.Err != nil || first.ConnectionID == "" || first.Generation != before.Generation || first.Address != relay.listener.Addr().String() {
		t.Fatalf("connected=%+v", first)
	}
	relay.cut()
	dropped := nextConnectionEvent(t, fixture.ctx, events)
	second := nextConnectionEvent(t, fixture.ctx, events)
	if dropped.Err == nil || errors.Is(dropped.Err, chronicle.ErrClosed) || dropped.Generation != first.Generation || dropped.ConnectionID != first.ConnectionID {
		t.Fatalf("disconnected=%+v", dropped)
	}
	if second.Err != nil || second.ConnectionID == "" || second.ConnectionID == first.ConnectionID || second.Generation <= first.Generation {
		t.Fatalf("reconnected=%+v", second)
	}
	after, err := store.WaitForRegistration(fixture.ctx)
	if err != nil || after.Generation != second.Generation {
		t.Fatalf("registration=%+v %v", after, err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	last := nextConnectionEvent(t, fixture.ctx, events)
	if !errors.Is(last.Err, chronicle.ErrClosed) || last.Generation != second.Generation || last.ConnectionID != second.ConnectionID {
		t.Fatalf("close=%+v", last)
	}
	select {
	case extra := <-events:
		t.Fatalf("extra notification=%+v", extra)
	default:
	}
}

type secondRelayBalancer struct{}

func (secondRelayBalancer) Next(ctx context.Context, candidates []chronicle.ServerAddress) (chronicle.ServerAddress, error) {
	if err := ctx.Err(); err != nil {
		return chronicle.ServerAddress{}, err
	}
	return candidates[1], nil
}

func TestKernelCustomLoadBalancerSelectsOAuthAndGRPCRelay(t *testing.T) {
	fixture := newKernelFixture(t)
	uri, err := chronicle.ParseConnectionString(fixture.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	first, second := newRelay(t, uri.Addresses()[0].String()), newRelay(t, uri.Addresses()[0].String())
	connected := make(chan chronicle.ConnectionEvent, 1)
	client := fixture.client(chronicle.NewRegistry(), chronicle.WithConnectionString("chronicle://"+first.listener.Addr().String()+","+second.listener.Addr().String()), chronicle.WithLoadBalancer(secondRelayBalancer{}), chronicle.WithOnConnected(func(_ context.Context, e chronicle.ConnectionEvent) { connected <- e }))
	event := nextConnectionEvent(t, fixture.ctx, connected)
	if event.Address != second.listener.Addr().String() || event.ConnectionID == "" {
		t.Fatalf("selected=%+v", event)
	}
	// Separate OAuth HTTP and gRPC transports both tunnel TLS to the kernel.
	if first.acceptCount.Load() != 0 || second.acceptCount.Load() < 2 {
		t.Fatalf("relay connections=%d/%d", first.acceptCount.Load(), second.acceptCount.Load())
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}
