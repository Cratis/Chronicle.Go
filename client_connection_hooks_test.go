// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type hookNotification struct {
	event ConnectionEvent
	ctx   context.Context
}

func awaitHook(t *testing.T, ctx context.Context, events <-chan hookNotification) hookNotification {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return hookNotification{}
	}
}

func TestConnectionHooksPairReconnectAndClose(t *testing.T) {
	notifications := make(chan hookNotification, 8)
	hook := func(ctx context.Context, event ConnectionEvent) { notifications <- hookNotification{event, ctx} }
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel, WithKeepAliveTimeout(time.Minute), WithOnConnected(hook), WithOnDisconnected(hook))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := awaitHook(t, ctx, notifications)
	if first.event.Err != nil || first.event.Generation != before.Generation || first.event.ConnectionID == "" || first.event.Address != "" {
		t.Fatalf("connected=%+v", first.event)
	}
	kernel.endStream <- status.Error(codes.Unavailable, "restart")
	dropped := awaitHook(t, ctx, notifications)
	second := awaitHook(t, ctx, notifications)
	if status.Code(dropped.event.Err) != codes.Unavailable || dropped.event.Generation != first.event.Generation || dropped.event.ConnectionID != first.event.ConnectionID {
		t.Fatalf("disconnected=%+v", dropped.event)
	}
	awaitSignal(t, ctx, first.ctx.Done())
	if second.event.Err != nil || second.event.Generation <= first.event.Generation || second.event.ConnectionID == "" || second.event.ConnectionID == first.event.ConnectionID {
		t.Fatalf("reconnected=%+v", second.event)
	}
	after, err := store.WaitForRegistration(ctx)
	if err != nil || after.Generation != second.event.Generation {
		t.Fatalf("registration=%+v %v", after, err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	last := awaitHook(t, ctx, notifications)
	if !errors.Is(last.event.Err, ErrClosed) || last.event.Generation != second.event.Generation || last.ctx.Err() == nil {
		t.Fatalf("close=%+v", last.event)
	}
	select {
	case extra := <-notifications:
		t.Fatalf("unpaired extra=%+v", extra.event)
	default:
	}
}

func TestConnectionHooksCanCallClientWhileRegistrationPending(t *testing.T) {
	pending, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{register: func(ctx context.Context) error {
		close(pending)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	entered, result := make(chan struct{}), make(chan error, 1)
	var client *Client
	hook := func(ctx context.Context, _ ConnectionEvent) {
		close(entered)
		store, err := client.EventStore(ctx, "store")
		if err == nil {
			_, err = store.WaitForRegistration(ctx)
		}
		if err == nil {
			err = client.Ready(ctx)
		}
		result <- err
	}
	client, ctx := supervisionClient(t, kernel, WithKeepAliveTimeout(time.Minute), WithOnConnected(hook))
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, pending)
	awaitSignal(t, ctx, entered) // Hook started before registration completed.
	select {
	case err := <-result:
		t.Fatalf("registration did not block: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestConnectionHooksNilRejectedAndCaptureFrozen(t *testing.T) {
	for _, option := range []ClientOption{WithOnConnected(nil), WithOnDisconnected(nil)} {
		if _, err := NewClient(option); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatal(err)
		}
	}
	first, second := func(context.Context, ConnectionEvent) {}, func(context.Context, ConnectionEvent) {}
	var source *clientConfig
	p, err := CaptureClient(func(c *clientConfig) { source = c }, WithOnConnected(first), WithOnConnected(second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Client().Close(); err != nil {
			t.Error(err)
		}
	}()
	source.connectedHooks[0] = nil
	if len(p.Client().hooks.connected) != 2 || p.Client().hooks.connected[0] == nil {
		t.Fatal("hook capture aliases options")
	}
}
