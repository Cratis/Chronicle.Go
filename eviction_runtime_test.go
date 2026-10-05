// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
)

func TestEvictionReturnsDuringHandlerAndClientStillJoins(t *testing.T) {
	for _, unregister := range []bool{false, true} {
		t.Run(map[bool]string{false: "client_close", true: "unregister_shared"}[unregister], func(t *testing.T) {
			registry := reactorRegistry(t)
			entered, release := make(chan context.Context, 2), make(chan struct{})
			var released sync.Once
			var handled atomic.Int32
			if err := chronicle.RegisterReactorHandler(registry, "eviction", func(ctx context.Context, _ ReactorInput) error {
				handled.Add(1)
				entered <- ctx
				<-release // Deliberately ignore cancellation: only the owner joins.
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			kernel := &reactorKernel{}
			client, old, ctx := reactorClient(t, kernel, registry)
			t.Cleanup(func() { released.Do(func() { close(release) }) })
			session := receive(t, ctx, kernel.sessions)
			session.batches <- batch(1)
			handlerCtx := receive(t, ctx, entered)
			evictor, ok := any(client).(interface{ EvictEventStores() error })
			if !ok {
				t.Fatal("Client.EvictEventStores is missing")
			}
			done := make(chan error, 1)
			go func() { done <- evictor.EvictEventStores() }()
			if err := receive(t, ctx, done); err != nil {
				t.Fatal(err)
			}
			if handlerCtx.Err() != nil {
				t.Fatal("eviction canceled live handler")
			}
			fresh, err := client.EventStore(ctx, old.Name())
			if err != nil || fresh == old {
				t.Fatal("reacquisition", err)
			}
			if unregister {
				go func() { done <- fresh.UnregisterReactor(ctx, "eviction") }()
			} else {
				go func() { done <- client.Close() }()
			}
			select {
			case <-handlerCtx.Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-done:
				t.Fatal("owner returned without joining handler")
			default:
			}
			released.Do(func() { close(release) })
			if err := receive(t, ctx, done); err != nil {
				t.Fatal(err)
			}
			if unregister {
				if _, err := old.WaitForRegistration(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := fresh.WaitForRegistration(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if handled.Load() != 1 {
				t.Fatal("duplicate delivery")
			}
			select {
			case <-kernel.sessions:
				t.Fatal("reacquisition created a duplicate observer stream")
			default:
			}
		})
	}
}
