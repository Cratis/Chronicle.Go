// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDerivedReconnectReusesFrozenClassificationsAndCodecs(t *testing.T) {
	registry := NewRegistry()
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	if _, err := RegisterEvent[derivedfixtures.MembersChanged](registry, events.WithCodecs(codecs), events.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		calls.Add(1)
		return compliance.Classification{}, nil
	}))); err != nil {
		t.Fatal(err)
	}
	frozenCalls := calls.Load()
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), WithNamingPolicy(serialization.CamelCase))
	store, err := client.EventStore(ctx, "derived")
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, kernel.registered)
	client.mu.Lock()
	old := client.current
	client.mu.Unlock()
	kernel.endStream <- status.Error(codes.Unavailable, "test-owned reconnect")
	awaitSignal(t, ctx, old.ctx.Done())
	awaitSignal(t, ctx, kernel.registered)
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WaitForRegistration(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != frozenCalls || frozenCalls == 0 {
		t.Fatal("application classification provider replayed")
	}
	result, err := store.EventLog().Append(ctx, "source", derivedfixtures.Sample(), eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil || result.Err() != nil {
		t.Fatalf("reconnected codec: %+v %v", result, err)
	}
}
