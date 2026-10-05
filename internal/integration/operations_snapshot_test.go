//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
)

func TestKernelOperationsReplayabilitySubscriptionAndCustomSequenceRemoval(t *testing.T) {
	f := newKernelFixture(t)
	registry := integrationRegistry[OperationsEvent](t)
	const sequence events.SequenceID = "operations-custom"
	for _, onceOnly := range []bool{false, true} {
		id := reactors.ID("operations-replayable")
		options := []reactors.Option{reactors.WithEventSequence(sequence)}
		if onceOnly {
			id = "operations-once-only"
			options = append(options, reactors.OnceOnly())
		}
		if err := chronicle.RegisterReactorHandler(registry, id, func(context.Context, OperationsEvent) error { return nil }, options...); err != nil {
			t.Fatal(err)
		}
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	observers := store.Observers()
	for _, id := range []observation.ID{"operations-replayable", "operations-once-only"} {
		waitOperations(t, f.ctx, func() bool {
			info, err := observers.Get(f.ctx, id, sequence)
			if err != nil {
				t.Fatal(err)
			}
			return info != nil && info.SubscriptionKnown() && info.IsSubscribed()
		})
		info, err := observers.Get(f.ctx, id, sequence)
		if err != nil || info == nil || info.IsReplayable() != (id == "operations-replayable") {
			t.Fatalf("Get replay policy for %s: %v, %v", id, info, err)
		}
	}
	all, err := observers.List(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[observation.ID]bool)
	for _, info := range all {
		// Kernel-owned observers can share this store. Assert our two identities,
		// never assume List contains only declarations made by this client.
		if info.ID() != "operations-replayable" && info.ID() != "operations-once-only" {
			continue
		}
		if seen[info.ID()] {
			t.Fatal("duplicate observer definition", info.ID())
		}
		seen[info.ID()] = true
		if info.Sequence() != sequence || info.IsReplayable() != (info.ID() == "operations-replayable") || info.SubscriptionKnown() || info.IsSubscribed() {
			t.Fatalf("List %s: sequence=%s replayable=%t subscriptionKnown=%t", info.ID(), info.Sequence(), info.IsReplayable(), info.SubscriptionKnown())
		}
		// A supplied event-log target is unsafe for this definition. The SDK must
		// reject it locally, never ask the kernel to inspect the wrong activation.
		if _, err := observers.RemoveFrom(f.ctx, info.ID(), events.EventLog); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatal("wrong sequence removal was not rejected", err)
		}
		// Exercise both snapshot and backwards-compatible ID-only removal. Neither
		// may remove a live custom-sequence definition, even if state looks idle.
		for _, remove := range []func(context.Context) (observation.RemovalResult, error){
			info.Remove,
			func(ctx context.Context) (observation.RemovalResult, error) { return observers.Remove(ctx, info.ID()) },
		} {
			result, err := remove(f.ctx)
			if err != nil || result.Outcome != observation.ObserverSubscribed || result.BlockingNamespace != chronicle.DefaultNamespace {
				t.Fatal("custom sequence subscribed removal not refused", result, err)
			}
		}
		stillLive, err := observers.Get(f.ctx, info.ID(), sequence)
		if err != nil || stillLive == nil || !stillLive.IsSubscribed() {
			t.Fatal("refused removal changed live observer", stillLive, err)
		}
		t.Logf("%s replayable=%t: List subscription unavailable, Get subscribed; custom-sequence removal refused", info.ID(), info.IsReplayable())
	}
	if len(seen) != 2 {
		t.Fatal("List omitted a registered observer", seen)
	}
	remaining, err := observers.List(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range remaining {
		delete(seen, info.ID())
	}
	if len(seen) != 0 {
		t.Fatal("refused removal deleted a shared definition", seen)
	}
}
