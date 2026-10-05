// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestEvictionRetainsLocalWatchAndAppendSubscriptions(t *testing.T) {
	client, ctx := supervisionClient(t, &supervisedKernel{})
	old, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[RuntimeOtherModel](readmodels.WithObserver(readmodels.Reducer, "fold"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	service, err := readmodels.New(old.name, old.namespace, catalog, client.transport, readmodels.WithReductionChanges(&old.readModelChanges))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := readmodels.For(service, model).Watch(ctx, readmodels.WithWatchBuffer(1, 128))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	notifications := 0
	defer old.EventLog().OnAppend(func(eventsequences.AppendNotification) { notifications++ })()
	evictStores(t, client)
	fresh, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	g := client.current
	client.mu.Unlock()
	fresh.readModelChanges.Publish(g.ctx, model.Identifier(), "key", json.RawMessage(`{"id":"key","name":"after eviction"}`))
	change, err := sub.Recv()
	if err != nil || change.Value.Name != "after eviction" {
		t.Fatal("old watch detached from live feed", change, err)
	}
	if _, err := fresh.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
		t.Fatal(err)
	}
	if notifications != 1 {
		t.Fatal("append callback ownership lost or multiplied", notifications)
	}
	// The same retained feed keeps its original byte cap after reacquisition.
	fresh.readModelChanges.Publish(g.ctx, model.Identifier(), "key", json.RawMessage(`{"name":"`+strings.Repeat("x", 128)+`"}`))
	awaitSignal(t, ctx, sub.Done())
	if !errors.Is(sub.Err(), readmodels.ErrOverloaded) {
		t.Fatal("byte bound changed", sub.Err())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
