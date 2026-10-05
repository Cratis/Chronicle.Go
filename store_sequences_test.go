// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"errors"
	"sync"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestStoreSequenceHandlesShareAppendNotifications(t *testing.T) {
	client, _ := testClient(t, &fakeKernel{})
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.EventSequence("x")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.EventSequence("x")
	if err != nil || first != second {
		t.Fatalf("handles = %p, %p, error = %v", first, second, err)
	}
	log, err := store.EventSequence(events.EventLog)
	if err != nil || log != store.EventLog() {
		t.Fatalf("event log = %p, want %p, error = %v", log, store.EventLog(), err)
	}
	var received []eventsequences.AppendNotification
	defer first.OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })()
	if _, err = second.Append(ctx, "source", CustomerRegistered{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	if len(received) != 1 || received[0].Operation.Sequence() != "x" || received[0].Result.Disposition != eventsequences.Committed {
		t.Fatalf("notifications = %+v", received)
	}
	otherNamespace, err := client.EventStore(ctx, "customers", chronicle.WithNamespace("other"))
	if err != nil {
		t.Fatal(err)
	}
	otherStore, err := client.EventStore(ctx, "other")
	if err != nil {
		t.Fatal(err)
	}
	otherClient, _ := testClient(t, &fakeKernel{})
	otherClientStore, err := otherClient.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	for _, isolatedStore := range []*chronicle.EventStore{otherNamespace, otherStore, otherClientStore} {
		sequence, err := isolatedStore.EventSequence("x")
		if err != nil || sequence == first {
			t.Fatalf("isolated handle = %p, error = %v", sequence, err)
		}
		if _, err = sequence.Append(ctx, "source", CustomerRegistered{Name: "Grace"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = log.Append(ctx, "source", CustomerRegistered{Name: "Grace"}); err != nil {
		t.Fatal(err)
	}
	if len(received) != 1 {
		t.Fatalf("isolated appends notified: %+v", received)
	}
}

func TestConcurrentStoreSequenceCallsShareHandles(t *testing.T) {
	client, _ := testClient(t, &fakeKernel{})
	store, err := client.EventStore(testContext(t), "customers")
	if err != nil {
		t.Fatal(err)
	}
	const callers = 64
	ids := []events.SequenceID{"x", "y", events.EventLog}
	handles := make([][]*eventsequences.Sequence, callers)
	failures := make([]error, callers)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range callers {
		workers.Go(func() {
			<-start
			for _, id := range ids {
				sequence, err := store.EventSequence(id)
				if err != nil {
					failures[i] = err
					return
				}
				handles[i] = append(handles[i], sequence)
			}
		})
	}
	close(start)
	workers.Wait()
	for i := range callers {
		if failures[i] != nil {
			t.Fatal(failures[i])
		}
		for j, id := range ids {
			if handles[i][j] == nil || handles[i][j] != handles[0][j] {
				t.Fatalf("caller %d sequence %q = %p, want %p", i, id, handles[i][j], handles[0][j])
			}
		}
	}
	if handles[0][2] != store.EventLog() || handles[0][0] == handles[0][1] {
		t.Fatal("sequence cache lost distinct IDs or seeded event log")
	}
}

func TestStoreSequenceRejectsInvalidIDs(t *testing.T) {
	client, _ := testClient(t, &fakeKernel{})
	store, err := client.EventStore(testContext(t), "customers")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []events.SequenceID{"", " \t\n"} {
		for range 2 {
			sequence, err := store.EventSequence(id)
			if sequence != nil || !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("sequence %q = %p, error = %v", id, sequence, err)
			}
		}
	}
}
