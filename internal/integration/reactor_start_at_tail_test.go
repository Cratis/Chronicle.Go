//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
)

type StartAtTailInput struct{ Name string }

type sink struct {
	mu    sync.Mutex
	names []string
}

func (s *sink) add(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.names = append(s.names, name)
}

func (s *sink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.names)
}

type TailReactor struct{ seen *sink }

func (r *TailReactor) On(input StartAtTailInput) { r.seen.add(input.Name) }

// HistoryReactor is the control: the same registration without StartAtTail catches up on all history.
type HistoryReactor struct{ seen *sink }

func (r *HistoryReactor) On(input StartAtTailInput) { r.seen.add(input.Name) }

func TestKernelReactorStartAtTailSkipsHistory(t *testing.T) {
	f := newKernelFixture(t)

	// History exists before either reactor has ever been registered.
	writer := f.client(integrationRegistry[StartAtTailInput](t))
	writerStore, err := writer.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"one", "two", "three"} {
		appendSuccessfully(t, f.ctx, writerStore, events.SourceID("source-"+name), StartAtTailInput{Name: name})
	}

	tail, history := &sink{}, &sink{}
	registry := integrationRegistry[StartAtTailInput](t)
	if err := chronicle.RegisterReactor[*TailReactor](registry, func() *TailReactor { return &TailReactor{tail} }, reactors.WithID("start-at-tail"), reactors.StartAtTail()); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactor[*HistoryReactor](registry, func() *HistoryReactor { return &HistoryReactor{history} }, reactors.WithID("from-the-beginning")); err != nil {
		t.Fatal(err)
	}
	reader := f.client(registry)
	store, err := reader.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	eventually := func(what string, accept func() bool) {
		t.Helper()
		for !accept() {
			select {
			case <-ctx.Done():
				t.Fatalf("timed out waiting for %s; tail=%v history=%v", what, tail.snapshot(), history.snapshot())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	// The control proves the history was deliverable, so the tail reactor's silence is not vacuous.
	eventually("the control reactor to catch up on history", func() bool { return len(history.snapshot()) == 3 })

	appendSuccessfully(t, ctx, store, "source-after", StartAtTailInput{Name: "after"})
	eventually("both reactors to see the new event", func() bool { return len(tail.snapshot()) == 1 && len(history.snapshot()) == 4 })

	if got := tail.snapshot(); !slices.Equal(got, []string{"after"}) {
		t.Fatalf("start-at-tail reactor saw %v, want only the event appended after it registered", got)
	}
	// Each event has its own event source, so the kernel may deliver the partitions in any order.
	got := history.snapshot()
	slices.Sort(got)
	if want := []string{"after", "one", "three", "two"}; !slices.Equal(got, want) {
		t.Fatalf("control reactor saw %v, want %v", got, want)
	}
}
