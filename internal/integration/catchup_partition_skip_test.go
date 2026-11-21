//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

// CatchUpCounted carries its own partition and sequence so the reactor can
// report exactly which events it received.
type CatchUpCounted struct {
	Partition string `json:"partition"`
	Seq       int    `json:"seq"`
}

// CatchUpTally is the reducer's per-partition count of witnessed events.
type CatchUpTally struct {
	ID    string `json:"id"`
	Count int    `json:"count"`
}

const (
	catchUpWitnessTag   events.Tag = "catchup-witness"
	catchUpNoiseTag     events.Tag = "catchup-noise"
	catchUpHistory                 = 20
	catchUpLive                    = 10
	catchUpWitnessOptIn            = "CHRONICLE_CATCHUP_PARTITION_WITNESS"
)

var catchUpPartitions = []string{"catchup-a", "catchup-b", "catchup-c"}

// catchUpFilteredPartition has witnessed history, then only filtered events,
// so its catch-up advances past events the observers never receive
// (Chronicle PR #4575 widens the window this way).
const catchUpFilteredPartition = "catchup-filtered"

type catchUpDeliveries struct {
	mu   sync.Mutex
	seen map[string]map[int]bool
}

func (d *catchUpDeliveries) record(event CatchUpCounted) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[event.Partition] == nil {
		d.seen[event.Partition] = map[int]bool{}
	}
	d.seen[event.Partition][event.Seq] = true
}

// missing lists every expected (partition, seq) the reactor has not received.
func (d *catchUpDeliveries) missing(expected map[string]int) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var gaps []string
	for partition, count := range expected {
		for seq := range count {
			if !d.seen[partition][seq] {
				gaps = append(gaps, fmt.Sprintf("%s#%d", partition, seq))
			}
		}
	}
	return gaps
}

func catchUpRegistry(t *testing.T, deliveries *catchUpDeliveries) (*chronicle.Registry, readmodels.Model[CatchUpTally]) {
	t.Helper()
	registry := integrationRegistry[CatchUpCounted](t)
	model, err := chronicle.RegisterReadModel[CatchUpTally](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactorHandler(registry, "go-catchup-witness", func(_ context.Context, event CatchUpCounted) error {
		deliveries.record(event)
		return nil
	}, reactors.WithEventTagFilter(string(catchUpWitnessTag))); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReducerHandlers(registry, model, "go-catchup-tally", []reducers.Handler{
		reducers.On(func(_ context.Context, _ CatchUpCounted, current *CatchUpTally, ec events.Context) (*CatchUpTally, error) {
			next := &CatchUpTally{ID: string(ec.SourceID), Count: 1}
			if current != nil {
				next.Count += current.Count
			}
			return next, nil
		}),
	}, reducers.WithEventTagFilter(catchUpWitnessTag)); err != nil {
		t.Fatal(err)
	}
	return registry, model
}

func appendCatchUp(t *testing.T, ctx context.Context, store *chronicle.EventStore, partition string, seq int, tag events.Tag) {
	t.Helper()
	appendSuccessfully(t, ctx, store, events.SourceID(partition), CatchUpCounted{Partition: partition, Seq: seq}, eventsequences.WithTags(tag))
}

// seedCatchUpHistory appends history before any observer exists, so the
// observers registered later must catch up several partitions at once.
func seedCatchUpHistory(t *testing.T, f *kernelFixture) map[string]int {
	t.Helper()
	store, err := f.client(integrationRegistry[CatchUpCounted](t)).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]int{catchUpFilteredPartition: 1}
	for seq := range catchUpHistory {
		for _, partition := range catchUpPartitions {
			appendCatchUp(t, f.ctx, store, partition, seq, catchUpWitnessTag)
		}
	}
	appendCatchUp(t, f.ctx, store, catchUpFilteredPartition, 0, catchUpWitnessTag)
	for seq := 1; seq <= catchUpHistory; seq++ {
		appendCatchUp(t, f.ctx, store, catchUpFilteredPartition, seq, catchUpNoiseTag)
	}
	for _, partition := range catchUpPartitions {
		expected[partition] = catchUpHistory
	}
	return expected
}

// appendCatchUpLive appends to every partition while the new observers are
// still catching up, so live events race the per-partition catch-up steps.
func appendCatchUpLive(t *testing.T, ctx context.Context, store *chronicle.EventStore, expected map[string]int) {
	t.Helper()
	for range catchUpLive {
		for _, partition := range catchUpPartitions {
			appendCatchUp(t, ctx, store, partition, expected[partition], catchUpWitnessTag)
			expected[partition]++
		}
	}
}

func awaitCatchUpComplete(t *testing.T, ctx context.Context, deliveries *catchUpDeliveries, reader *readmodels.Reader[CatchUpTally], expected map[string]int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		gaps := deliveries.missing(expected)
		stale, err := staleCatchUpTallies(ctx, reader, expected)
		if err == nil && len(gaps) == 0 && len(stale) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("catch-up skipped events (Chronicle#4583): reactor missing %v; reducer stale %v; read error %v", gaps, stale, err)
		case <-ticker.C:
		}
	}
}

func staleCatchUpTallies(ctx context.Context, reader *readmodels.Reader[CatchUpTally], expected map[string]int) ([]string, error) {
	var stale []string
	for partition, count := range expected {
		tally, err := reader.Get(ctx, readmodels.Key(partition))
		if err != nil {
			return nil, err
		}
		if !tally.Exists || tally.Value.Count != count {
			stale = append(stale, fmt.Sprintf("%s=%+v want %d", partition, tally, count))
		}
	}
	return stale, nil
}

// Chronicle#4583: observer-wide catch-up across partitions can skip an event
// for one partition that arrives while another partition finishes catch-up.
// Reactors can miss the event and reducers can keep stale state.
func TestKernelCatchUpAcrossPartitionsDeliversEveryEvent(t *testing.T) {
	if os.Getenv(catchUpWitnessOptIn) == "" {
		t.Skipf("skipped by default: the race in https://github.com/Cratis/Chronicle/issues/4583 depends on kernel scheduling and cannot be reproduced or ruled out deterministically (https://github.com/Cratis/Chronicle.Go/issues/93); set %s=1 to run it, and remove this skip when the kernel bump in https://github.com/Cratis/Chronicle.Go/issues/96 pins a fixed kernel", catchUpWitnessOptIn)
	}
	f := newKernelFixture(t)
	expected := seedCatchUpHistory(t, f)
	deliveries := &catchUpDeliveries{seen: map[string]map[int]bool{}}
	registry, model := catchUpRegistry(t, deliveries)
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WaitForRegistration(f.ctx); err != nil {
		t.Fatal(err)
	}
	appendCatchUpLive(t, f.ctx, store, expected)
	awaitCatchUpComplete(t, f.ctx, deliveries, readmodels.For(store.ReadModels(), model), expected)
}
