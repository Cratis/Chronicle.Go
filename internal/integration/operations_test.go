//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
)

type OperationsEvent struct{ Number int }
type OperationsReactor struct {
	entered       chan int
	release       <-chan struct{}
	replayGate    <-chan struct{}
	replaying     *atomic.Bool
	replayStarted chan struct{}
	replayEnded   chan struct{}
}

func (r *OperationsReactor) Handle(ctx context.Context, e OperationsEvent) error {
	if e.Number < 0 {
		return errors.New("operations deliberate failed partition")
	}
	gate := r.release
	if r.replaying.Load() {
		gate = r.replayGate
	}
	select {
	case r.entered <- e.Number:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *OperationsReactor) BeginReplay(context.Context) error {
	r.replaying.Store(true)
	r.replayStarted <- struct{}{}
	return nil
}
func (r *OperationsReactor) EndReplay(context.Context) error {
	r.replaying.Store(false)
	r.replayEnded <- struct{}{}
	return nil
}

func TestKernelOperationsCompletionReplayAndJobEvidence(t *testing.T) {
	f := newKernelFixture(t)
	registry := integrationRegistry[OperationsEvent](t)
	entered := make(chan int, 8)
	release := make(chan struct{})
	replayGate := make(chan struct{})
	started := make(chan struct{}, 1)
	ended := make(chan struct{}, 1)
	var replaying atomic.Bool
	// Cleanup releases a task-owned blocked handler on early failure before closing its client.
	var released, replayReleased atomic.Bool
	releaseLive := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	releaseReplay := func() {
		if replayReleased.CompareAndSwap(false, true) {
			close(replayGate)
		}
	}
	if err := chronicle.RegisterReactor[*OperationsReactor](registry, func() *OperationsReactor {
		return &OperationsReactor{entered, release, replayGate, &replaying, started, ended}
	}, reactors.WithID("operations")); err != nil {
		t.Fatal(err)
	}
	client := f.client(registry)
	t.Cleanup(releaseLive)
	t.Cleanup(releaseReplay)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	observers := store.Observers()
	waitOperations(t, f.ctx, func() bool {
		state, err := observers.Get(f.ctx, "operations", events.EventLog)
		if err != nil {
			t.Fatal(err)
		}
		return state != nil && state.IsSubscribed()
	})
	appended, err := store.EventLog().AppendWithMetadata(f.ctx, "blocked", OperationsEvent{1})
	if err != nil || appended.Result().Err() != nil {
		t.Fatal(err, appended.Result().Err())
	}
	select {
	case <-entered:
	case <-f.ctx.Done():
		t.Fatal("live delivery missing", f.ctx.Err())
	}
	timedOut, err := appended.WaitForCompletion(f.ctx, observers, 50*time.Millisecond)
	if err != nil || timedOut.IsSuccess() || !timedOut.TimedOut() {
		t.Fatalf("blocked catch-up=%+v error=%v", timedOut, err)
	}
	t.Logf("blocked catch-up: timedOut=%v outstanding=%v", timedOut.TimedOut(), timedOut.OutstandingObservers())
	releaseLive()
	completed, err := appended.WaitForCompletion(f.ctx, observers, 5*time.Second)
	if err != nil || !completed.IsSuccess() || completed.TimedOut() || completed.Trivial() {
		t.Fatalf("released catch-up=%+v error=%v", completed, err)
	}
	info, err := observers.Get(f.ctx, "operations", events.EventLog)
	if err != nil || info == nil {
		t.Fatal(info, err)
	}
	tail, exists, err := store.EventLog().TailForObserver(f.ctx, info.EventTypes())
	if err != nil || !exists || tail != *appended.Result().Position {
		t.Fatal(tail, exists, err)
	}
	handle, err := observers.Replay(f.ctx, "operations", events.EventLog)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal("replay did not start", f.ctx.Err())
	}
	job, err := handle.Get(f.ctx)
	if err != nil || job == nil {
		t.Fatalf("blocked replay job=%v error=%v", job, err)
	}
	if job.ID() != handle.ID() {
		t.Fatal("replay job identity changed")
	}
	steps, err := job.Steps(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("accepted replay job=%s type=%s status=%d steps=%d", job.ID(), job.Type(), job.Status(), len(steps))
	releaseReplay()
	select {
	case <-ended:
	case <-f.ctx.Done():
		t.Fatal("replay did not end", f.ctx.Err())
	}
	final, err := store.Jobs().WaitForTerminalOrAbsent(f.ctx, handle.ID(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if final != nil && final.Status() != jobs.CompletedSuccessfully {
		t.Fatalf("replay retained failed terminal job: status=%d changes=%v", final.Status(), final.StatusChanges())
	}
	if final == nil {
		t.Log("replay job disappeared: outcome remains absent; EndReplay and observer catch-up are separate evidence")
	} else {
		t.Log("kernel retained CompletedSuccessfully job evidence")
	}
	completed, err = appended.WaitForCompletion(f.ctx, observers, 5*time.Second)
	if err != nil || !completed.IsSuccess() {
		t.Fatal(completed, err)
	}
	// Persistent removal is refused while the local runtime remains subscribed.
	removal, err := observers.Remove(f.ctx, "operations")
	if err != nil || (removal.Outcome != observation.ObserverActive && removal.Outcome != observation.ObserverSubscribed) {
		t.Fatal(removal, err)
	}
	if err := store.UnregisterReactor(f.ctx, "operations"); err != nil {
		t.Fatal(err)
	}
	waitOperations(t, f.ctx, func() bool {
		state, err := observers.Get(f.ctx, "operations", events.EventLog)
		if err != nil {
			t.Fatal(err)
		}
		return state != nil && !state.IsSubscribed() && state.RunningState() == observation.Disconnected
	})
	removal, err = observers.Remove(f.ctx, "operations")
	if err != nil || removal.Outcome != observation.Removed {
		t.Fatal(removal, err)
	}
	all, err := observers.List(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range all {
		if state.ID() == "operations" {
			t.Fatal("persistent removal left observer")
		}
	}
}

func TestKernelOperationsFailedPartitionDiagnostics(t *testing.T) {
	f := newKernelFixture(t)
	registry := integrationRegistry[OperationsEvent](t)
	if err := chronicle.RegisterReactorHandler(registry, "operations-failure", func(context.Context, OperationsEvent) error {
		return errors.New("operations deliberate failed partition")
	}); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appended, err := store.EventLog().AppendWithMetadata(f.ctx, "failed-partition", OperationsEvent{-1})
	if err != nil || appended.Result().Err() != nil {
		t.Fatal(err, appended.Result().Err())
	}
	var failure observation.FailedPartition
	waitOperations(t, f.ctx, func() bool {
		all, err := store.Observers().FailedPartitions(f.ctx, "operations-failure")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range all {
			if p.Partition() == "failed-partition" {
				failure = p
				return len(p.Attempts()) > 0
			}
		}
		return false
	})
	found := false
	for _, a := range failure.Attempts() {
		for _, message := range a.Messages() {
			if strings.Contains(message, "operations deliberate failed partition") {
				found = true
			}
		}
	}
	if !found || failure.Observer() != "operations-failure" {
		t.Fatal("missing failed attempt diagnostic", failure)
	}
	result, err := appended.WaitForCompletion(f.ctx, store.Observers(), time.Second)
	if err != nil || result.IsSuccess() || len(result.FailedPartitions()) == 0 {
		t.Fatal(result, err)
	}
	t.Logf("failed partition=%s resolved=%v quarantined=%v attempts=%d", failure.Partition(), failure.IsResolved(), failure.IsQuarantined(), len(failure.Attempts()))
}
func waitOperations(t *testing.T, parent context.Context, predicate func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("operations state did not arrive", ctx.Err())
		case <-ticker.C:
		}
	}
}
