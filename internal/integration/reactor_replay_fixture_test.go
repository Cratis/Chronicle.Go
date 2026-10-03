// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/cratis/chronicle.go/events"
)

type KernelReplayInput struct{ Number int }
type KernelOnceInput struct{ Number int }

type replayInvocation struct {
	handler string
	context events.Context
}

type replayTrace struct {
	mu      sync.Mutex
	calls   []replayInvocation
	changed chan struct{}
	log     func(string, events.Context)
}

func newReplayTrace(log func(string, events.Context)) *replayTrace {
	return &replayTrace{changed: make(chan struct{}, 1), log: log}
}

func (trace *replayTrace) record(handler string, ec events.Context) error {
	trace.mu.Lock()
	trace.calls = append(trace.calls, replayInvocation{handler, ec})
	trace.mu.Unlock()
	if trace.log != nil {
		trace.log(handler, ec)
	}
	// Coalesce readiness signals; the synchronized trace retains every invocation.
	select {
	case trace.changed <- struct{}{}:
	default:
	}
	return validateReplayInvocation(replayInvocation{handler, ec})
}

func validateReplayInvocation(call replayInvocation) error {
	isReplay := call.context.ObservationState&events.ObservationReplay != 0
	switch call.handler {
	case "live", "once":
		if isReplay {
			return fmt.Errorf("%s handler received replay flag at position %d", call.handler, call.context.SequenceNumber)
		}
	case "replay":
		if !isReplay {
			return fmt.Errorf("replacement did not receive replay flag at position %d", call.context.SequenceNumber)
		}
	}
	return nil
}

func (trace *replayTrace) snapshot() []replayInvocation {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return slices.Clone(trace.calls)
}

// ready checks coverage, not cardinality: C# ReactorInvoker.HandlerMethods.Resolve
// excludes OnceOnly methods only for replay and routes replay to its replacement.
// Neither guarantee deduplicates ordinary/recovery delivery or replay delivery.
func (trace *replayTrace) ready(livePosition, oncePosition events.SequenceNumber, replay bool) (bool, error) {
	var liveSeen, onceSeen, replaySeen bool
	var notifications []string
	for _, call := range trace.snapshot() {
		if err := validateReplayInvocation(call); err != nil {
			return false, err
		}
		position := livePosition
		switch call.handler {
		case "live":
			liveSeen = true
		case "once":
			position = oncePosition
			onceSeen = true
		case "replay":
			replaySeen = true
			if !replay {
				return false, fmt.Errorf("replacement invoked before explicit replay")
			}
		case "begin", "end":
			notifications = append(notifications, call.handler)
			continue
		default:
			return false, fmt.Errorf("unknown handler %q", call.handler)
		}
		if call.context.SequenceNumber != position {
			return false, fmt.Errorf("%s position = %d, want %d", call.handler, call.context.SequenceNumber, position)
		}
	}
	if !replay {
		if len(notifications) != 0 {
			return false, fmt.Errorf("replay notifications before explicit replay: %v", notifications)
		}
		return liveSeen && onceSeen, nil
	}
	if len(notifications) > 2 || (len(notifications) > 0 && notifications[0] != "begin") || (len(notifications) == 2 && notifications[1] != "end") {
		return false, fmt.Errorf("replay notifications = %v, want begin then end", notifications)
	}
	return liveSeen && onceSeen && replaySeen && len(notifications) == 2, nil
}

type KernelReplayReactor struct{ trace *replayTrace }

func (r *KernelReplayReactor) Live(_ KernelReplayInput, ec events.Context) error {
	return r.trace.record("live", ec)
}
func (r *KernelReplayReactor) Rebuild(_ KernelReplayInput, ec events.Context) error {
	return r.trace.record("replay", ec)
}
func (r *KernelReplayReactor) Once(_ KernelOnceInput, ec events.Context) error {
	return r.trace.record("once", ec)
}
func (r *KernelReplayReactor) BeginReplay(context.Context) error {
	return r.trace.record("begin", events.Context{})
}
func (r *KernelReplayReactor) EndReplay(context.Context) error {
	return r.trace.record("end", events.Context{})
}

func TestReactorReplayTraceAllowsDuplicateDelivery(t *testing.T) {
	trace := newReplayTrace(nil)
	reactor := &KernelReplayReactor{trace: trace}
	live := events.Context{SequenceNumber: 7}
	once := events.Context{SequenceNumber: 8}
	for range 2 {
		if err := reactor.Live(KernelReplayInput{1}, live); err != nil {
			t.Fatal(err)
		}
		if err := reactor.Once(KernelOnceInput{2}, once); err != nil {
			t.Fatal(err)
		}
	}
	if ready, err := trace.ready(7, 8, false); err != nil || !ready {
		t.Fatalf("duplicate live delivery: ready=%t err=%v", ready, err)
	}
	if err := reactor.BeginReplay(context.Background()); err != nil {
		t.Fatal(err)
	}
	live.ObservationState = events.ObservationReplay
	for range 2 {
		if err := reactor.Rebuild(KernelReplayInput{1}, live); err != nil {
			t.Fatal(err)
		}
	}
	if err := reactor.EndReplay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ready, err := trace.ready(7, 8, true); err != nil || !ready {
		t.Fatalf("duplicate replay delivery: ready=%t err=%v", ready, err)
	}
}

func TestReactorReplayTraceRejectsIncorrectObservationState(t *testing.T) {
	for _, handler := range []string{"live", "once", "replay"} {
		t.Run(handler, func(t *testing.T) {
			trace := newReplayTrace(nil)
			reactor := &KernelReplayReactor{trace: trace}
			ec := events.Context{SequenceNumber: 7, ObservationState: events.ObservationReplay}
			var err error
			switch handler {
			case "live":
				err = reactor.Live(KernelReplayInput{1}, ec)
			case "once":
				ec.SequenceNumber = 8
				err = reactor.Once(KernelOnceInput{2}, ec)
			case "replay":
				ec.ObservationState = 0
				err = reactor.Rebuild(KernelReplayInput{1}, ec)
			}
			if err == nil {
				t.Fatal("invalid callback did not fail the handler")
			}
			if ready, err := trace.ready(7, 8, true); err == nil || ready {
				t.Fatalf("invalid callback did not fail the assertion: ready=%t err=%v", ready, err)
			}
		})
	}
}

func TestReactorReplayTraceRequiresEveryPositionAndNotifications(t *testing.T) {
	trace := newReplayTrace(nil)
	reactor := &KernelReplayReactor{trace: trace}
	assertNotReady := func(replay bool) {
		t.Helper()
		if ready, err := trace.ready(7, 8, replay); err != nil || ready {
			t.Fatalf("incomplete delivery: ready=%t err=%v", ready, err)
		}
	}
	assertNotReady(false)
	for range 2 {
		if err := reactor.Live(KernelReplayInput{1}, events.Context{SequenceNumber: 7}); err != nil {
			t.Fatal(err)
		}
	}
	assertNotReady(false) // Duplicate live coverage cannot replace the missing Once position.
	if err := reactor.Once(KernelOnceInput{2}, events.Context{SequenceNumber: 8}); err != nil {
		t.Fatal(err)
	}
	assertNotReady(true)
	if err := reactor.BeginReplay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := reactor.EndReplay(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertNotReady(true) // Notifications alone cannot replace the replayed event.
	if err := reactor.Rebuild(KernelReplayInput{1}, events.Context{SequenceNumber: 9, ObservationState: events.ObservationReplay}); err != nil {
		t.Fatal(err)
	}
	if ready, err := trace.ready(7, 8, true); err == nil || ready {
		t.Fatalf("wrong position satisfied coverage: ready=%t err=%v", ready, err)
	}
}
