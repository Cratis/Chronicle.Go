//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
)

type KernelEffectTrigger struct{ Mode string }
type KernelEffectResult struct{ Number int }

func TestKernelReactorRichEffects(t *testing.T) {
	f := newKernelFixture(t)
	r := integrationRegistry[KernelEffectTrigger](t)
	if _, err := chronicle.RegisterEvent[KernelEffectResult](r); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactor[*KernelRichEffects](r, func() *KernelRichEffects { return &KernelRichEffects{} }, reactors.WithID("go-rich-effects"), reactors.OnceOnly("Produce")); err != nil {
		t.Fatal(err)
	}
	client := f.client(r)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	// The reactor stream is open, but the kernel subscribes it asynchronously.
	// Appending first would start a reactor catch-up that drops later live
	// events (https://github.com/Cratis/Chronicle/issues/4558).
	if _, err = store.WaitForRegistration(f.ctx); err != nil {
		t.Fatal(err)
	}
	awaitObserversObserving(t, f, store.Namespace(), append([]string{"go-rich-effects"}, eventLogStatisticsObservers...)...)
	for _, mode := range []string{"bare", "targeted", "mixed", "scoped"} {
		appendSuccessfully(t, f.ctx, store, events.SourceID(mode), KernelEffectTrigger{Mode: mode})
	}
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		all := true
		for _, mode := range []string{"bare", "targeted", "mixed", "scoped"} {
			source := events.SourceID(mode)
			if mode == "targeted" || mode == "scoped" {
				source = events.SourceID(mode + "-target")
			}
			history, err := store.EventLog().ReadSource(ctx, source, eventsequences.SourceFilter{})
			if err != nil {
				t.Fatal(err)
			}
			var values []int
			for _, event := range history {
				if event.Context.EventType.ID == "KernelEffectResult" {
					var value KernelEffectResult
					if err := json.Unmarshal(event.Content, &value); err != nil {
						t.Fatal(err)
					}
					values = append(values, value.Number)
				}
			}
			want := []int{1, 2}
			if mode == "mixed" {
				want = []int{1}
			}
			if !slices.Equal(values, want) {
				all = false
			}
		}
		if all {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("returned effect history incomplete", ctx.Err())
		case <-ticker.C:
		}
	}
	// The wrapper in the mixed collection must retain its own source and subject.
	history, err := store.EventLog().ReadSource(ctx, "mixed-target", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Context.Subject != "explicit-subject" {
		t.Fatal(history)
	}
}

type KernelRichEffects struct{}

func (*KernelRichEffects) Produce(e KernelEffectTrigger) []any {
	subject := events.Subject("explicit-subject")
	entry := func(number int) eventsequences.Entry {
		return eventsequences.Entry{Source: events.SourceID(e.Mode + "-target"), Event: KernelEffectResult{number}, Subject: &subject}
	}
	switch e.Mode {
	case "bare":
		return []any{KernelEffectResult{1}, KernelEffectResult{2}}
	case "targeted":
		return []any{entry(1), entry(2)}
	case "mixed":
		return []any{KernelEffectResult{1}, entry(2)}
	case "scoped":
		source := events.SourceID("independent")
		return []any{eventsequences.EventsWithConcurrencyScopes{Events: []eventsequences.Entry{entry(1), entry(2)}, Scopes: []eventsequences.LabeledScope{{Label: "independent", Scope: eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent(), Filter: eventsequences.ScopeFilter{SourceID: &source}}}}}}
	}
	return nil
}

func TestKernelReactorReplayReplacementOnceOnlyAndNotifications(t *testing.T) {
	f := newKernelFixture(t)
	t.Logf("replay fixture store=%s", f.storeName)
	r := integrationRegistry[KernelReplayInput](t)
	if _, err := chronicle.RegisterEvent[KernelOnceInput](r); err != nil {
		t.Fatal(err)
	}
	trace := newReplayTrace(func(handler string, ec events.Context) {
		t.Logf("%s handler=%s position=%d observation=%d", time.Now().UTC().Format(time.RFC3339Nano), handler, ec.SequenceNumber, ec.ObservationState)
	})
	if err := chronicle.RegisterReactor[*KernelReplayReactor](r, func() *KernelReplayReactor {
		return &KernelReplayReactor{trace: trace}
	}, reactors.WithID("replay-parity"), reactors.Replay("Rebuild"), reactors.OnceOnly("Once")); err != nil {
		t.Fatal(err)
	}
	client := f.client(r)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	// The reactor/reducer stream is open, but the kernel subscribes it asynchronously.
	// Appending first would start a catch-up that drops later live events
	// (https://github.com/Cratis/Chronicle/issues/4558).
	if _, err = store.WaitForRegistration(f.ctx); err != nil {
		t.Fatal(err)
	}
	awaitObserversObserving(t, f, store.Namespace(), append([]string{"replay-parity"}, eventLogStatisticsObservers...)...)
	first := appendSuccessfully(t, f.ctx, store, "one", KernelReplayInput{1})
	last := appendSuccessfully(t, f.ctx, store, "one", KernelOnceInput{2})
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	observers := contracts.NewObserversClient(f.conn)
	request := &contracts.GetObserverInformationRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "replay-parity"}
	// Capture evidence before client cleanup, including when the poll RPC itself
	// reaches the deadline. Neither a timeout nor a duplicate implies #4548.
	diagnose := func() {
		diagnostics, done := context.WithTimeout(f.ctx, 5*time.Second)
		defer done()
		info, infoErr := observers.GetObserverInformation(diagnostics, request)
		history, historyErr := store.EventLog().ReadSource(diagnostics, "one", eventsequences.SourceFilter{})
		failures, failuresErr := store.Observers().FailedPartitions(diagnostics, "replay-parity")
		jobs, jobsErr := store.Jobs().List(diagnostics)
		t.Logf("invocations=%+v state=%s subscribed=%t last=%d next=%d tail=%d handled=%d infoError=%v", trace.snapshot(), info.GetRunningState(), info.GetIsSubscribed(), info.GetLastHandledEventSequenceNumber(), info.GetNextEventSequenceNumber(), info.GetTailEventSequenceNumber(), info.GetHandledEventCount(), infoErr)
		t.Logf("historyError=%v failures=%v failureError=%v jobs=%v jobError=%v", historyErr, failures, failuresErr, jobs, jobsErr)
		for _, event := range history {
			t.Logf("persisted position=%d type=%s content=%s", event.Context.SequenceNumber, event.Context.EventType.ID, event.Content)
		}
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := observers.GetObserverInformation(ctx, request)
		if err != nil {
			diagnose()
			t.Fatal(err)
		}
		// OnceOnly is replay exclusion, not exactly-once live/recovery delivery.
		// Require both appended positions, allowing repeated ordinary callbacks.
		ready, err := trace.ready(*first.Position, *last.Position, false)
		if err != nil {
			diagnose()
			t.Fatal(err)
		}
		if info.LastHandledEventSequenceNumber == uint64(*last.Position) && ready {
			break
		}
		select {
		case <-ctx.Done():
			diagnose()
			t.Fatal("live observer did not finish", ctx.Err())
		case <-ticker.C:
		}
	}
	// Exact C# Reactors.Replay request: empty EventSequenceId, resolved by kernel.
	job, err := observers.Replay(ctx, &contracts.Replay{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "replay-parity", EventSequenceId: ""})
	if err != nil {
		t.Fatal(err)
	}
	if job.JobId == "" {
		t.Fatal("replay returned no job")
	}
	// Wait for begin/end AND replacement coverage of the original event position.
	// Every invocation is checked for its replay flag; duplicate coverage is OK.
	for {
		ready, err := trace.ready(*first.Position, *last.Position, true)
		if err != nil {
			diagnose()
			t.Fatal(err)
		}
		if ready {
			break
		}
		select {
		case <-trace.changed:
		case <-ctx.Done():
			diagnose()
			t.Fatal("replay did not complete", ctx.Err())
		}
	}
	failures, err := store.Observers().FailedPartitions(ctx, "replay-parity")
	if err != nil || len(failures) != 0 {
		diagnose()
		t.Fatalf("replay handler failures: %v, read error: %v", failures, err)
	}
}
