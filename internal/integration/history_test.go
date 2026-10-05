//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestKernelEventlessChecksAndUpperBoundExpectations(t *testing.T) {
	ctx, sequence := batchFixture(t)
	history, err := sequence.ReadHistory(ctx, "decision", eventsequences.SourceFilter{})
	if err != nil || len(history.Events) != 0 || history.Expectation != eventsequences.NoMatchingEvent() {
		t.Fatalf("%+v %v", history, err)
	}
	check := eventsequences.LabeledScope{Label: "decision", Scope: history.Scope()}
	result, err := sequence.AppendBatch(ctx, nil, eventsequences.WithScopes(check))
	if err != nil || result.Err() != nil || len(result.Positions) != 0 || !result.ConcurrencyCheckPerformed {
		t.Fatalf("eventless absence: %+v %v", result, err)
	}
	if has, err := sequence.HasEvents(ctx, "decision"); err != nil || has {
		t.Fatalf("check must not append: %v %v", has, err)
	}
	appended, err := sequence.AppendMany(ctx, "decision", []any{BatchOpened{"first"}}, eventsequences.WithScope(history.Scope()))
	if err != nil || appended.Err() != nil {
		t.Fatalf("%+v %v", appended, err)
	}
	if tail, exists, err := sequence.Tail(ctx, eventsequences.TailFilter{}); err != nil || !exists || tail != 0 {
		t.Fatalf("zero tail: %d %v %v", tail, exists, err)
	}
	result, err = sequence.AppendBatch(ctx, nil, eventsequences.WithScopes(check))
	if err != nil || result.Disposition != eventsequences.Rejected || len(result.ConcurrencyViolations) != 1 {
		t.Fatalf("eventless conflict: %+v %v", result, err)
	}
	// Exact is an upper bound, not equality: a tail below 20 is accepted.
	result, err = sequence.AppendBatch(ctx, nil, eventsequences.WithScopes(sourceScope("decision", eventsequences.Exact(20))))
	if err != nil || result.Err() != nil || !result.ConcurrencyCheckPerformed {
		t.Fatalf("eventless upper bound: %+v %v", result, err)
	}
	result, err = sequence.AppendMany(ctx, "decision", nil, eventsequences.WithScope(sourceScope("decision", eventsequences.Exact(0)).Scope))
	if err != nil || result.Err() != nil || !result.ConcurrencyCheckPerformed {
		t.Fatalf("same-source eventless: %+v %v", result, err)
	}
	// Resolving a narrow type filter protects only matching history.
	source := events.SourceID("decision")
	filter := eventsequences.ScopeFilter{SourceID: &source, EventTypes: []events.TypeRef{{ID: "batch-opened", Generation: 1}}}
	result, err = sequence.AppendBatch(ctx, []eventsequences.Entry{{Source: "decision", Event: BatchChanged{"second"}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "decision", Scope: eventsequences.Scope{Expectation: eventsequences.Resolve(), Filter: filter}}))
	if err != nil || result.Err() != nil || !result.ConcurrencyCheckPerformed {
		t.Fatalf("resolved filtered scope: %+v %v", result, err)
	}
	tail, exists, err := sequence.Tail(ctx, eventsequences.TailFilter(filter))
	if err != nil || !exists || tail != 0 {
		t.Fatalf("filtered tail: %d %v %v", tail, exists, err)
	}
	if next, err := sequence.Next(ctx); err != nil || next != 2 {
		t.Fatalf("next: %d %v", next, err)
	}
}
