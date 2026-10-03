// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

type configuredScopeStrategy struct{ filters []eventsequences.ScopeFilter }

func (s *configuredScopeStrategy) GetScope(_ context.Context, _ *eventsequences.Sequence, filter eventsequences.ScopeFilter) (eventsequences.Scope, error) {
	s.filters = append(s.filters, filter)
	return eventsequences.Scope{Filter: filter}, nil
}

func TestClientConfiguredPolicyResolvesExplicitFilters(t *testing.T) {
	strategy := &configuredScopeStrategy{}
	tailCalls := 0
	kernel := &fakeKernel{tail: func(_ context.Context, r *sequences.TailSequenceNumberRequest) (*sequences.QueryResult_EventSequenceTailResponse, error) {
		tailCalls++
		if r.EventSourceId != "target" || r.EventStreamId != "selected" || r.EventSourceType != "" || r.EventStreamType != "" {
			t.Errorf("filter changed: %v", r)
		}
		return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: uint64(events.Unavailable)}}, nil
	}}
	client, _ := testClient(t, kernel, chronicle.WithDefaultConcurrencyStrategy(strategy), chronicle.WithCheckFirstAppendIntoAScope(true))
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	source, stream := events.SourceID("target"), events.StreamID("selected")
	scope, err := store.EventLog().ResolveScope(ctx, eventsequences.ScopeFilter{SourceID: &source, StreamID: &stream})
	if err != nil || scope.Expectation != eventsequences.NoMatchingEvent() || tailCalls != 1 || kernel.appendCalls.Load() != 0 || len(strategy.filters) != 1 {
		t.Fatalf("scope=%+v error=%v tails=%d appends=%d strategy=%d", scope, err, tailCalls, kernel.appendCalls.Load(), len(strategy.filters))
	}
	if *strategy.filters[0].SourceID != source || *strategy.filters[0].StreamID != stream || strategy.filters[0].StreamType != nil {
		t.Fatal("strategy did not receive selected dimensions")
	}
}
