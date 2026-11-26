// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/reactoreffects"
	"google.golang.org/protobuf/proto"
)

func appendScopedSource(t *testing.T, sequence *eventsequences.Sequence, variant string, scope eventsequences.Scope) error {
	t.Helper()
	ctx := testContext(t)
	entries := []eventsequences.Entry{{Source: "A", Event: opened{}, EventSource: "Billing", Route: eventsequences.Route{SourceType: "Explicit"}}}
	labeled := eventsequences.LabeledScope{Label: "A", Scope: scope}
	switch variant {
	case "single":
		_, err := sequence.Append(ctx, "A", opened{}, eventsequences.WithEventSource("Billing"), eventsequences.WithRoute(eventsequences.Route{SourceType: "Explicit"}), eventsequences.WithScope(scope))
		return err
	case "batch":
		_, err := sequence.AppendBatch(ctx, entries, eventsequences.WithScopes(labeled))
		return err
	case "prepared":
		pending, err := sequence.PrepareBatch(ctx, entries, eventsequences.WithScopes(labeled))
		if err != nil {
			return err
		}
		_, err = sequence.AppendPreparedBatch(ctx, pending)
		return err
	default:
		dispatch, err := sequence.PrepareReturnedEvents(ctx, reactoreffects.Preparation[eventsequences.Entry, eventsequences.LabeledScope]{Entries: entries, Batch: true, Scopes: []eventsequences.LabeledScope{labeled}})
		if err != nil {
			return err
		}
		return dispatch(ctx)
	}
}

func TestEventSourceDoesNotRewriteExplicitScope(t *testing.T) {
	for _, variant := range []string{"single", "batch", "prepared", "returned"} {
		t.Run(variant, func(t *testing.T) {
			captured, tails := make(chan proto.Message, 1), make(chan *sequences.TailSequenceNumberRequest, 1)
			sequence, calls := sequenceFixture(t, sourceAppendHandlers(captured, tails))
			filterType := events.SourceType("Unrelated")
			scope := eventsequences.Scope{Filter: eventsequences.ScopeFilter{SourceType: &filterType}, Expectation: eventsequences.Exact(4)}
			if err := appendScopedSource(t, sequence, variant, scope); err != nil {
				t.Fatal(err)
			}
			request := <-captured
			want := &sequences.ConcurrencyScope{EventSourceType: "Unrelated", SequenceNumber: 4}
			if actual := requestScope(request); !proto.Equal(actual, want) {
				t.Fatalf("scope = %v, want %v", actual, want)
			}
			if calls.Load() != 1 || len(tails) != 0 {
				t.Fatal("explicit scope performed an automatic tail read")
			}
		})
	}
}

func TestEventSourceStrategyReceivesEffectiveTypeWithoutChangingRoute(t *testing.T) {
	for _, sourceType := range []events.SourceType{"Default", "Explicit"} {
		t.Run(string(sourceType), func(t *testing.T) {
			captured, tails := make(chan proto.Message, 1), make(chan *sequences.TailSequenceNumberRequest, 1)
			strategyCalls := 0
			strategy := strategyFunc(func(_ context.Context, _ *eventsequences.Sequence, filter eventsequences.ScopeFilter) (eventsequences.Scope, error) {
				strategyCalls++
				assertEffectiveSourceFilter(t, filter, sourceType)
				return eventsequences.Scope{Filter: filter, Expectation: eventsequences.Exact(4)}, nil
			})
			catalog := sourceScopeCatalog(t)
			sequence, _ := parityFixture(t, sourceAppendHandlers(captured, tails), catalog, eventsequences.ConcurrencyPolicy{Strategy: strategy, CheckFirstAppendIntoAScope: true})
			_, err := sequence.Append(testContext(t), "A", opened{}, eventsequences.WithEventSource("Billing"), eventsequences.WithRoute(eventsequences.Route{SourceType: sourceType}))
			if err != nil {
				t.Fatal(err)
			}
			request := (<-captured).(*sequences.AppendRequest)
			if request.EventSourceType != string(sourceType) || request.EventSource != "Billing" {
				t.Fatal(request)
			}
			if strategyCalls != 1 {
				t.Fatal(strategyCalls)
			}
			if len(tails) != 0 {
				t.Fatal("strategy unexpectedly read tail")
			}
		})
	}
}

func sourceScopeCatalog(t *testing.T) *events.Catalog {
	t.Helper()
	definition, err := events.Define[opened](events.WithID("opened"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func assertEffectiveSourceFilter(t *testing.T, filter eventsequences.ScopeFilter, sourceType events.SourceType) {
	t.Helper()
	want := sourceType
	if want == "Default" {
		want = "Billing"
	}
	if filter.SourceType == nil || filter.SourceID == nil {
		t.Fatalf("missing dimensions: %+v", filter)
	}
	if *filter.SourceType != want || *filter.SourceID != "A" {
		t.Fatalf("filter = %+v", filter)
	}
}

func TestEventSourceUnifiesDefaultAndExplicitEffectiveTypeInBatch(t *testing.T) {
	captured, tails := make(chan proto.Message, 1), make(chan *sequences.TailSequenceNumberRequest, 2)
	sequence, _ := sequenceFixture(t, sourceAppendHandlers(captured, tails))
	_, err := sequence.AppendBatch(testContext(t), []eventsequences.Entry{
		{Source: "A", Event: opened{}, EventSource: "Billing"},
		{Source: "A", Event: changed{}, EventSource: "Billing", Route: eventsequences.Route{SourceType: "Billing"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := (<-captured).(*sequences.AppendManyForEventSourcesRequest)
	if len(request.ConcurrencyScopes) != 1 || len(tails) != 1 {
		t.Fatal(request.ConcurrencyScopes, len(tails))
	}
	if request.Events[0].EventSourceType != "Default" || request.Events[1].EventSourceType != "Billing" {
		t.Fatal(request.Events)
	}
}
