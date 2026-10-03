// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

func TestResolveScopeIndependentTargetsStageWithoutReresolving(t *testing.T) {
	for _, path := range []string{"batch", "unit"} {
		t.Run(path, func(t *testing.T) {
			tails := map[string]uint64{"A": 0, "B": 12}
			sequence, calls := parityFixture(t, map[string]rpcHandler{
				"TailSequenceNumber": func(_ context.Context, raw any) (any, error) {
					r := raw.(*sequences.TailSequenceNumberRequest)
					if r.EventSourceType != "orders" || r.EventStreamType != "sales" || r.EventStreamId != "selected" || r.EventTypeIds != "personChanged" || r.EventStore != "store" || r.Namespace != "tenant" || r.EventSequenceId != "event-log" {
						t.Errorf("filter/coordinates lost: %v", r)
					}
					tail, ok := tails[r.EventSourceId]
					if !ok {
						t.Fatalf("unexpected source: %q", r.EventSourceId)
					}
					return tailResponse(tail), nil
				},
				"AppendManyForEventSources": func(_ context.Context, raw any) (any, error) {
					r := raw.(*sequences.AppendManyForEventSourcesRequest)
					if len(r.ConcurrencyScopes) != 2 {
						t.Fatalf("scopes = %v", r.ConcurrencyScopes)
					}
					for _, labeled := range r.ConcurrencyScopes {
						scope := labeled.Scope
						if !scope.EventSourceId || scope.SequenceNumber != tails[labeled.EventSourceId] || scope.EventStreamId != "selected" || scope.EventTypes[0].Id != "personChanged" {
							t.Errorf("resolved expectation changed: %v", labeled)
						}
					}
					return batchSuccess(metadata.CorrelationID{}, true, len(r.Events)), nil
				},
			}, parityCatalog(t), eventsequences.ConcurrencyPolicy{})
			ctx := testContext(t)
			var scopes []eventsequences.LabeledScope
			for _, target := range []events.SourceID{"A", "B"} {
				sourceType, streamType, streamID := events.SourceType("orders"), events.StreamType("sales"), events.StreamID("selected")
				refs := []events.TypeRef{{ID: "personChanged", Generation: 3}}
				scope, err := sequence.ResolveScope(ctx, eventsequences.ScopeFilter{SourceID: &target, SourceType: &sourceType, StreamType: &streamType, StreamID: &streamID, EventTypes: refs})
				if err != nil || scope.Expectation != eventsequences.Exact(events.SequenceNumber(tails[string(target)])) {
					t.Fatalf("target %s: scope=%+v error=%v", target, scope, err)
				}
				scopes = append(scopes, eventsequences.LabeledScope{Label: string(target), Scope: scope})
				// Returned filters own their pointers and event-type slices.
				target, streamID, refs[0].ID = "mutated", "mutated", "mutated"
			}
			if calls.Load() != 2 {
				t.Fatalf("resolution appended or retried: calls=%d", calls.Load())
			}
			entries := []eventsequences.Entry{{Source: "A", Event: personChanged{}}, {Source: "B", Event: personChanged{}}}
			var err error
			if path == "batch" {
				_, err = sequence.AppendBatch(ctx, entries, eventsequences.WithScopes(scopes...))
			} else {
				unit, owner, beginErr := transactions.Begin(ctx, sequence)
				if beginErr != nil {
					t.Fatal(beginErr)
				}
				if err := unit.Stage(ctx, entries, scopes...); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 2 {
					t.Fatal("staging performed I/O")
				}
				_, err = owner.Commit(ctx)
			}
			if err != nil || calls.Load() != 3 {
				t.Fatalf("dispatch reread the tail: error=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestResolveScopeUsesCustomStrategyAndFirstAppendPolicy(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, custom := range []bool{false, true} {
			name := map[bool]string{false: "unchecked", true: "protected"}[enabled] + "/" + map[bool]string{false: "default", true: "custom"}[custom]
			t.Run(name, func(t *testing.T) {
				source, stream := events.SourceID("decision"), events.StreamID("selected")
				filter := eventsequences.ScopeFilter{SourceID: &source, StreamID: &stream}
				strategyCalls := 0
				var expectedSequence *eventsequences.Sequence
				ctx := testContext(t)
				var strategy eventsequences.ConcurrencyScopeStrategy
				if custom {
					strategy = strategyFunc(func(gotCtx context.Context, sequence *eventsequences.Sequence, got eventsequences.ScopeFilter) (eventsequences.Scope, error) {
						strategyCalls++
						if gotCtx != ctx || sequence != expectedSequence || !reflect.DeepEqual(got, filter) {
							t.Errorf("strategy inputs = %v %p %+v", gotCtx, sequence, got)
						}
						return eventsequences.Scope{Filter: got}, nil
					})
				}
				sequence, calls := parityFixture(t, map[string]rpcHandler{
					"TailSequenceNumber": func(_ context.Context, raw any) (any, error) {
						r := raw.(*sequences.TailSequenceNumberRequest)
						if r.EventSourceId != "decision" || r.EventStreamId != "selected" || r.EventStreamType != "" || r.EventSourceType != "" {
							t.Errorf("explicit filter acquired route defaults: %v", r)
						}
						return tailResponse(uint64(events.Unavailable)), nil
					},
					"AppendManyForEventSources": func(_ context.Context, raw any) (any, error) {
						scope := raw.(*sequences.AppendManyForEventSourcesRequest).ConcurrencyScopes[0].Scope
						if scope.ExpectsNoMatchingEvent != enabled || scope.SequenceNumber != uint64(events.Unavailable) || !scope.EventSourceId || scope.EventStreamId != "selected" {
							t.Errorf("resolved empty scope = %v", scope)
						}
						return batchSuccess(metadata.CorrelationID{}, enabled, 1), nil
					},
				}, parityCatalog(t), eventsequences.ConcurrencyPolicy{Strategy: strategy, CheckFirstAppendIntoAScope: enabled})
				expectedSequence = sequence
				scope, err := sequence.ResolveScope(ctx, filter)
				if err != nil || calls.Load() != 1 || scope.Expectation == eventsequences.Resolve() || !reflect.DeepEqual(scope.Filter, filter) {
					t.Fatalf("resolution = %+v %v calls=%d", scope, err, calls.Load())
				}
				if enabled && scope.Expectation != eventsequences.NoMatchingEvent() {
					t.Fatal("first append was not protected")
				}
				_, err = sequence.AppendBatch(ctx, []eventsequences.Entry{{Source: source, Event: personChanged{}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: string(source), Scope: scope}))
				wantStrategyCalls := 0
				if custom {
					wantStrategyCalls = 1
				}
				if err != nil || calls.Load() != 2 || strategyCalls != wantStrategyCalls {
					t.Fatalf("error=%v RPC=%d strategy=%d", err, calls.Load(), strategyCalls)
				}
			})
		}
	}
}

func TestResolveScopeExplicitStrategyExpectationsDoNotReadTail(t *testing.T) {
	for _, expectation := range []eventsequences.Expectation{eventsequences.Exact(0), eventsequences.NoMatchingEvent(), eventsequences.NoCheck()} {
		sequence, calls := parityFixture(t, nil, parityCatalog(t), eventsequences.ConcurrencyPolicy{CheckFirstAppendIntoAScope: true, Strategy: strategyFunc(func(context.Context, *eventsequences.Sequence, eventsequences.ScopeFilter) (eventsequences.Scope, error) {
			return eventsequences.Scope{Expectation: expectation}, nil
		})})
		scope, err := sequence.ResolveScope(testContext(t), eventsequences.ScopeFilter{})
		if err != nil || scope.Expectation != expectation || calls.Load() != 0 {
			t.Fatalf("scope=%+v error=%v calls=%d", scope, err, calls.Load())
		}
	}
}

func TestResolveScopeValidationCancellationAndFailuresNeverAppendOrRetry(t *testing.T) {
	failure := errors.New("resolution failed")
	for _, mode := range []string{"invalid-filter", "invalid-strategy", "strategy-error", "tail-error", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			strategyCalls := 0
			sequence, calls := parityFixture(t, map[string]rpcHandler{
				"TailSequenceNumber": func(context.Context, any) (any, error) { return nil, failure },
			}, parityCatalog(t), eventsequences.ConcurrencyPolicy{Strategy: strategyFunc(func(_ context.Context, _ *eventsequences.Sequence, filter eventsequences.ScopeFilter) (eventsequences.Scope, error) {
				strategyCalls++
				if mode == "strategy-error" {
					return eventsequences.Scope{}, failure
				}
				if mode == "invalid-strategy" {
					other := events.SourceID("other")
					filter.SourceID = &other
				}
				return eventsequences.Scope{Filter: filter}, nil
			})})
			source := events.SourceID("source")
			filter := eventsequences.ScopeFilter{SourceID: &source}
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			if mode == "invalid-filter" {
				filter.EventTypes = []events.TypeRef{{ID: "invalid,comma", Generation: 1}}
			}
			_, err := sequence.ResolveScope(ctx, filter)
			if err == nil {
				t.Fatal("resolution succeeded")
			}
			wantCalls, wantStrategy := int32(0), 1
			if mode == "tail-error" {
				wantCalls = 1
			}
			if mode == "canceled" || mode == "invalid-filter" {
				wantStrategy = 0
			}
			if calls.Load() != wantCalls || strategyCalls != wantStrategy {
				t.Fatalf("RPC=%d strategy=%d", calls.Load(), strategyCalls)
			}
			if mode == "strategy-error" && !errors.Is(err, failure) || mode == "canceled" && !errors.Is(err, context.Canceled) || (mode == "invalid-filter" || mode == "invalid-strategy") && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("error identity lost: %v", err)
			}
		})
	}
}
