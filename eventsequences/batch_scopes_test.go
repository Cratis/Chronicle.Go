// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
)

func TestBatchScopeExpectationsAndResolution(t *testing.T) {
	for _, test := range []struct {
		name                       string
		expectation                eventsequences.Expectation
		tail, want                 uint64
		resolves, noMatch, checked bool
	}{
		{"resolve absent", eventsequences.Resolve(), ^uint64(0), ^uint64(0), true, false, false},
		{"resolve present", eventsequences.Resolve(), 8, 8, true, false, true},
		{"exact zero", eventsequences.Exact(0), 0, 0, false, false, true},
		{"protected absence", eventsequences.NoMatchingEvent(), 0, ^uint64(0), false, true, true},
		{"unchecked", eventsequences.NoCheck(), 0, ^uint64(0), false, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, stream := events.SourceID("decision"), events.StreamID("protected")
			filter := eventsequences.ScopeFilter{SourceID: &source, StreamID: &stream, EventTypes: []events.TypeRef{{ID: "opened", Generation: 1}}}
			if test.expectation == eventsequences.NoCheck() {
				filter = eventsequences.ScopeFilter{}
			}
			captured := make(chan *sequences.AppendManyForEventSourcesRequest, 1)
			sequence, calls := sequenceFixture(t, map[string]rpcHandler{
				"TailSequenceNumber": func(_ context.Context, raw any) (any, error) {
					request := raw.(*sequences.TailSequenceNumberRequest)
					if request.EventSourceId != "decision" || request.EventStreamId != "protected" || request.EventTypeIds != "opened" {
						t.Error(request)
					}
					return tailResponse(test.tail), nil
				},
				"AppendManyForEventSources": func(_ context.Context, raw any) (any, error) {
					captured <- raw.(*sequences.AppendManyForEventSourcesRequest)
					return batchSuccess(metadata.CorrelationID{}, false, 1), nil
				},
			})
			result, err := sequence.AppendBatch(testContext(t), []eventsequences.Entry{{Source: "effect", Event: opened{}}}, eventsequences.WithScopes(
				eventsequences.LabeledScope{Label: "decision", Scope: eventsequences.Scope{Expectation: test.expectation, Filter: filter}},
				eventsequences.LabeledScope{Label: "effect", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}},
			))
			if err != nil || result.Err() != nil {
				t.Fatalf("%+v %v", result, err)
			}
			scope := (<-captured).ConcurrencyScopes[0].Scope
			if scope.SequenceNumber != test.want || scope.ExpectsNoMatchingEvent != test.noMatch {
				t.Fatal(scope)
			}
			wantCalls := int32(1)
			if test.resolves {
				wantCalls++
			}
			if calls.Load() != wantCalls {
				t.Fatal(calls.Load())
			}
		})
	}
}

func TestBatchSnapshotsBeforeResolvingTail(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	captured := make(chan *sequences.AppendManyForEventSourcesRequest, 1)
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{
		"TailSequenceNumber": func(ctx context.Context, _ any) (any, error) {
			close(entered)
			select {
			case <-release:
				return tailResponse(3), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		"AppendManyForEventSources": func(_ context.Context, raw any) (any, error) {
			captured <- raw.(*sequences.AppendManyForEventSourcesRequest)
			return batchSuccess(metadata.CorrelationID{}, true, 1), nil
		},
	})
	value := &opened{Value: "original"}
	tags := []events.Tag{"original-tag"}
	properties := map[string]string{"key": "original"}
	done := make(chan error, 1)
	ctx := testContext(t)
	go func() {
		_, err := sequence.AppendBatch(ctx, []eventsequences.Entry{{Source: "A", Event: value, Tags: tags, Causation: []metadata.Causation{{Type: "entry", Occurred: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Properties: properties}}}})
		done <- err
	}()
	<-entered
	value.Value, tags[0], properties["key"] = "mutated", "mutated", "mutated"
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	request := <-captured
	if request.Events[0].Content != `{"value":"original"}` || request.Events[0].Tags[2] != "original-tag" || request.Events[0].Causation[0].Properties["key"] != "original" {
		t.Fatal(request)
	}
}

func TestResolvedEmptyEventlessScopeIsNotASuccessfulNoOp(t *testing.T) {
	sequence, calls := sequenceFixture(t, map[string]rpcHandler{"TailSequenceNumber": func(context.Context, any) (any, error) { return tailResponse(^uint64(0)), nil }})
	result, err := sequence.AppendBatch(testContext(t), nil, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "decision", Scope: eventsequences.Scope{Expectation: eventsequences.Resolve()}}))
	if !errors.Is(err, chronicle.ErrInvalidConfiguration) || result.Disposition != eventsequences.Unknown || calls.Load() != 1 {
		t.Fatalf("%+v %v calls=%d", result, err, calls.Load())
	}
}
