// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

func mergeSnapshot(sequence *Sequence, content string, scopes ...LabeledScope) *PreparedBatch {
	return &PreparedBatch{sequence: sequence, explicit: scopes, batch: preparedBatch{
		request: &sequences.AppendManyForEventSourcesRequest{Events: []*sequences.EventForEventSourceId{{Content: content}}},
	}}
}

func TestPreparedMergePreservesOrderAndScopeSets(t *testing.T) {
	sequence := &Sequence{}
	a, b := events.TypeRef{ID: "a"}, events.TypeRef{ID: "b"}
	first := mergeSnapshot(sequence, "first", LabeledScope{Label: "source", Scope: Scope{Expectation: NoCheck(), Filter: ScopeFilter{EventTypes: []events.TypeRef{a, b, a}}}})
	second := mergeSnapshot(sequence, "second", LabeledScope{Label: "source", Scope: Scope{Expectation: NoCheck(), Filter: ScopeFilter{EventTypes: []events.TypeRef{b, a}}}}, LabeledScope{Label: "other", Scope: Scope{Expectation: NoCheck()}})
	merged, err := first.Merge(second)
	if err != nil {
		t.Fatal(err)
	}
	contents := merged.GetEvents()
	if !reflect.DeepEqual(contents, []json.RawMessage{json.RawMessage("first"), json.RawMessage("second")}) {
		t.Fatal("event order changed", contents)
	}
	if !reflect.DeepEqual(merged.explicit, []LabeledScope{first.explicit[0], second.explicit[1]}) {
		t.Fatal("scope order or set equality changed", merged.explicit)
	}
	if len(first.explicit) != 1 {
		t.Fatal("merge changed the first input's scopes")
	}
	for _, input := range []*PreparedBatch{first, second} {
		if len(input.GetEvents()) != 1 {
			t.Fatal("merge changed an input's events")
		}
	}
}

func TestPreparedMergeRejectsIncompatibleInputsInOrder(t *testing.T) {
	sequence := &Sequence{}
	first := mergeSnapshot(sequence, "first", LabeledScope{Label: "source", Scope: Scope{Expectation: NoCheck()}})
	for _, tc := range []struct {
		name string
		next *PreparedBatch
		want string
	}{
		{"nil", nil, "batches must use the same sequence handle"},
		{"handle before conflicting scopes", mergeSnapshot(&Sequence{}, "second", LabeledScope{Label: "source", Scope: Scope{Expectation: NoMatchingEvent()}}), "batches must use the same sequence handle"},
		{"conflicting scope", mergeSnapshot(sequence, "second", LabeledScope{Label: "source", Scope: Scope{Expectation: NoMatchingEvent()}}), "conflicting concurrency scopes for label \"source\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := first.Merge(tc.next)
			if !errors.Is(err, faults.ErrInvalidConfiguration) || err.Error() != "chronicle: invalid configuration: "+tc.want {
				t.Fatalf("error = %v, want %s", err, tc.want)
			}
		})
	}
}

func TestScopeEqualityIncludesEveryFilter(t *testing.T) {
	source, sourceType, streamType, stream := events.SourceID("source"), events.SourceType("type"), events.StreamType("stream"), events.StreamID("id")
	base := Scope{Expectation: NoCheck()}
	for _, tc := range []struct {
		name  string
		scope Scope
	}{
		{"expectation", Scope{Expectation: NoMatchingEvent()}},
		{"source", Scope{Expectation: NoCheck(), Filter: ScopeFilter{SourceID: &source}}},
		{"source type", Scope{Expectation: NoCheck(), Filter: ScopeFilter{SourceType: &sourceType}}},
		{"stream type", Scope{Expectation: NoCheck(), Filter: ScopeFilter{StreamType: &streamType}}},
		{"stream", Scope{Expectation: NoCheck(), Filter: ScopeFilter{StreamID: &stream}}},
		{"event types", Scope{Expectation: NoCheck(), Filter: ScopeFilter{EventTypes: []events.TypeRef{{ID: "event"}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if equalScopes(base, tc.scope) || equalScopes(tc.scope, base) {
				t.Fatal("different scopes compare equal")
			}
			if !equalScopes(tc.scope, cloneScope(tc.scope)) {
				t.Fatal("owned copy compares unequal")
			}
		})
	}
	left := Scope{Filter: ScopeFilter{EventTypes: []events.TypeRef{{ID: "a"}}}}
	right := Scope{Filter: ScopeFilter{EventTypes: []events.TypeRef{{ID: "b"}}}}
	if equalScopes(left, right) {
		t.Fatal("same-sized distinct event sets compare equal")
	}
}
