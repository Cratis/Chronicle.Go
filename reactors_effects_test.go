// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reactors"
)

func registerEffect[F any](t *testing.T, registry *chronicle.Registry, value F, options ...reactors.Option) {
	t.Helper()
	if err := chronicle.RegisterReactorHandlers(registry, "effects", []reactors.Handler{
		reactors.Returning(func(context.Context, ReactorInput) (F, error) { return value, nil }),
	}, options...); err != nil {
		t.Fatal(err)
	}
}
func batchSuccess(count int) *sequences.CommandResult_AppendManyResponse {
	positions := make([]uint64, count)
	for i := range positions {
		positions[i] = uint64(i)
	}
	return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: positions, ConcurrencyCheckPerformed: true}}
}

func TestReactorRichEffectsWireShapesAndOrdering(t *testing.T) {
	subject := events.Subject("subject")
	occurred := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	entry := eventsequences.Entry{Source: "target", Event: ReactorOutput{7}, Route: eventsequences.Route{SourceType: "customer", StreamType: "audit", StreamID: "stream"}, Subject: &subject, Occurred: &occurred, Tags: []events.Tag{"one", "two"}, Causation: []metadata.Causation{{Type: "entry", Occurred: occurred}}}
	for _, tc := range []struct {
		name     string
		register func(*testing.T, *chronicle.Registry)
		count    int
		many     bool
		targeted bool
		scoped   bool
	}{
		{"bare", func(t *testing.T, r *chronicle.Registry) { registerEffect(t, r, []ReactorOutput{{1}, {2}}) }, 2, true, false, false},
		{"array", func(t *testing.T, r *chronicle.Registry) { registerEffect(t, r, [2]ReactorOutput{{1}, {2}}) }, 2, true, false, false},
		{"targeted", func(t *testing.T, r *chronicle.Registry) { registerEffect(t, r, entry) }, 1, false, true, false},
		{"targeted pointer", func(t *testing.T, r *chronicle.Registry) { registerEffect(t, r, &entry) }, 1, false, true, false},
		{"targeted collection", func(t *testing.T, r *chronicle.Registry) {
			registerEffect(t, r, []eventsequences.Entry{entry, {Source: "other", Event: ReactorOutput{8}}, entry})
		}, 3, false, true, false},
		{"mixed", func(t *testing.T, r *chronicle.Registry) { registerEffect(t, r, []any{entry, ReactorOutput{8}, entry}) }, 3, false, true, false},
		{"scoped", func(t *testing.T, r *chronicle.Registry) {
			registerEffect(t, r, eventsequences.EventsWithConcurrencyScopes{Events: []eventsequences.Entry{entry, {Source: "other", Event: ReactorOutput{8}}, entry}, Scopes: []eventsequences.LabeledScope{{Label: "decision", Scope: eventsequences.Scope{Expectation: eventsequences.Exact(5)}}}})
		}, 3, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := reactorRegistry(t)
			tc.register(t, r)
			k := &reactorKernel{}
			var calls atomic.Int32
			k.appendMany = func(request *sequences.AppendManyRequest) *sequences.CommandResult_AppendManyResponse {
				calls.Add(1)
				if !tc.many || len(request.Events) != tc.count || request.EventSequenceId != string(events.EventLog) || request.EventSourceId != "source" {
					t.Errorf("unexpected many: %v", request)
				}
				return batchSuccess(tc.count)
			}
			k.appendBatch = func(request *sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
				calls.Add(1)
				if tc.many || len(request.Events) != tc.count || request.EventSequenceId != string(events.EventLog) {
					t.Errorf("unexpected batch: %v", request)
					return batchSuccess(tc.count)
				}
				first := request.Events[0]
				if first.EventSourceId != "target" || first.EventSourceType != "customer" || first.EventStreamType != "audit" || first.EventStreamId != "stream" || first.Subject != "subject" || first.GetOccurred().GetValue() != "2026-01-02T03:04:05.0000000+00:00" || !slices.Equal(first.Tags, []string{"one", "two"}) || len(first.Causation) != 2 {
					t.Errorf("wrapper metadata lost: %v", first)
				}
				if tc.count == 3 && (request.Events[2].EventSourceId != "target" || request.Events[1].Content != `{"number":8}`) {
					t.Errorf("order lost: %v", request.Events)
				}
				if tc.scoped && (request.ConcurrencyScopes[0].EventSourceId != "decision" || request.ConcurrencyScopes[0].Scope.SequenceNumber != 5) {
					t.Errorf("scope lost: %v", request.ConcurrencyScopes)
				}
				return batchSuccess(tc.count)
			}
			_, _, ctx := reactorClient(t, k, r)
			s := receive(t, ctx, k.sessions)
			s.batches <- batch(0)
			result := receive(t, ctx, s.results)
			if result.State != contracts.ObservationState_Success || calls.Load() != 1 {
				t.Fatalf("result %v calls %d", result, calls.Load())
			}
		})
	}
}

type EffectMetadataReactor struct{}

func (*EffectMetadataReactor) Handle(ReactorInput) []any {
	return []any{ReactorOutput{1}, eventsequences.Entry{Source: "explicit", Event: ReactorOutput{2}}}
}
func (*EffectMetadataReactor) GetEventSourceID() events.SourceID { return "provided-source" }
func (*EffectMetadataReactor) GetEventStreamID() events.StreamID { return "provided-stream" }
func (*EffectMetadataReactor) GetSubject() *events.Subject {
	value := events.Subject("provided-subject")
	return &value
}

func TestReactorMetadataProvidersAndFiltersVersusLabels(t *testing.T) {
	r := reactorRegistry(t)
	labels := []string{"label", "label"}
	filters := []string{"filtered", "other", "filtered", "other"}
	options := []reactors.Option{reactors.WithID("metadata"), reactors.WithTags(labels...), reactors.WithEventTagFilter(filters...), reactors.WithEventSourceType("source-type"), reactors.WithEventStreamType("stream-type"), reactors.WithEventStreamID("configured-stream"), reactors.WithEventSequence("external-inbox"), reactors.OnceOnly()}
	labels[0] = "mutated"
	filters[0] = "mutated"
	if err := chronicle.RegisterReactor[*EffectMetadataReactor](r, func() *EffectMetadataReactor { return &EffectMetadataReactor{} }, options...); err != nil {
		t.Fatal(err)
	}
	k := &reactorKernel{}
	k.appendBatch = func(request *sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
		if request.EventSequenceId != string(events.EventLog) || len(request.Events) != 2 {
			t.Errorf("not event log: %v", request)
			return batchSuccess(2)
		}
		bare, target := request.Events[0], request.Events[1]
		if bare.EventSourceId != "provided-source" || bare.EventSourceType != "source-type" || bare.EventStreamType != "stream-type" || bare.EventStreamId != "provided-stream" || bare.Subject != "provided-subject" {
			t.Errorf("bare metadata: %v", bare)
		}
		if target.EventSourceId != "explicit" || target.EventSourceType != "Default" || target.EventStreamType != "All" || target.EventStreamId != "Default" || target.Subject != "explicit" {
			t.Errorf("reactor defaults leaked into wrapper: %v", target)
		}
		return batchSuccess(2)
	}
	_, _, ctx := reactorClient(t, k, r)
	s := receive(t, ctx, k.sessions)
	definition := s.registration.Reactor
	if definition.IsReplayable || definition.EventSequenceId != "external-inbox" || !slices.Equal(definition.Tags, []string{"label"}) || !slices.Equal(definition.Filters.FilterTags, []string{"filtered", "other"}) || definition.Filters.EventSourceType != "source-type" || definition.Filters.EventStreamType != "stream-type" {
		t.Fatal(definition)
	}
	s.batches <- batch(0)
	if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success {
		t.Fatal(result)
	}
}

func TestReactorEffectsValidateBeforeWritesAndPropagateRejection(t *testing.T) {
	for _, tc := range []struct {
		name        string
		values      []any
		rejected    bool
		wantWrites  int
		wantSuccess bool
	}{
		{"nil", nil, false, 0, true},
		{"empty", []any{}, false, 0, true},
		{"unregistered member", []any{ReactorOutput{1}, struct{ Unknown string }{"bad"}}, false, 0, false},
		{"nil member", []any{ReactorOutput{1}, (*ReactorOutput)(nil)}, false, 0, false},
		{"invalid wrapper", []any{ReactorOutput{1}, eventsequences.Entry{Event: ReactorOutput{2}}}, false, 0, false},
		{"rejected", []any{ReactorOutput{1}, ReactorOutput{2}}, true, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := reactorRegistry(t)
			registerEffect(t, r, tc.values)
			k := &reactorKernel{}
			var writes atomic.Int32
			respond := func() *sequences.CommandResult_AppendManyResponse {
				writes.Add(1)
				return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{HasConstraintViolations: true, ConstraintViolations: []*sequences.ConstraintViolation{{ConstraintName: "unique", Message: "rejected"}}}}
			}
			k.appendMany = func(*sequences.AppendManyRequest) *sequences.CommandResult_AppendManyResponse { return respond() }
			k.appendBatch = func(*sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
				return respond()
			}
			_, _, ctx := reactorClient(t, k, r)
			s := receive(t, ctx, k.sessions)
			s.batches <- batch(0, 1)
			result := receive(t, ctx, s.results)
			if (result.State == contracts.ObservationState_Success) != tc.wantSuccess || int(writes.Load()) != tc.wantWrites {
				t.Fatalf("%v writes %d", result, writes.Load())
			}
			if !tc.wantSuccess && result.LastSuccessfulObservation != uint64(events.Unavailable) {
				t.Fatal("failed effect acknowledged", result)
			}
		})
	}
}

type ReturnedCommand struct{ Value int }
type CommandEffects struct {
	name                string
	trace               *[]string
	failure             error
	panicClassification bool
	claimEvent          bool
}

func (h *CommandEffects) CanHandleReturnType(t reflect.Type) bool {
	return t == reflect.TypeFor[ReturnedCommand]()
}
func (h *CommandEffects) CanHandle(c reactors.SideEffectContext, value any) bool {
	if h.panicClassification {
		panic("classification failure")
	}
	if h.trace != nil {
		*h.trace = append(*h.trace, "classify-"+h.name)
	}
	if h.claimEvent {
		_, ok := c.Events.Lookup(value)
		return ok
	}
	_, ok := value.(ReturnedCommand)
	return ok
}
func (h *CommandEffects) Handle(ctx context.Context, c reactors.SideEffectContext, _ any) error {
	if h.trace != nil {
		*h.trace = append(*h.trace, "handle-"+h.name)
	}
	if c.Scope == nil || c.Runtime == nil || c.Events == nil || c.Delivery.Store != "reactors" || metadata.Identity(ctx).OnBehalfOf == nil {
		return errors.New("extension missing invocation context")
	}
	return h.failure
}
func TestReactorCustomEffectsInvokeAllMatchesAndFailPartition(t *testing.T) {
	r := reactorRegistry(t)
	var trace []string
	for _, name := range []string{"registry-one", "registry-two"} {
		h := &CommandEffects{name: name, trace: &trace}
		if name == "registry-one" {
			h.failure = errors.New("command denied")
		}
		if err := chronicle.RegisterReactorSideEffectHandler(r, h); err != nil {
			t.Fatal(err)
		}
	}
	registerEffect(t, r, ReturnedCommand{42}, reactors.WithSideEffectHandlers(&CommandEffects{name: "local", trace: &trace}))
	k := &reactorKernel{}
	_, _, ctx := reactorClient(t, k, r)
	s := receive(t, ctx, k.sessions)
	s.batches <- batch(0)
	result := receive(t, ctx, s.results)
	want := []string{"classify-registry-one", "classify-registry-two", "classify-local", "handle-registry-one", "handle-registry-two", "handle-local"}
	if result.State != contracts.ObservationState_Failed || !slices.Equal(trace, want) || k.appendCalls.Load() != 0 {
		t.Fatal(result, trace)
	}
}
func TestReactorClassifiesExtensionsBeforeBuiltinAppend(t *testing.T) {
	r := reactorRegistry(t)
	registerEffect(t, r, ReactorOutput{42}, reactors.WithSideEffectHandlers(&CommandEffects{panicClassification: true}))
	k := &reactorKernel{}
	_, _, ctx := reactorClient(t, k, r)
	s := receive(t, ctx, k.sessions)
	s.batches <- batch(0)
	if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Failed || k.appendCalls.Load() != 0 {
		t.Fatal(result, k.appendCalls.Load())
	}
}
