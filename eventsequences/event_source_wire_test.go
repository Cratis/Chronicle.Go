// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/reactoreffects"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type sourceAppendCase struct {
	variant string
	source  events.SourceName
	options []eventsequences.AppendOption
}

func appendSourceVariant(t *testing.T, sequence *eventsequences.Sequence, c sourceAppendCase) error {
	t.Helper()
	ctx := testContext(t)
	options := append([]eventsequences.AppendOption{eventsequences.WithEventSource(c.source)}, c.options...)
	named := append(append([]eventsequences.AppendOption{}, options...), eventsequences.WithNamedTags(events.NamedTag{Name: "key", Value: "value"}))
	operations := map[string]func() error{
		"single":       func() error { _, err := sequence.Append(ctx, "A", opened{}, options...); return err },
		"single named": func() error { _, err := sequence.Append(ctx, "A", opened{}, named...); return err },
		"many": func() error {
			_, err := sequence.AppendMany(ctx, "A", []any{opened{}, changed{}}, options...)
			return err
		},
		"many named": func() error {
			_, err := sequence.AppendMany(ctx, "A", []any{opened{}, changed{}}, named...)
			return err
		},
	}
	if operation := operations[c.variant]; operation != nil {
		return operation()
	}
	return appendSourceEntries(ctx, sequence, c)
}

func appendSourceEntries(ctx context.Context, sequence *eventsequences.Sequence, c sourceAppendCase) error {
	entries := []eventsequences.Entry{{Source: "A", Event: opened{"original"}, EventSource: c.source}}
	switch c.variant {
	case "batch":
		_, err := sequence.AppendBatch(ctx, entries)
		return err
	case "batch named":
		_, err := sequence.AppendBatch(ctx, entries, eventsequences.WithBatchNamedTags(events.NamedTag{Name: "key", Value: "value"}))
		return err
	case "prepared":
		pending, err := sequence.PrepareBatch(ctx, entries)
		if err != nil {
			return err
		}
		entries[0].EventSource, entries[0].Event = "mutated", opened{"mutated"}
		_, err = sequence.AppendPreparedBatch(ctx, pending)
		return err
	default:
		dispatch, err := sequence.PrepareReturnedEvents(ctx, reactoreffects.Preparation[eventsequences.Entry, eventsequences.LabeledScope]{Entries: entries, Bare: true, Single: c.variant == "returned single"})
		if err != nil {
			return err
		}
		entries[0].EventSource = "mutated"
		return dispatch(ctx)
	}
}

func sourceAppendHandlers(captured chan<- proto.Message, tail chan<- *sequences.TailSequenceNumberRequest) map[string]rpcHandler {
	handlers := map[string]rpcHandler{
		"TailSequenceNumber": func(_ context.Context, raw any) (any, error) {
			tail <- raw.(*sequences.TailSequenceNumberRequest)
			return tailResponse(7), nil
		},
	}
	for _, method := range []string{"Append", "AppendWithNamedTags", "AppendMany", "AppendManyWithNamedTags", "AppendManyForEventSources", "AppendManyForEventSourcesWithNamedTags"} {
		handlers[method] = func(_ context.Context, raw any) (any, error) {
			captured <- raw.(proto.Message)
			if method == "Append" || method == "AppendWithNamedTags" {
				return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, ConcurrencyCheckPerformed: true}}, nil
			}
			count := wireField(raw.(proto.Message).ProtoReflect(), "Events").List().Len()
			return batchSuccess(metadata.CorrelationID{}, true, count), nil
		}
	}
	return handlers
}

func wireField(message protoreflect.Message, name protoreflect.Name) protoreflect.Value {
	return message.Get(message.Descriptor().Fields().ByName(name))
}

func sourceWireMessages(t *testing.T, request proto.Message) []protoreflect.Message {
	t.Helper()
	message := request.ProtoReflect()
	field := message.Descriptor().Fields().ByName("Events")
	if field == nil {
		return []protoreflect.Message{message}
	}
	list := message.Get(field).List()
	if list.Len() == 0 {
		t.Fatal("no outgoing events")
	}
	if list.Get(0).Message().Descriptor().Fields().ByName("EventSource") == nil {
		return []protoreflect.Message{message}
	}
	result := make([]protoreflect.Message, list.Len())
	for i := range result {
		result[i] = list.Get(i).Message()
	}
	return result
}

func assertSourceWire(t *testing.T, request proto.Message, name string) {
	t.Helper()
	for _, message := range sourceWireMessages(t, request) {
		assertSourceDimensions(t, message, name)
	}
}

func assertSourceDimensions(t *testing.T, message protoreflect.Message, name string) {
	t.Helper()
	if message.Descriptor().Fields().ByName("EventSource") == nil {
		if name != "" {
			t.Fatal("legacy many cannot carry source attribution")
		}
		if wireField(message, "EventSourceId").String() != "A" {
			t.Fatal("legacy many lost source ID")
		}
		return
	}
	for field, want := range map[protoreflect.Name]string{"EventSource": name, "EventSourceId": "A", "EventSourceType": "Default", "EventStreamType": "All", "EventStreamId": "Default"} {
		if got := wireField(message, field).String(); got != want {
			t.Errorf("%s = %q, want %q", field, got, want)
		}
	}
}

func requestScope(request proto.Message) *sequences.ConcurrencyScope {
	message := request.ProtoReflect()
	if field := message.Descriptor().Fields().ByName("ConcurrencyScope"); field != nil {
		return message.Get(field).Message().Interface().(*sequences.ConcurrencyScope)
	}
	labeled := wireField(message, "ConcurrencyScopes").List().Get(0).Message()
	return wireField(labeled, "Scope").Message().Interface().(*sequences.ConcurrencyScope)
}

func effectiveSourceType(name events.SourceName) string {
	if name == "" {
		return "Default"
	}
	return string(name)
}

func assertAutomaticSourceScope(t *testing.T, request proto.Message, tail *sequences.TailSequenceNumberRequest) {
	t.Helper()
	want := &sequences.ConcurrencyScope{EventSourceId: true, EventSourceType: tail.EventSourceType, EventStreamType: "All", EventStreamId: "Default", SequenceNumber: 7}
	if actual := requestScope(request); !proto.Equal(actual, want) {
		t.Fatalf("scope = %v, want %v", actual, want)
	}
}

func assertAutomaticSourceTail(t *testing.T, tail *sequences.TailSequenceNumberRequest, name events.SourceName) {
	t.Helper()
	want := &sequences.TailSequenceNumberRequest{EventStore: "store", Namespace: "tenant", EventSequenceId: "event-log", EventSourceId: "A", EventSourceType: effectiveSourceType(name), EventStreamType: "All", EventStreamId: "Default"}
	if !proto.Equal(tail, want) {
		t.Fatalf("tail = %v, want %v", tail, want)
	}
}

func TestEventSourceAppendVariantsKeepWireRouteAndNarrowAutomaticFilter(t *testing.T) {
	for _, variant := range []string{"single", "single named", "many", "many named", "batch", "batch named", "prepared", "returned single", "returned many"} {
		for _, name := range []events.SourceName{"", "Billing"} {
			t.Run(variant+"/"+string(name), func(t *testing.T) {
				captured, tails := make(chan proto.Message, 1), make(chan *sequences.TailSequenceNumberRequest, 1)
				sequence, _ := sequenceFixture(t, sourceAppendHandlers(captured, tails))
				if err := appendSourceVariant(t, sequence, sourceAppendCase{variant: variant, source: name}); err != nil {
					t.Fatal(err)
				}
				request, tail := <-captured, <-tails
				assertSourceWire(t, request, string(name))
				assertAutomaticSourceTail(t, tail, name)
				assertAutomaticSourceScope(t, request, tail)
				assertSourceRPC(t, sourceAppendCase{variant: variant, source: name}, request)
				assertPreparedSourceContent(t, variant, request)
			})
		}
	}
}

func assertPreparedSourceContent(t *testing.T, variant string, request proto.Message) {
	t.Helper()
	if variant != "prepared" {
		return
	}
	content := wireField(wireField(request.ProtoReflect(), "Events").List().Get(0).Message(), "Content").String()
	if content != `{"value":"original"}` {
		t.Fatal("prepared content changed")
	}
}

func assertSourceRPC(t *testing.T, c sourceAppendCase, request proto.Message) {
	t.Helper()
	want := map[string]string{"single": "AppendRequest", "single named": "AppendWithNamedTagsRequest", "many": "AppendManyRequest", "many named": "AppendManyWithNamedTagsRequest", "batch": "AppendManyForEventSourcesRequest", "batch named": "AppendManyForEventSourcesWithNamedTagsRequest", "prepared": "AppendManyForEventSourcesRequest", "returned single": "AppendRequest", "returned many": "AppendManyRequest"}[c.variant]
	if c.source != "" {
		want = attributedSourceRPC(c.variant, want)
	}
	if got := string(request.ProtoReflect().Descriptor().Name()); got != want {
		t.Fatalf("request = %s, want %s", got, want)
	}
}

func attributedSourceRPC(variant, fallback string) string {
	switch variant {
	case "many", "returned many":
		return "AppendManyForEventSourcesRequest"
	case "many named":
		return "AppendManyForEventSourcesWithNamedTagsRequest"
	default:
		return fallback
	}
}

func TestWithEventSourceLastWinsAndEmptyClears(t *testing.T) {
	for _, variant := range []string{"single", "many"} {
		for _, last := range []events.SourceName{"", "Final"} {
			t.Run(variant+"/"+string(last), func(t *testing.T) {
				captured, tails := make(chan proto.Message, 1), make(chan *sequences.TailSequenceNumberRequest, 1)
				sequence, _ := sequenceFixture(t, sourceAppendHandlers(captured, tails))
				err := appendSourceVariant(t, sequence, sourceAppendCase{variant: variant, source: " First ", options: []eventsequences.AppendOption{eventsequences.WithEventSource(last)}})
				if err != nil {
					t.Fatal(err)
				}
				assertSourceWire(t, <-captured, string(last))
				assertAutomaticSourceTail(t, <-tails, last)
			})
		}
	}
}

func TestInvalidEventSourceIsRejectedBeforeRPC(t *testing.T) {
	names := map[string]events.SourceName{"blank": " \t", "leading": " Billing", "trailing": "Billing "}
	variants := []string{"single", "single named", "many", "many named", "batch", "batch named", "prepared", "returned single", "returned many"}
	for label, source := range names {
		for _, variant := range variants {
			t.Run(label+"/"+variant, func(t *testing.T) {
				sequence, calls := sequenceFixture(t, nil)
				err := appendSourceVariant(t, sequence, sourceAppendCase{variant: variant, source: source})
				if !errors.Is(err, chronicle.ErrInvalidConfiguration) || calls.Load() != 0 {
					t.Fatalf("error = %v, RPCs = %d", err, calls.Load())
				}
			})
		}
	}
}

func TestProtectedEmptyAppendManyRejectsPaddedSourceBeforeRPC(t *testing.T) {
	sequence, calls := sequenceFixture(t, nil)
	scope := eventsequences.Scope{Expectation: eventsequences.Exact(7)}
	_, err := sequence.AppendMany(testContext(t), "A", nil, eventsequences.WithEventSource(" Billing"), eventsequences.WithScope(scope))
	if !errors.Is(err, chronicle.ErrInvalidConfiguration) || calls.Load() != 0 {
		t.Fatalf("error = %v, RPCs = %d", err, calls.Load())
	}
}

func sourceFailureHandlers(diagnostic string) map[string]rpcHandler {
	return map[string]rpcHandler{
		"TailSequenceNumber": func(context.Context, any) (any, error) { return tailResponse(7), nil },
		"Append": func(context.Context, any) (any, error) {
			return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{HasErrors: true, ConcurrencyCheckPerformed: true, Errors: []string{diagnostic}}}, nil
		},
		"AppendManyForEventSources": func(context.Context, any) (any, error) {
			return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{HasErrors: true, ConcurrencyCheckPerformed: true, Errors: []string{diagnostic}}}, nil
		},
	}
}

func sourceFailureResult(t *testing.T, sequence *eventsequences.Sequence, many bool, route eventsequences.Route) (eventsequences.Disposition, error, error) {
	t.Helper()
	options := []eventsequences.AppendOption{eventsequences.WithEventSource("Billing"), eventsequences.WithRoute(route)}
	if many {
		result, err := sequence.AppendMany(testContext(t), "A", []any{opened{}}, options...)
		return result.Disposition, result.Err(), err
	}
	result, err := sequence.Append(testContext(t), "A", opened{}, options...)
	return result.Disposition, result.Err(), err
}

func assertUnknownSourceDiagnostic(t *testing.T, err error, diagnostic string) {
	t.Helper()
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("diagnostic = %v", err)
	}
	if !errors.Is(err, eventsequences.AppendError(diagnostic)) {
		t.Fatalf("missing %q: %v", diagnostic, err)
	}
}

func TestEventSourceDiagnosticsPreserveLegacyUnknownDisposition(t *testing.T) {
	cases := map[string]struct {
		route eventsequences.Route
		want  string
	}{
		"unknown source":  {want: "UnknownEventSource: The event source 'Billing' is not registered."},
		"mismatched type": {eventsequences.Route{SourceType: "Other"}, "EventSourceTypeDoesNotMatchEventSource: The event source type 'Other' does not match the event source 'Billing'."},
		"unknown stream":  {eventsequences.Route{StreamType: "Missing"}, "UnknownEventStreamForEventSource: The event stream 'Missing' does not belong to the event source 'Billing'."},
	}
	for name, c := range cases {
		for variant, many := range map[string]bool{"single": false, "many": true} {
			t.Run(name+"/"+variant, func(t *testing.T) {
				sequence, _ := sequenceFixture(t, sourceFailureHandlers(c.want))
				disposition, diagnostic, err := sourceFailureResult(t, sequence, many, c.route)
				if disposition != eventsequences.Unknown {
					t.Fatal(disposition)
				}
				assertUnknownSourceDiagnostic(t, diagnostic, c.want)
				assertUnknownSourceDiagnostic(t, err, c.want)
			})
		}
	}
}
