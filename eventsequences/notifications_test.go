// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These paths share result assertions, including the named-tag RPC variants and
// metadata wrappers. The heterogeneous inputs deliberately repeat source A.
var notificationPaths = []struct {
	name, rpc string
	single    bool
}{
	{"single", "Append", true},
	{"single named", "AppendWithNamedTags", true},
	{"single metadata", "Append", true},
	{"many", "AppendMany", false},
	{"many named", "AppendManyWithNamedTags", false},
	{"many routed", "AppendManyForEventSources", false},
	{"many metadata", "AppendMany", false},
	{"batch", "AppendManyForEventSources", false},
	{"batch named", "AppendManyForEventSourcesWithNamedTags", false},
	{"batch metadata", "AppendManyForEventSources", false},
	{"prepared", "AppendManyForEventSources", false},
	{"unit commit", "AppendManyForEventSources", false},
}

func notificationAppend(ctx context.Context, sequence *eventsequences.Sequence, path string) (eventsequences.BatchResult, error) {
	unchecked := eventsequences.Scope{Expectation: eventsequences.NoCheck()}
	options := []eventsequences.AppendOption{eventsequences.WithScope(unchecked)}
	batchOptions := []eventsequences.BatchOption{eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: unchecked}, eventsequences.LabeledScope{Label: "B", Scope: unchecked})}
	values := []any{opened{}, changed{}, opened{}}
	entries := []eventsequences.Entry{{Source: "A", Event: values[0]}, {Source: "B", Event: values[1]}, {Source: "A", Event: values[2]}}
	switch path {
	case "single", "single named", "single metadata":
		if path == "single named" {
			options = append(options, eventsequences.WithNamedTags(events.NamedTag{Name: "tag", Value: "value"}))
		}
		var result eventsequences.AppendResult
		var err error
		if path == "single metadata" {
			wrapped, failure := sequence.AppendWithMetadata(ctx, "A", opened{}, options...)
			result, err = wrapped.Result(), failure
		} else {
			result, err = sequence.Append(ctx, "A", opened{}, options...)
		}
		batch := eventsequences.BatchResult{Disposition: result.Disposition, CorrelationID: result.CorrelationID,
			ConstraintViolations: result.ConstraintViolations, ConcurrencyViolations: result.ConcurrencyViolations,
			Errors: result.Errors, ConcurrencyCheckPerformed: result.ConcurrencyCheckPerformed, Target: result.Target}
		if result.Position != nil {
			batch.Positions = []events.SequenceNumber{*result.Position}
		}
		return batch, err
	case "many", "many named", "many routed", "many metadata":
		if path == "many named" {
			options = append(options, eventsequences.WithNamedTags(events.NamedTag{Name: "tag"}))
		}
		if path == "many routed" {
			options = append(options, eventsequences.WithRoute(eventsequences.Route{SourceType: "Customer", StreamType: "Audit", StreamID: "audit"}))
		}
		if path == "many metadata" {
			wrapped, err := sequence.AppendManyWithMetadata(ctx, "A", values, options...)
			return wrapped.Result(), err
		}
		return sequence.AppendMany(ctx, "A", values, options...)
	case "batch named":
		batchOptions = append(batchOptions, eventsequences.WithBatchNamedTags(events.NamedTag{Name: "tag"}))
	case "batch metadata":
		wrapped, err := sequence.AppendBatchWithMetadata(ctx, entries, batchOptions...)
		return wrapped.Result(), err
	case "prepared":
		snapshot, err := sequence.PrepareBatch(ctx, entries, batchOptions...)
		if err != nil {
			return eventsequences.BatchResult{}, err
		}
		return sequence.AppendPreparedBatch(ctx, snapshot)
	case "unit commit":
		unit, owner, err := transactions.Begin(ctx, sequence)
		if err != nil {
			return eventsequences.BatchResult{}, err
		}
		if err = unit.Stage(ctx, entries, eventsequences.LabeledScope{Label: "A", Scope: unchecked}, eventsequences.LabeledScope{Label: "B", Scope: unchecked}); err != nil {
			return eventsequences.BatchResult{}, err
		}
		return owner.Commit(ctx)
	}
	return sequence.AppendBatch(ctx, entries, batchOptions...)
}

func notificationResponse(single bool, outcome string, correlation metadata.CorrelationID) (any, error) {
	if outcome == "transport" {
		return nil, status.Error(codes.Unavailable, "acknowledgment lost")
	}
	response := batchSuccess(correlation, true, 3)
	if single {
		response.Response.SequenceNumbers = []uint64{0}
	}
	switch outcome {
	case "constraints", "constraints and errors":
		response.Response = &sequences.AppendManyResponse{CorrelationId: wire.Guid(correlation), HasConstraintViolations: true,
			ConstraintViolations: []*sequences.ConstraintViolation{{EventTypeId: "opened", ConstraintType: sequences.ConstraintType_Unique, ConstraintName: "name", Message: "taken", Details: map[string]string{"PropertyValue": "Ada"}}}}
		if outcome == "constraints and errors" {
			response.Response.HasErrors = true
			response.Response.Errors = []string{"FutureCode"}
		}
	case "concurrency":
		response.Response = &sequences.AppendManyResponse{CorrelationId: wire.Guid(correlation), HasConcurrencyViolations: true,
			ConcurrencyViolations: []*sequences.ConcurrencyViolation{{EventSourceId: "A", ExpectedSequenceNumber: 4, ActualSequenceNumber: 5}}}
	case "errors only":
		response.Response = &sequences.AppendManyResponse{CorrelationId: wire.Guid(correlation), HasErrors: true, Errors: []string{"FutureCode"}}
	case "malformed":
		response.Response = nil
	case "unauthorized":
		response = &sequences.CommandResult_AppendManyResponse{CorrelationId: wire.Guid(correlation)}
	}
	if !single {
		return response, nil
	}
	one := &sequences.CommandResult_AppendResponse{IsAuthorized: response.IsAuthorized, CorrelationId: response.CorrelationId}
	if r := response.Response; r != nil {
		one.Response = &sequences.AppendResponse{CorrelationId: r.CorrelationId, IsSuccess: r.IsSuccess, ConcurrencyCheckPerformed: r.ConcurrencyCheckPerformed,
			HasConstraintViolations: r.HasConstraintViolations, ConstraintViolations: r.ConstraintViolations, HasConcurrencyViolations: r.HasConcurrencyViolations,
			HasErrors: r.HasErrors, Errors: r.Errors}
		if len(r.ConcurrencyViolations) != 0 {
			one.Response.ConcurrencyViolation = r.ConcurrencyViolations[0]
		}
	}
	return one, nil
}

func TestAppendNotificationsAcrossPathsAndDispositions(t *testing.T) {
	for _, path := range notificationPaths {
		t.Run(path.name, func(t *testing.T) {
			for _, outcome := range []string{"committed", "constraints", "constraints and errors", "concurrency", "errors only", "transport", "malformed", "unauthorized"} {
				t.Run(outcome, func(t *testing.T) {
					id, err := metadata.NewCorrelationID()
					if err != nil {
						t.Fatal(err)
					}
					sequence, calls := sequenceFixture(t, map[string]rpcHandler{path.rpc: func(context.Context, any) (any, error) {
						return notificationResponse(path.single, outcome, id)
					}})
					var received []eventsequences.AppendNotification
					defer sequence.OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })()
					result, operationErr := notificationAppend(metadata.WithCorrelation(testContext(t), id), sequence, path.name)
					if len(received) != 1 || calls.Load() != 1 {
						t.Fatalf("notifications=%d RPCs=%d", len(received), calls.Load())
					}
					n := received[0]
					want := eventsequences.Unknown
					switch outcome {
					case "committed":
						want = eventsequences.Committed
					case "constraints", "constraints and errors", "concurrency", "unauthorized":
						want = eventsequences.Rejected
					}
					// The unit fills missing response correlation in its retained result;
					// notifications preserve the actual append result before completion.
					if path.name == "unit commit" && n.Result.CorrelationID == (metadata.CorrelationID{}) {
						result.CorrelationID = metadata.CorrelationID{}
					}
					if n.Result.Disposition != want || !reflect.DeepEqual(n.Result, result) || n.Err != operationErr {
						t.Fatalf("notification=%+v result=%+v err=%v", n, result, operationErr)
					}
					if n.CorrelationID != id || n.Operation.Store() != "store" || n.Operation.Namespace() != "tenant" || n.Operation.Sequence() != "event-log" {
						t.Fatalf("lost attempted coordinates: %+v", n)
					}
					count := 3
					if path.single {
						count = 1
					}
					if len(n.Events) != count {
						t.Fatalf("events=%+v", n.Events)
					}
					for i, event := range n.Events {
						ref := events.TypeRef{ID: "opened", Generation: 1}
						source := events.SourceID("A")
						if i == 1 {
							ref.ID = "changed"
							if path.rpc != "AppendMany" && path.rpc != "AppendManyWithNamedTags" && path.name != "many routed" {
								source = "B"
							}
						}
						if event.EventType != ref || event.Source != source || (event.Position != nil) != (want == eventsequences.Committed) {
							t.Fatalf("input %d: %+v", i, event)
						}
						if event.Position != nil && *event.Position != events.SequenceNumber(i) {
							t.Fatalf("position=%v input=%d", *event.Position, i)
						}
						wantRoute := eventsequences.Route{SourceType: "Default", StreamType: "All", StreamID: "Default"}
						if path.name == "many routed" {
							wantRoute = eventsequences.Route{SourceType: "Customer", StreamType: "Audit", StreamID: "audit"}
						}
						if event.Route != wantRoute {
							t.Fatalf("route=%+v want=%+v", event.Route, wantRoute)
						}
					}
					var unknown *eventsequences.OutcomeUnknownError
					if errors.As(n.Err, &unknown) != (want == eventsequences.Unknown) {
						t.Fatalf("unknown error not retained: %v", n.Err)
					}
				})
			}
		})
	}
}

func TestAppendNotificationsKeepRequestCorrelationAndGeneration(t *testing.T) {
	definition, err := events.Define[opened](events.WithID("opened"), events.WithGeneration(3))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, generated := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit option overrides context", true: "generated"}[generated], func(t *testing.T) {
			var requestID metadata.CorrelationID
			sequence, _ := parityFixture(t, map[string]rpcHandler{"Append": func(_ context.Context, raw any) (any, error) {
				requestID = wire.Correlation(raw.(*sequences.AppendRequest).CorrelationId)
				return notificationResponse(true, "committed", metadata.CorrelationID{})
			}}, catalog, eventsequences.ConcurrencyPolicy{})
			var n eventsequences.AppendNotification
			defer sequence.OnAppend(func(notification eventsequences.AppendNotification) { n = notification })()
			options := []eventsequences.AppendOption{eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})}
			ctx := testContext(t)
			var override metadata.CorrelationID
			if !generated {
				contextID, err := metadata.NewCorrelationID()
				if err != nil {
					t.Fatal(err)
				}
				ctx = metadata.WithCorrelation(ctx, contextID)
				override, err = metadata.NewCorrelationID()
				if err != nil {
					t.Fatal(err)
				}
				options = append(options, eventsequences.WithCorrelation(override))
			}
			if _, err := sequence.Append(ctx, "A", opened{}, options...); err != nil {
				t.Fatal(err)
			}
			if n.CorrelationID == (metadata.CorrelationID{}) || n.CorrelationID != requestID || (!generated && n.CorrelationID != override) || n.Result.CorrelationID != (metadata.CorrelationID{}) {
				t.Fatalf("request correlation lost or response changed: %+v", n)
			}
			if n.Events[0].EventType.Generation != 3 || !reflect.DeepEqual(n.Operation.EventTypes(), []events.TypeRef{{ID: "opened", Generation: 3}}) {
				t.Fatal(n)
			}
		})
	}
}
