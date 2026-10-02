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
)

type personChanged struct{ Person string }

func parityCatalog(t *testing.T) *events.Catalog {
	t.Helper()
	definition, err := events.Define[personChanged](events.WithGeneration(3), events.WithSubjectResolver(func(event personChanged) (events.Subject, bool) {
		return events.Subject(event.Person), event.Person != ""
	}))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestFirstAppendPolicyAndSubjectsAcrossAllPaths(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, path := range []string{"single", "many", "batch", "unit"} {
			for _, subjectCase := range []string{"resolved", "override", "absent"} {
				t.Run(path+"/"+subjectCase+"/"+map[bool]string{false: "default", true: "protected"}[enabled], func(t *testing.T) {
					value := &personChanged{Person: "person"}
					var override *events.Subject
					wantSubject := "person"
					if subjectCase == "override" {
						subject := events.Subject("override")
						override = &subject
						wantSubject = string(subject)
					}
					if subjectCase == "absent" {
						value.Person = ""
						wantSubject = "source"
					}
					assertScope := func(scope *sequences.ConcurrencyScope) {
						if scope.ExpectsNoMatchingEvent != enabled || scope.SequenceNumber != uint64(events.Unavailable) || !scope.EventSourceId {
							t.Errorf("scope = %v", scope)
						}
					}
					handlers := map[string]rpcHandler{
						"TailSequenceNumber": func(context.Context, any) (any, error) { return tailResponse(uint64(events.Unavailable)), nil },
						"Append": func(_ context.Context, raw any) (any, error) {
							r := raw.(*sequences.AppendRequest)
							assertScope(r.ConcurrencyScope)
							if r.Subject != wantSubject {
								t.Errorf("subject = %q, want %q", r.Subject, wantSubject)
							}
							return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, ConcurrencyCheckPerformed: enabled}}, nil
						},
						"AppendMany": func(_ context.Context, raw any) (any, error) {
							r := raw.(*sequences.AppendManyRequest)
							assertScope(r.ConcurrencyScope)
							if r.Events[0].Subject != wantSubject {
								t.Errorf("subject = %q", r.Events[0].Subject)
							}
							return batchSuccess(wire.Correlation(r.CorrelationId), enabled, len(r.Events)), nil
						},
						"AppendManyForEventSources": func(_ context.Context, raw any) (any, error) {
							r := raw.(*sequences.AppendManyForEventSourcesRequest)
							assertScope(r.ConcurrencyScopes[0].Scope)
							if r.Events[0].Subject != wantSubject {
								t.Errorf("subject = %q", r.Events[0].Subject)
							}
							return batchSuccess(wire.Correlation(r.CorrelationId), enabled, len(r.Events)), nil
						},
					}
					sequence, calls := parityFixture(t, handlers, parityCatalog(t), eventsequences.ConcurrencyPolicy{CheckFirstAppendIntoAScope: enabled})
					ctx := testContext(t)
					var options []eventsequences.AppendOption
					if override != nil {
						options = append(options, eventsequences.WithSubject(*override))
					}
					entry := eventsequences.Entry{Source: "source", Event: value, Subject: override}
					var err error
					switch path {
					case "single":
						_, err = sequence.Append(ctx, "source", value, options...)
					case "many":
						_, err = sequence.AppendMany(ctx, "source", []any{value}, options...)
					case "batch":
						_, err = sequence.AppendBatch(ctx, []eventsequences.Entry{entry})
					case "unit":
						unit, owner, beginErr := transactions.Begin(ctx, sequence)
						if beginErr != nil {
							t.Fatal(beginErr)
						}
						if err = unit.Stage(ctx, []eventsequences.Entry{entry}); err != nil {
							t.Fatal(err)
						}
						if calls.Load() != 0 {
							t.Fatal("staging performed I/O")
						}
						value.Person = "changed-after-stage"
						_, err = owner.Commit(ctx)
					}
					if err != nil {
						t.Fatal(err)
					}
					if calls.Load() != 2 {
						t.Fatalf("RPC calls = %d", calls.Load())
					}
				})
			}
		}
	}
}

type strategyFunc func(context.Context, *eventsequences.Sequence, eventsequences.ScopeFilter) (eventsequences.Scope, error)

func (f strategyFunc) GetScope(ctx context.Context, s *eventsequences.Sequence, filter eventsequences.ScopeFilter) (eventsequences.Scope, error) {
	return f(ctx, s, filter)
}

func TestDefaultStrategyReplacesAutomaticScopesAndExplicitScopesWin(t *testing.T) {
	for _, path := range []string{"single", "many", "batch", "unit"} {
		t.Run(path, func(t *testing.T) {
			strategyCalls := 0
			strategy := strategyFunc(func(ctx context.Context, _ *eventsequences.Sequence, filter eventsequences.ScopeFilter) (eventsequences.Scope, error) {
				if ctx.Err() != nil || filter.SourceID == nil || *filter.SourceID != "source" || filter.StreamID == nil || *filter.StreamID != events.DefaultStreamID {
					t.Errorf("strategy filter = %+v", filter)
				}
				strategyCalls++
				return eventsequences.Scope{Expectation: eventsequences.NoCheck()}, nil
			})
			sequence, calls := parityFixture(t, map[string]rpcHandler{
				"Append": func(context.Context, any) (any, error) {
					return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true}}, nil
				},
				"AppendMany":                func(context.Context, any) (any, error) { return batchSuccess(metadata.CorrelationID{}, false, 1), nil },
				"AppendManyForEventSources": func(context.Context, any) (any, error) { return batchSuccess(metadata.CorrelationID{}, false, 1), nil },
			}, parityCatalog(t), eventsequences.ConcurrencyPolicy{CheckFirstAppendIntoAScope: true, Strategy: strategy})
			ctx := testContext(t)
			for _, explicit := range []bool{false, true} {
				var options []eventsequences.AppendOption
				var scopes []eventsequences.LabeledScope
				if explicit {
					scope := eventsequences.Scope{Expectation: eventsequences.NoCheck()}
					options = append(options, eventsequences.WithScope(scope))
					scopes = append(scopes, eventsequences.LabeledScope{Label: "source", Scope: scope})
				}
				entries := []eventsequences.Entry{{Source: "source", Event: personChanged{}}}
				var err error
				switch path {
				case "single":
					_, err = sequence.Append(ctx, "source", personChanged{}, options...)
				case "many":
					_, err = sequence.AppendMany(ctx, "source", []any{personChanged{}}, options...)
				case "batch":
					_, err = sequence.AppendBatch(ctx, entries, eventsequences.WithScopes(scopes...))
				case "unit":
					unit, owner, e := transactions.Begin(ctx, sequence)
					if e != nil {
						t.Fatal(e)
					}
					if e = unit.Stage(ctx, entries, scopes...); e != nil {
						t.Fatal(e)
					}
					_, err = owner.Commit(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if strategyCalls != 1 || calls.Load() != 2 {
				t.Fatalf("strategy=%d RPC=%d", strategyCalls, calls.Load())
			}
		})
	}
}

func TestStrategyOwnsItsTailReadAndFailuresDoNotDispatch(t *testing.T) {
	failure := errors.New("strategy failed")
	for _, mode := range []string{"resolved", "error", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			strategy := strategyFunc(func(ctx context.Context, sequence *eventsequences.Sequence, filter eventsequences.ScopeFilter) (eventsequences.Scope, error) {
				if mode == "error" {
					return eventsequences.Scope{}, failure
				}
				if mode == "invalid" {
					other := events.SourceID("other")
					filter.SourceID = &other
					return eventsequences.Scope{Expectation: eventsequences.Exact(4), Filter: filter}, nil
				}
				tail, exists, err := sequence.Tail(ctx, eventsequences.TailFilter(filter))
				if err != nil {
					return eventsequences.Scope{}, err
				}
				if !exists {
					t.Error("expected a matching tail")
				}
				return eventsequences.Scope{Expectation: eventsequences.Exact(tail), Filter: filter}, nil
			})
			sequence, calls := parityFixture(t, map[string]rpcHandler{
				"TailSequenceNumber": func(context.Context, any) (any, error) { return tailResponse(4), nil },
				"Append": func(_ context.Context, raw any) (any, error) {
					if raw.(*sequences.AppendRequest).ConcurrencyScope.SequenceNumber != 4 {
						t.Error("strategy tail lost")
					}
					return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, SequenceNumber: 5, ConcurrencyCheckPerformed: true}}, nil
				},
			}, parityCatalog(t), eventsequences.ConcurrencyPolicy{Strategy: strategy})
			_, err := sequence.Append(testContext(t), "source", personChanged{})
			if mode == "resolved" {
				if err != nil || calls.Load() != 2 {
					t.Fatalf("error=%v calls=%d", err, calls.Load())
				}
			} else {
				if err == nil || calls.Load() != 0 {
					t.Fatalf("error=%v calls=%d", err, calls.Load())
				}
				if mode == "error" && !errors.Is(err, failure) {
					t.Fatalf("strategy error lost: %v", err)
				}
			}
		})
	}
}

func TestEmptyScopeDimensionsAreConsistentWithTailReads(t *testing.T) {
	source := events.SourceID("source")
	sourceType, streamType, streamID := events.SourceType(""), events.StreamType(""), events.StreamID("")
	filter := eventsequences.ScopeFilter{SourceID: &source, SourceType: &sourceType, StreamType: &streamType, StreamID: &streamID}
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{
		"TailSequenceNumber": func(_ context.Context, raw any) (any, error) {
			r := raw.(*sequences.TailSequenceNumberRequest)
			if r.EventSourceType != "" || r.EventStreamType != "" || r.EventStreamId != "" {
				t.Errorf("tail dimensions = %v", r)
			}
			return tailResponse(uint64(events.Unavailable)), nil
		},
		"Append": func(_ context.Context, raw any) (any, error) {
			r := raw.(*sequences.AppendRequest)
			if r.ConcurrencyScope.EventSourceType != "" || r.ConcurrencyScope.EventStreamType != "" || r.ConcurrencyScope.EventStreamId != "" {
				t.Errorf("scope = %v", r.ConcurrencyScope)
			}
			return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true}}, nil
		},
	})
	if _, err := sequence.Append(testContext(t), source, opened{}, eventsequences.WithScope(eventsequences.Scope{Filter: filter})); err != nil {
		t.Fatal(err)
	}
	if _, err := sequence.AppendBatch(testContext(t), []eventsequences.Entry{{Source: source, Event: opened{}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "source", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck(), Filter: filter}})); err == nil {
		t.Fatal("NoCheck with source narrowing accepted")
	}
}

func TestOperationMetadataSurvivesRejectionAndUnknownOutcome(t *testing.T) {
	for _, disposition := range []string{"committed", "rejected", "unknown"} {
		t.Run(disposition, func(t *testing.T) {
			sequence, _ := parityFixture(t, map[string]rpcHandler{
				"Append": func(context.Context, any) (any, error) {
					if disposition == "unknown" {
						return nil, errors.New("lost response")
					}
					response := &sequences.AppendResponse{IsSuccess: true}
					if disposition == "rejected" {
						response.IsSuccess = false
						response.HasConcurrencyViolations = true
						response.ConcurrencyViolation = &sequences.ConcurrencyViolation{EventSourceId: "source"}
					}
					return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: response}, nil
				},
				"AppendManyForEventSources": func(context.Context, any) (any, error) {
					if disposition == "unknown" {
						return nil, errors.New("lost response")
					}
					response := &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: []uint64{0, 1}}
					if disposition == "rejected" {
						response.IsSuccess = false
						response.SequenceNumbers = nil
						response.HasConcurrencyViolations = true
						response.ConcurrencyViolations = []*sequences.ConcurrencyViolation{{EventSourceId: "source"}}
					}
					return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: response}, nil
				},
			}, parityCatalog(t), eventsequences.ConcurrencyPolicy{})
			result, err := sequence.AppendWithMetadata(testContext(t), "source", personChanged{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
			if (err != nil) != (disposition == "unknown") {
				t.Fatalf("error = %v", err)
			}
			assertOperation(t, result.Operation())
			if disposition != "committed" && result.Result().Target.First != nil {
				t.Fatal("failed append has completion target")
			}
			batch, err := sequence.AppendBatchWithMetadata(testContext(t), []eventsequences.Entry{{Source: "source", Event: personChanged{}}, {Source: "source", Event: personChanged{}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "source", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}))
			if (err != nil) != (disposition == "unknown") {
				t.Fatalf("error = %v", err)
			}
			assertOperation(t, batch.Operation())
			if disposition != "committed" && batch.Result().Target.First != nil {
				t.Fatal("failed batch has completion target")
			}
		})
	}
}

func TestManyAndPreparedOperationMetadata(t *testing.T) {
	sequence, calls := parityFixture(t, map[string]rpcHandler{
		"AppendMany": func(_ context.Context, raw any) (any, error) {
			request := raw.(*sequences.AppendManyRequest)
			return batchSuccess(wire.Correlation(request.CorrelationId), false, len(request.Events)), nil
		},
	}, parityCatalog(t), eventsequences.ConcurrencyPolicy{})
	ctx := testContext(t)
	result, err := sequence.AppendManyWithMetadata(ctx, "source", []any{personChanged{}, personChanged{}}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil || result.Result().Disposition != eventsequences.Committed {
		t.Fatalf("many result = %+v, %v", result, err)
	}
	assertOperation(t, result.Operation())
	prepared, err := sequence.PrepareBatch(ctx, []eventsequences.Entry{{Source: "source", Event: personChanged{}}})
	if err != nil {
		t.Fatal(err)
	}
	assertOperation(t, prepared.Operation())
	if calls.Load() != 1 {
		t.Fatal("metadata performed I/O")
	}
}

func TestLegacyResultLayoutsRemainUnchanged(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[eventsequences.AppendResult](), reflect.TypeFor[eventsequences.BatchResult]()} {
		if typ.NumField() != 8 {
			t.Fatalf("%s layout changed: unkeyed literals would break", typ)
		}
		for i := range typ.NumField() {
			if !typ.Field(i).IsExported() {
				t.Fatalf("%s has a private field: external unkeyed literals would break", typ)
			}
		}
	}
}

func assertOperation(t *testing.T, operation eventsequences.OperationMetadata) {
	t.Helper()
	want := []events.TypeRef{{ID: "personChanged", Generation: 3}}
	if operation.Store() != "store" || operation.Namespace() != "tenant" || operation.Sequence() != events.EventLog || !reflect.DeepEqual(operation.EventTypes(), want) {
		t.Fatalf("operation = %+v", operation)
	}
	refs := operation.EventTypes()
	refs[0].Generation = 99
	if !reflect.DeepEqual(operation.EventTypes(), want) {
		t.Fatal("metadata aliases returned types")
	}
}
