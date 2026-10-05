// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestAppendScopesOnTheWire(t *testing.T) {
	source := events.SourceID("source")
	for _, test := range []struct {
		name                       string
		scope                      *eventsequences.Scope
		tail, expected             uint64
		noMatch, checked, resolves bool
	}{
		{"default empty unchecked", nil, ^uint64(0), ^uint64(0), false, false, true},
		{"default existing optimistic", nil, 5, 5, false, true, true},
		{"exact position zero", &eventsequences.Scope{Expectation: eventsequences.Exact(0), Filter: eventsequences.ScopeFilter{SourceID: &source}}, 0, 0, false, true, false},
		{"protected empty", &eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent(), Filter: eventsequences.ScopeFilter{SourceID: &source}}, 0, ^uint64(0), true, true, false},
		{"explicit unchecked", &eventsequences.Scope{Expectation: eventsequences.NoCheck()}, 0, ^uint64(0), false, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := make(chan *sequences.AppendRequest, 1)
			tails := make(chan *sequences.TailSequenceNumberRequest, 1)
			kernel := &fakeKernel{
				append: func(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
					requests <- proto.Clone(request).(*sequences.AppendRequest)
					return success(request, 0), nil
				},
				tail: func(_ context.Context, request *sequences.TailSequenceNumberRequest) (*sequences.QueryResult_EventSequenceTailResponse, error) {
					tails <- request
					return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: test.tail}}, nil
				},
			}
			client, _ := testClient(t, kernel)
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "customers", chronicle.WithNamespace("tenant"))
			if err != nil {
				t.Fatal(err)
			}
			var options []eventsequences.AppendOption
			if test.scope != nil {
				options = append(options, eventsequences.WithScope(*test.scope))
			}
			result, err := store.EventLog().Append(ctx, source, &CustomerRegistered{Name: "Ada"}, options...)
			if err != nil || result.Err() != nil || result.Position == nil || *result.Position != 0 || result.ConcurrencyCheckPerformed != test.checked {
				t.Fatalf("result = %+v, error = %v", result, err)
			}
			request := <-requests
			if request.Content != `{"name":"Ada"}` || request.EventStore != "customers" || request.Namespace != "tenant" || request.EventSequenceId != "event-log" || request.Subject != "source" {
				t.Fatalf("request = %v", request)
			}
			if request.EventSourceType != "Default" || request.EventStreamType != "All" || request.EventStreamId != "Default" {
				t.Fatal("wrong default route")
			}
			if request.ConcurrencyScope.SequenceNumber != test.expected || request.ConcurrencyScope.ExpectsNoMatchingEvent != test.noMatch {
				t.Fatal(request.ConcurrencyScope)
			}
			if request.Occurred == nil || request.Occurred.Value != "" {
				t.Fatal("missing occurrence must be empty wrapper")
			}
			if result.Target.First == nil || *result.Target.First != 0 || result.Target.EventTypeTails["CustomerRegistered"] != 0 {
				t.Fatal("completion target missing")
			}
			if test.resolves {
				tail := <-tails
				if tail.EventSourceId != "source" || tail.EventSourceType != request.ConcurrencyScope.EventSourceType || tail.EventStreamId != request.ConcurrencyScope.EventStreamId || tail.EventStreamType != request.ConcurrencyScope.EventStreamType {
					t.Fatal("tail narrowing differs from scope")
				}
			} else if len(tails) != 0 {
				t.Fatal("unexpected tail query")
			}
		})
	}
}

func TestAppendMetadataAndNamedTags(t *testing.T) {
	requests := make(chan *sequences.AppendWithNamedTagsRequest, 1)
	kernel := &fakeKernel{named: func(_ context.Context, request *sequences.AppendWithNamedTagsRequest) (*sequences.CommandResult_AppendResponse, error) {
		requests <- request
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, SequenceNumber: 9, CorrelationId: request.CorrelationId}}, nil
	}}
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CustomerRegistered](registry, events.WithTags("static")); err != nil {
		t.Fatal(err)
	}
	client, _ := testClient(t, kernel, chronicle.WithRegistry(registry))
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	id, err := metadata.ParseCorrelationID("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	occurred := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	ctx = metadata.WithCorrelation(ctx, id)
	ctx = metadata.WithIdentity(ctx, identities.Identity{Subject: "service", OnBehalfOf: &identities.Identity{Subject: "user"}})
	ctx = metadata.WithCausation(ctx, metadata.Causation{Type: "command", Occurred: occurred, Properties: map[string]string{"name": "RegisterCustomer"}})
	result, err := store.EventLog().Append(ctx, "source", CustomerRegistered{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}), eventsequences.WithOccurred(occurred), eventsequences.WithSubject("person"), eventsequences.WithTags("static", "dynamic"), eventsequences.WithNamedTags(events.NamedTag{Name: "opaque", Value: ""}, events.NamedTag{Name: "opaque", Value: ""}), eventsequences.WithRoute(eventsequences.Route{SourceType: "Customer", StreamType: "Audit", StreamID: "audit"}))
	if err != nil || result.CorrelationID != id {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	request := <-requests
	if request.CausedBy.OnBehalfOf.Subject != "user" || len(request.Causation) != 1 || request.Causation[0].Properties["name"] != "RegisterCustomer" || request.Subject != "person" {
		t.Fatal("audit metadata lost")
	}
	if request.Occurred.Value != "2026-01-02T03:04:05.1234567+00:00" || len(request.Tags) != 2 || len(request.NamedTags) != 1 || request.NamedTags[0].Value != "" || request.EventStreamId != "audit" {
		t.Fatal("append options lost")
	}
}

func TestAppendDomainRejectionsPreserveEveryDiagnostic(t *testing.T) {
	kernel := &fakeKernel{append: func(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{CorrelationId: request.CorrelationId, HasConstraintViolations: true, HasConcurrencyViolations: true, HasErrors: true, ConcurrencyCheckPerformed: true,
			ConstraintViolations: []*sequences.ConstraintViolation{{ConstraintName: "unique-email", ConstraintType: sequences.ConstraintType_Unique, EventTypeId: "CustomerRegistered", SequenceNumber: 5, Message: "duplicate", Details: map[string]string{"email": "taken"}}},
			ConcurrencyViolation: &sequences.ConcurrencyViolation{EventSourceId: "source", ExpectedSequenceNumber: 4, ActualSequenceNumber: 5}, Errors: []string{"FutureKernelCode"}}}, nil
	}}
	client, _ := testClient(t, kernel)
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.EventLog().Append(ctx, "source", CustomerRegistered{})
	if err != nil || result.Disposition != eventsequences.Rejected || result.Position != nil {
		t.Fatalf("result = %+v err = %v", result, err)
	}
	var constraint *eventsequences.ConstraintError
	var concurrency *eventsequences.ConcurrencyError
	if !errors.As(result.Err(), &constraint) || !errors.As(result.Err(), &concurrency) || constraint.Violations[0].Details["email"] != "taken" || concurrency.Violations[0].Actual != 5 || result.Errors[0] != "FutureKernelCode" {
		t.Fatal("rejection detail lost")
	}
}

func TestAppendNeverRetriesAnAmbiguousWrite(t *testing.T) {
	kernel := &fakeKernel{append: func(context.Context, *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		return nil, status.Error(codes.Unavailable, "response lost after commit")
	}}
	client, _ := testClient(t, kernel)
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.EventLog().Append(ctx, "source", CustomerRegistered{})
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(err, &unknown) || status.Code(err) != codes.Unavailable || result.Disposition != eventsequences.Unknown || kernel.appendCalls.Load() != 1 {
		t.Fatalf("result = %+v err = %v calls = %d", result, err, kernel.appendCalls.Load())
	}
}
