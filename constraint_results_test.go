// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestConstraintDetailsAndMessagesInEveryAppendResult(t *testing.T) {
	for _, operation := range []string{"Append", "AppendMany", "AppendBatch", "routed AppendMany", "named Append"} {
		t.Run(operation, func(t *testing.T) {
			registry := chronicle.NewRegistry()
			event, err := chronicle.RegisterEvent[CustomerRegistered](registry)
			if err != nil {
				t.Fatal(err)
			}
			definition, err := constraints.UniqueValues("unique-name").On(event.Descriptor(), "name").WithMessage("{PropertyName} '{PropertyValue}' is taken").Build()
			if err != nil {
				t.Fatal(err)
			}
			if err = registry.AddConstraint(definition); err != nil {
				t.Fatal(err)
			}
			violations := func() []*sequences.ConstraintViolation {
				return []*sequences.ConstraintViolation{
					{EventTypeId: "CustomerRegistered", SequenceNumber: 17, ConstraintType: sequences.ConstraintType_Unique, ConstraintName: "unique-name", Message: "kernel message", Details: map[string]string{constraints.PropertyName: "name", constraints.PropertyValue: "Ada", "FutureDetail": "retained"}},
					{EventTypeId: "CustomerRegistered", SequenceNumber: uint64(events.Unavailable), ConstraintType: sequences.ConstraintType_Schema, ConstraintName: "built-in-schema", Message: "keep this diagnostic", Details: map[string]string{"Path": "name"}},
				}
			}
			single := func() *sequences.CommandResult_AppendResponse {
				return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{HasConstraintViolations: true, ConstraintViolations: violations()}}
			}
			batch := func() *sequences.CommandResult_AppendManyResponse {
				return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{HasConstraintViolations: true, ConstraintViolations: violations()}}
			}
			kernel := &fakeKernel{
				append: func(context.Context, *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
					return single(), nil
				},
				named: func(context.Context, *sequences.AppendWithNamedTagsRequest) (*sequences.CommandResult_AppendResponse, error) {
					return single(), nil
				},
				appendMany: func(*sequences.AppendManyRequest) *sequences.CommandResult_AppendManyResponse { return batch() },
				appendBatch: func(*sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
					return batch()
				},
			}
			client, _ := testClient(t, kernel, chronicle.WithRegistry(registry))
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Fatal(err)
			}
			var details []constraints.Violation
			var disposition eventsequences.Disposition
			var rejection error
			unchecked := eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})
			if operation == "Append" || operation == "named Append" {
				options := []eventsequences.AppendOption{unchecked}
				if operation == "named Append" {
					options = append(options, eventsequences.WithNamedTags(events.NamedTag{Name: "name", Value: "value"}))
				}
				result, callErr := store.EventLog().Append(ctx, "source", CustomerRegistered{Name: "Ada"}, options...)
				err, details, disposition, rejection = callErr, result.ConstraintViolations, result.Disposition, result.Err()
				if result.Position != nil {
					t.Fatal("rejection returned a committed position")
				}
			} else {
				var result eventsequences.BatchResult
				if operation == "AppendBatch" {
					result, err = store.EventLog().AppendBatch(ctx, []eventsequences.Entry{{Source: "source", Event: CustomerRegistered{Name: "Ada"}}})
				} else {
					options := []eventsequences.AppendOption{unchecked}
					if operation == "routed AppendMany" {
						options = append(options, eventsequences.WithRoute(eventsequences.Route{StreamID: "routed"}))
					}
					result, err = store.EventLog().AppendMany(ctx, "source", []any{CustomerRegistered{Name: "Ada"}}, options...)
				}
				details, disposition, rejection = result.ConstraintViolations, result.Disposition, result.Err()
				if len(result.Positions) != 0 {
					t.Fatal("rejection returned committed positions")
				}
			}
			var constraintError *eventsequences.ConstraintError
			if err != nil || disposition != eventsequences.Rejected || !errors.As(rejection, &constraintError) {
				t.Fatalf("lost known rejection: %v, %v", err, rejection)
			}
			want := []constraints.Violation{
				{EventTypeID: "CustomerRegistered", SequenceNumber: 17, Type: constraints.Unique, ConstraintName: "unique-name", Message: "name 'Ada' is taken", Details: map[string]string{constraints.PropertyName: "name", constraints.PropertyValue: "Ada", "FutureDetail": "retained"}},
				{EventTypeID: "CustomerRegistered", SequenceNumber: events.Unavailable, Type: constraints.Schema, ConstraintName: "built-in-schema", Message: "keep this diagnostic", Details: map[string]string{"Path": "name"}},
			}
			if !reflect.DeepEqual(details, want) || !reflect.DeepEqual(constraintError.Violations, want) {
				t.Fatalf("details: %+v, want %+v", details, want)
			}
		})
	}
}
