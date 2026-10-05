// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestConstraintMessageProviderSkipsFailedEnvelopesAndMayCloseClient(t *testing.T) {
	for _, authorized := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed envelope", true: "provider closes client"}[authorized], func(t *testing.T) {
			registry := chronicle.NewRegistry()
			event, err := chronicle.RegisterEvent[CustomerRegistered](registry)
			if err != nil {
				t.Fatal(err)
			}
			var client *chronicle.Client
			var calls atomic.Int32
			definition, err := constraints.UniqueEventTypes(event.Descriptor()).WithMessageProvider(func(constraints.Violation) string {
				calls.Add(1)
				if closeErr := client.Close(); closeErr != nil {
					t.Error(closeErr)
				}
				return "resolved"
			}).Build()
			if err != nil {
				t.Fatal(err)
			}
			if err = registry.AddConstraint(definition); err != nil {
				t.Fatal(err)
			}
			kernel := &fakeKernel{append: func(context.Context, *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
				return &sequences.CommandResult_AppendResponse{IsAuthorized: authorized, Response: &sequences.AppendResponse{HasConstraintViolations: true, ConstraintViolations: []*sequences.ConstraintViolation{{ConstraintName: "CustomerRegistered", Message: "kernel message"}}}}, nil
			}}
			client, _ = testClient(t, kernel, chronicle.WithRegistry(registry))
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.EventLog().Append(ctx, "source", CustomerRegistered{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
			if authorized {
				if err != nil || calls.Load() != 1 || result.Disposition != eventsequences.Rejected || result.ConstraintViolations[0].Message != "resolved" {
					t.Fatalf("message resolution: %+v %v", result, err)
				}
			} else {
				var envelope *chronicle.EnvelopeError
				if !errors.As(err, &envelope) || calls.Load() != 0 {
					t.Fatalf("failed envelope invoked provider: %v, calls %d", err, calls.Load())
				}
			}
		})
	}
}
