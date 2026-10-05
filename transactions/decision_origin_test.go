// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDecisionCompletionUsesUnitOriginResolvedBeforeLease(t *testing.T) {
	for _, disposition := range []eventsequences.Disposition{eventsequences.Committed, eventsequences.Rejected, eventsequences.Unknown} {
		t.Run(map[eventsequences.Disposition]string{eventsequences.Committed: "committed", eventsequences.Rejected: "rejected", eventsequences.Unknown: "unknown"}[disposition], func(t *testing.T) {
			f := decisionSequence(t)
			callerOrigin, leaseOrigin := eventsequences.NewOrigin(), eventsequences.NewOrigin()
			f.leaseOrigin = &leaseOrigin
			ctx := eventsequences.WithOrigin(t.Context(), callerOrigin)
			unit, owner := begin(t, ctx, f.sequence)
			if err := unit.Enroll(f.token(5, "changed")); err != nil {
				t.Fatal(err)
			}
			if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "source", Event: changed{Value: "effect"}}}); err != nil {
				t.Fatal(err)
			}
			f.handle = func(any) (*sequences.CommandResult_AppendManyResponse, error) {
				switch disposition {
				case eventsequences.Rejected:
					return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{
						ConcurrencyCheckPerformed: true, HasConcurrencyViolations: true,
						ConcurrencyViolations: []*sequences.ConcurrencyViolation{{EventSourceId: "source", ExpectedSequenceNumber: 5, ActualSequenceNumber: 6}},
					}}, nil
				case eventsequences.Unknown:
					return nil, status.Error(codes.Unavailable, "acknowledgement lost")
				default:
					response := success(1)
					response.Response.ConcurrencyCheckPerformed = true
					return response, nil
				}
			}
			var notifications []eventsequences.AppendNotification
			defer f.sequence.OnAppend(func(n eventsequences.AppendNotification) { notifications = append(notifications, n) })()
			result, err := owner.Commit(ctx)
			if disposition == eventsequences.Unknown {
				var unknown *eventsequences.OutcomeUnknownError
				if !errors.As(err, &unknown) {
					t.Fatalf("lost unknown outcome: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if result.Disposition != disposition || len(f.requests) != 1 || len(notifications) != 1 {
				t.Fatalf("completion: %+v %v notifications=%d", result, err, len(notifications))
			}
			n := notifications[0]
			if n.Origin != unit.Origin() || n.Origin == callerOrigin || n.Origin == leaseOrigin || n.Result.Disposition != disposition || len(n.Events) != 1 {
				t.Fatalf("decision attribution was re-inferred: %+v", n)
			}
		})
	}
}
