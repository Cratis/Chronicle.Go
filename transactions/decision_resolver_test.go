// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/decision"
)

func TestGuardedOwnerCommitBypassesExternalOriginResolver(t *testing.T) {
	for _, failure := range []string{"error", "panic"} {
		for _, mode := range []string{"plain", "named", "eventless"} {
			t.Run(failure+"/"+mode, func(t *testing.T) {
				var resolutions int
				f := decisionSequence(t, func(context.Context) (eventsequences.Origin, bool, error) {
					resolutions++
					if failure == "panic" {
						panic("owner must bypass resolution")
					}
					return eventsequences.Origin{}, false, errors.New("owner must bypass resolution")
				})
				inherited, commitOrigin, leaseOrigin := eventsequences.NewOrigin(), eventsequences.NewOrigin(), eventsequences.NewOrigin()
				f.leaseOrigin = &leaseOrigin
				ctx := eventsequences.WithOrigin(t.Context(), inherited)
				unit, owner := begin(t, ctx, f.sequence)
				var entries []eventsequences.Entry
				if mode != "eventless" {
					entries = []eventsequences.Entry{{Source: "source", Event: changed{Value: "effect"}}}
					if mode == "named" {
						entries[0].NamedTags = []events.NamedTag{{Name: "kind", Value: "decision"}}
					}
					if err := unit.Stage(ctx, entries); err != nil {
						t.Fatal(err)
					}
				}
				if err := unit.Enroll(f.token(5, "changed")); err != nil {
					t.Fatal(err)
				}
				// Both enrollment and later staging replace the snapshot. Completion
				// must authorize the final pointer, not either earlier snapshot.
				if err := unit.Stage(ctx, nil); err != nil {
					t.Fatal(err)
				}
				f.handle = func(request any) (*sequences.CommandResult_AppendManyResponse, error) {
					_, named := request.(*sequences.AppendManyForEventSourcesWithNamedTagsRequest)
					if named != (mode == "named") {
						t.Errorf("wrong append variant: %T", request)
					}
					response := success(len(entries))
					response.Response.ConcurrencyCheckPerformed = true
					return response, nil
				}
				var notifications []eventsequences.AppendNotification
				defer f.sequence.OnAppend(func(n eventsequences.AppendNotification) { notifications = append(notifications, n) })()
				result, err := owner.Commit(eventsequences.WithOrigin(ctx, commitOrigin))
				if err != nil || result.Disposition != eventsequences.Committed || resolutions != 0 || f.acquired != 1 || len(f.requests) != 1 || len(notifications) != len(entries) {
					t.Fatalf("result=%+v err=%v resolutions=%d leases=%d RPCs=%d notifications=%d", result, err, resolutions, f.acquired, len(f.requests), len(notifications))
				}
				for _, n := range notifications {
					if n.Origin != unit.Origin() || n.Origin == inherited || n.Origin == commitOrigin || n.Origin == leaseOrigin {
						t.Fatalf("owner attribution lost: %+v", n)
					}
				}
			})
		}
	}
}

func TestGuardedPreparedAppendResolvesOriginBeforeDecisionLease(t *testing.T) {
	for _, outcome := range []string{"resolved", "error", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			var resolutions int
			resolved, inherited, leaseOrigin := eventsequences.NewOrigin(), eventsequences.NewOrigin(), eventsequences.NewOrigin()
			ctx := eventsequences.WithOrigin(t.Context(), inherited)
			f := decisionSequence(t, func(actual context.Context) (eventsequences.Origin, bool, error) {
				resolutions++
				if actual != ctx {
					t.Error("resolver did not receive the public append context")
				}
				switch outcome {
				case "error":
					return eventsequences.Origin{}, false, errors.New("private resolver failure")
				case "panic":
					panic("private resolver failure")
				default:
					return resolved, true, nil
				}
			})
			f.leaseOrigin = &leaseOrigin
			batch, err := f.sequence.PrepareBatch(ctx, []eventsequences.Entry{{Source: "source", Event: changed{Value: "effect"}}})
			if err != nil {
				t.Fatal(err)
			}
			// Test-only internal issuance: no application-facing guard constructor.
			if err = decision.Enroll(f.token(5, "changed"), f.target, f, func(guard *decision.Guard) error {
				batch, err = batch.WithDecisionGuard(guard)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			f.handle = func(any) (*sequences.CommandResult_AppendManyResponse, error) {
				response := success(1)
				response.Response.ConcurrencyCheckPerformed = true
				return response, nil
			}
			var notifications []eventsequences.AppendNotification
			defer f.sequence.OnAppend(func(n eventsequences.AppendNotification) { notifications = append(notifications, n) })()
			result, err := f.sequence.AppendPreparedBatch(ctx, batch)
			if resolutions != 1 {
				t.Fatalf("resolutions=%d", resolutions)
			}
			if outcome == "resolved" {
				if err != nil || result.Disposition != eventsequences.Committed || f.acquired != 1 || len(f.requests) != 1 || len(notifications) != 1 || notifications[0].Origin != resolved {
					t.Fatalf("result=%+v err=%v leases=%d notifications=%+v", result, err, f.acquired, notifications)
				}
				return
			}
			var failure *eventsequences.AppendOriginResolutionError
			if !errors.As(err, &failure) || failure.Panicked != (outcome == "panic") || result.Disposition != eventsequences.Rejected || f.acquired != 0 || len(f.requests) != 0 || len(notifications) != 0 {
				t.Fatalf("preflight: result=%+v err=%v leases=%d RPCs=%d notifications=%d", result, err, f.acquired, len(f.requests), len(notifications))
			}
		})
	}
}
