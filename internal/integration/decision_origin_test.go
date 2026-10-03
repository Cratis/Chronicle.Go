//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/transactions"
)

func TestKernelDecisionOwnerOriginBypassesResolver(t *testing.T) {
	for _, failure := range []string{"error", "panic"} {
		for _, mode := range []string{"plain", "named", "eventless"} {
			t.Run(failure+"/"+mode, func(t *testing.T) {
				fixture := newKernelFixture(t)
				registry, model := decisionRegistry(t)
				var resolutions int
				client := fixture.client(registry, chronicle.WithAppendOriginResolver(func(context.Context) (eventsequences.Origin, bool, error) {
					resolutions++
					if failure == "panic" {
						panic("owner completion must not invoke external resolution")
					}
					return eventsequences.Origin{}, false, errors.New("owner completion must not invoke external resolution")
				}))
				store, err := client.EventStore(fixture.ctx, fixture.storeName)
				if err != nil {
					t.Fatal(err)
				}
				inherited, commitOrigin := eventsequences.NewOrigin(), eventsequences.NewOrigin()
				ctx := eventsequences.WithOrigin(fixture.ctx, inherited)
				unit, owner := decisionUnit(t, ctx, store)
				read, err := readmodels.DecisionsFor(store.ReadModels(), model).Get(transactions.WithUnitOfWork(ctx, unit), "source")
				if err != nil || read.Token.IsZero() || read.Instance.Exists {
					t.Fatalf("decision read: %+v %v", read, err)
				}
				var entries []eventsequences.Entry
				if mode != "eventless" {
					entries = []eventsequences.Entry{{Source: "source", Event: DecisionAccountChanged{Name: "committed"}}}
					if mode == "named" {
						entries[0].NamedTags = []events.NamedTag{{Name: "kind", Value: "decision"}}
					}
					if err = unit.Stage(ctx, entries); err != nil {
						t.Fatal(err)
					}
				}
				var notifications []eventsequences.AppendNotification
				defer store.EventLog().OnAppend(func(n eventsequences.AppendNotification) { notifications = append(notifications, n) })()
				result, err := owner.Commit(eventsequences.WithOrigin(ctx, commitOrigin))
				if err != nil || !unit.IsSuccess() || !result.ConcurrencyCheckPerformed || resolutions != 0 || len(result.Positions) != len(entries) || len(notifications) != len(entries) {
					t.Fatalf("commit: %+v %v resolutions=%d notifications=%d", result, err, resolutions, len(notifications))
				}
				for _, n := range notifications {
					if n.Origin != unit.Origin() || n.Origin == inherited || n.Origin == commitOrigin {
						t.Fatalf("guarded origin lost: %+v", n)
					}
				}
				if got := len(fixture.read("source")); got != len(entries) {
					t.Fatalf("persisted events=%d, want %d", got, len(entries))
				}
			})
		}
	}
}
