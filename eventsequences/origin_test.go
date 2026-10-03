// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
)

func TestOriginsAreUniqueUnderConcurrentAllocation(t *testing.T) {
	const workers, perWorker = 32, 512
	origins := make(chan eventsequences.Origin, workers*perWorker)
	var workersDone sync.WaitGroup
	for range workers {
		workersDone.Go(func() {
			for range perWorker {
				origins <- eventsequences.NewOrigin()
			}
		})
	}
	workersDone.Wait()
	close(origins)
	seen := make(map[eventsequences.Origin]bool)
	for origin := range origins {
		if origin == (eventsequences.Origin{}) || seen[origin] {
			t.Fatal("zero or reused origin")
		}
		seen[origin] = true
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("origins = %d, want %d", len(seen), workers*perWorker)
	}
}

func TestOriginContextInheritanceReplacementAndMasking(t *testing.T) {
	base := context.Background()
	first, second := eventsequences.NewOrigin(), eventsequences.NewOrigin()
	parent := eventsequences.WithOrigin(base, first)
	child := eventsequences.WithOrigin(parent, second)
	masked := eventsequences.WithOrigin(child, eventsequences.Origin{})
	if eventsequences.OriginFrom(base) != (eventsequences.Origin{}) ||
		eventsequences.OriginFrom(parent) != first || eventsequences.OriginFrom(child) != second ||
		eventsequences.OriginFrom(masked) != (eventsequences.Origin{}) {
		t.Fatal("context origin was lost, leaked or failed to mask")
	}
	derived, cancel := context.WithCancel(parent)
	cancel()
	if eventsequences.OriginFrom(derived) != first {
		t.Fatal("derived context lost origin")
	}
}

func TestImmediateNotificationsInheritOriginAcrossPathsAndDispositions(t *testing.T) {
	for _, path := range notificationPaths {
		if path.name == "unit commit" {
			continue // Unit commits use their own identity, tested in transactions.
		}
		t.Run(path.name, func(t *testing.T) {
			for _, outcome := range []string{"committed", "constraints", "concurrency", "transport"} {
				t.Run(outcome, func(t *testing.T) {
					sequence, _ := sequenceFixture(t, map[string]rpcHandler{path.rpc: func(context.Context, any) (any, error) {
						return notificationResponse(path.single, outcome, metadata.CorrelationID{})
					}})
					for _, origin := range []eventsequences.Origin{{}, eventsequences.NewOrigin()} {
						var received []eventsequences.AppendNotification
						unsubscribe := sequence.OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })
						ctx := testContext(t)
						if origin != (eventsequences.Origin{}) {
							ctx = eventsequences.WithOrigin(ctx, origin)
						}
						_, _ = notificationAppend(ctx, sequence, path.name) // Outcome assertions are covered by the disposition matrix.
						unsubscribe()
						if len(received) != 1 || received[0].Origin != origin {
							t.Fatalf("notifications = %+v, want one with origin %v", received, origin)
						}
					}
				})
			}
		})
	}
}

func TestPreparedNotificationUsesAppendOriginNotPreparationOrigin(t *testing.T) {
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{"AppendManyForEventSources": func(context.Context, any) (any, error) {
		return notificationResponse(false, "committed", metadata.CorrelationID{})
	}})
	ctx := testContext(t)
	snapshot, err := sequence.PrepareBatch(eventsequences.WithOrigin(ctx, eventsequences.NewOrigin()),
		[]eventsequences.Entry{{Source: "A", Event: opened{}}, {Source: "B", Event: changed{}}, {Source: "A", Event: opened{}}},
		eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}},
			eventsequences.LabeledScope{Label: "B", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}))
	if err != nil {
		t.Fatal(err)
	}
	var received []eventsequences.AppendNotification
	defer sequence.OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })()
	origin := eventsequences.NewOrigin()
	if _, err := sequence.AppendPreparedBatch(eventsequences.WithOrigin(ctx, origin), snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := sequence.AppendPreparedBatch(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if len(received) != 2 || received[0].Origin != origin || received[1].Origin != (eventsequences.Origin{}) {
		t.Fatalf("prepared origins = %+v", received)
	}
}
