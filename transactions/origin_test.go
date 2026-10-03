// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

func TestConcurrentSameCorrelationUnitsAttributeNotificationsByOrigin(t *testing.T) {
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	ctx, sequence, calls := fixture(t, func(ctx context.Context, raw any) (any, error) {
		request := raw.(*sequences.AppendManyForEventSourcesRequest)
		arrived <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if request.Events[0].EventSourceId == "rejected" {
			return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{
				HasConcurrencyViolations: true, ConcurrencyViolations: []*sequences.ConcurrencyViolation{{EventSourceId: "rejected", ExpectedSequenceNumber: 4, ActualSequenceNumber: 5}},
			}}, nil
		}
		return success(1), nil
	})
	correlation, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	ctx = metadata.WithCorrelation(ctx, correlation)
	installed := eventsequences.NewOrigin()
	ctx = eventsequences.WithOrigin(ctx, installed)
	committed, commitOwner := begin(t, ctx, sequence)
	rejected, rejectOwner := begin(t, ctx, sequence)
	if committed.Origin() == (eventsequences.Origin{}) || rejected.Origin() == (eventsequences.Origin{}) ||
		committed.Origin() == rejected.Origin() || committed.Origin() == installed || rejected.Origin() == installed {
		t.Fatal("units did not get fresh, distinct origins")
	}
	stage(t, ctx, committed, "committed", "one")
	stage(t, eventsequences.WithOrigin(ctx, eventsequences.NewOrigin()), rejected, "rejected", "two")

	var mu sync.Mutex
	attributed := make(map[eventsequences.Origin][]eventsequences.AppendNotification)
	for _, unit := range []*transactions.UnitOfWork{committed, rejected} {
		origin := unit.Origin()
		defer sequence.OnAppend(func(n eventsequences.AppendNotification) {
			if n.Origin != origin {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			attributed[origin] = append(attributed[origin], n)
		})()
	}
	var commits sync.WaitGroup
	for _, owner := range []*transactions.Owner{commitOwner, rejectOwner} {
		commits.Go(func() {
			if _, err := owner.Commit(eventsequences.WithOrigin(ctx, eventsequences.NewOrigin())); err != nil {
				t.Error(err)
			}
		})
	}
	// Keep both commits in flight before allowing either RPC to complete. Always
	// release and join the owned goroutines, including on a deadline failure.
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			close(release)
			commits.Wait()
			t.Fatal(ctx.Err())
		}
	}
	close(release)
	commits.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls.Load() != 2 || len(attributed) != 2 {
		t.Fatalf("RPCs = %d, attributed units = %d", calls.Load(), len(attributed))
	}
	for _, tc := range []struct {
		unit        *transactions.UnitOfWork
		source      string
		disposition eventsequences.Disposition
	}{
		{committed, "committed", eventsequences.Committed},
		{rejected, "rejected", eventsequences.Rejected},
	} {
		notifications := attributed[tc.unit.Origin()]
		if len(notifications) != 1 {
			t.Fatalf("%s notifications = %d, want 1", tc.source, len(notifications))
		}
		n := notifications[0]
		if n.CorrelationID != correlation || n.Origin != tc.unit.Origin() || string(n.Events[0].Source) != tc.source || n.Result.Disposition != tc.disposition {
			t.Fatalf("%s attribution = %+v", tc.source, n)
		}
		if tc.disposition == eventsequences.Rejected {
			var violation *eventsequences.ConcurrencyError
			if n.Err != nil || !errors.As(n.Result.Err(), &violation) {
				t.Fatalf("lost rejection diagnostics: %+v", n)
			}
		}
		if _, err := map[*transactions.UnitOfWork]*transactions.Owner{committed: commitOwner, rejected: rejectOwner}[tc.unit].Commit(ctx); !errors.Is(err, transactions.ErrCompleted) {
			t.Fatalf("repeat commit = %v", err)
		}
		if len(attributed[tc.unit.Origin()]) != 1 {
			t.Fatal("repeat commit emitted another notification")
		}
	}
}

func TestUnitOriginZeroAndRollbackLifetime(t *testing.T) {
	var absent *transactions.UnitOfWork
	var zero transactions.UnitOfWork
	if absent.Origin() != (eventsequences.Origin{}) || zero.Origin() != (eventsequences.Origin{}) {
		t.Fatal("invalid unit origin must be zero")
	}
	ctx, sequence, _ := fixture(t, func(context.Context, any) (any, error) { return success(1), nil })
	unit, owner := begin(t, ctx, sequence)
	origin := unit.Origin()
	if err := owner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if unit.Origin() != origin || origin == (eventsequences.Origin{}) {
		t.Fatal("rollback lost origin")
	}
}
