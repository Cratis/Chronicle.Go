// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

func successfulNotificationHandler(_ context.Context, raw any) (any, error) {
	return notificationResponse(true, "committed", wire.Correlation(raw.(*sequences.AppendRequest).CorrelationId))
}

func TestAppendCallbackCanReenterSubscribeAndDisposeDuringDelivery(t *testing.T) {
	sequence, calls := sequenceFixture(t, map[string]rpcHandler{"Append": successfulNotificationHandler})
	ctx := testContext(t)
	var order []string
	var first, second, added func()
	first = sequence.OnAppend(func(n eventsequences.AppendNotification) {
		order = append(order, "first:"+string(n.Events[0].Source))
		first()  // Self-removal must not deadlock.
		second() // A snapshotted callback not yet admitted must be skipped.
		added = sequence.OnAppend(func(n eventsequences.AppendNotification) { order = append(order, "added:"+string(n.Events[0].Source)) })
		result, err := sequence.Append(ctx, "nested", opened{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
		if err != nil || result.Err() != nil {
			t.Errorf("nested append: %+v %v", result, err)
		}
	})
	second = sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("disposed callback invoked") })
	defer first()
	defer second()
	if _, err := sequence.Append(ctx, "outer", opened{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
		t.Fatal(err)
	}
	added()
	if !reflect.DeepEqual(order, []string{"first:outer", "added:nested"}) || calls.Load() != 2 {
		t.Fatalf("order=%v calls=%d", order, calls.Load())
	}
}

func TestAppendDisposalDoesNotWaitForAdmittedCallbacks(t *testing.T) {
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{"Append": successfulNotificationHandler})
	ctx := testContext(t)
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var seen atomic.Int32
	unsubscribe := sequence.OnAppend(func(eventsequences.AppendNotification) {
		seen.Add(1)
		close(entered)
		<-release
	})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	go func() {
		_, err := notificationAppend(ctx, sequence, "single")
		returned <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	unsubscribe()
	unsubscribe()
	if _, err := notificationAppend(ctx, sequence, "single"); err != nil {
		t.Fatal(err)
	}
	unblock.Do(func() { close(release) })
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if seen.Load() != 1 {
		t.Fatal(seen.Load())
	}
}

func TestConcurrentAppendSubscriptionsAndCommandAttribution(t *testing.T) {
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{"Append": successfulNotificationHandler})
	ctx := testContext(t)
	const commands, appends = 12, 8
	var workers sync.WaitGroup
	start := make(chan struct{})
	for range commands {
		id, err := metadata.NewCorrelationID()
		if err != nil {
			t.Fatal(err)
		}
		workers.Go(func() {
			commandCtx := metadata.WithCorrelation(ctx, id)
			var observed atomic.Int32
			unsubscribe := sequence.OnAppend(func(n eventsequences.AppendNotification) {
				if n.CorrelationID == id {
					observed.Add(1)
				}
			})
			defer unsubscribe()
			<-start
			for range appends {
				// Exercise subscription/disposal concurrently with other deliveries.
				remove := sequence.OnAppend(func(eventsequences.AppendNotification) {})
				if _, err := notificationAppend(commandCtx, sequence, "single"); err != nil {
					t.Error(err)
				}
				remove()
			}
			unsubscribe()
			if observed.Load() != appends {
				t.Errorf("command observed=%d want=%d", observed.Load(), appends)
			}
		})
	}
	close(start)
	workers.Wait()
}

func TestAppendNotificationSnapshotsDoNotAliasOtherSubscribersOrResults(t *testing.T) {
	for _, outcome := range []string{"committed", "constraints and errors", "concurrency"} {
		t.Run(outcome, func(t *testing.T) {
			sequence, _ := sequenceFixture(t, map[string]rpcHandler{"AppendManyForEventSources": func(context.Context, any) (any, error) {
				return notificationResponse(false, outcome, metadata.CorrelationID{})
			}})
			defer sequence.OnAppend(func(n eventsequences.AppendNotification) {
				n.Events[0].Source = "changed"
				if n.Events[0].Position != nil {
					*n.Events[0].Position = 999
					n.Result.Positions[0] = 999
					*n.Result.Target.First = 999
					n.Result.Target.EventTypeTails["opened"] = 999
				}
				if len(n.Result.ConstraintViolations) != 0 {
					n.Result.ConstraintViolations[0].Details["PropertyValue"] = "changed"
					n.Result.ConstraintViolations[0].Message = "changed"
					n.Result.Errors[0] = "changed"
				}
				if len(n.Result.ConcurrencyViolations) != 0 {
					n.Result.ConcurrencyViolations[0].Actual = 999
				}
				refs := n.Operation.EventTypes()
				refs[0].Generation = 999
			})()
			var retained eventsequences.AppendNotification
			defer sequence.OnAppend(func(n eventsequences.AppendNotification) { retained = n })()
			result, err := notificationAppend(testContext(t), sequence, "batch")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(retained.Result, result) || retained.Events[0].Source != "A" || retained.Operation.EventTypes()[0].Generation != 1 {
				t.Fatalf("snapshot changed: %+v, result=%+v", retained, result)
			}
			if outcome == "committed" && (*retained.Events[0].Position != 0 || result.Positions[0] != 0 || *result.Target.First != 0 || result.Target.EventTypeTails["opened"] != 2) {
				t.Fatal(retained)
			}
			if outcome == "constraints and errors" && (result.ConstraintViolations[0].Details["PropertyValue"] != "Ada" || result.ConstraintViolations[0].Message != "taken" || result.Errors[0] != "FutureCode") {
				t.Fatal(result)
			}
			if outcome == "concurrency" && result.ConcurrencyViolations[0].Actual != 5 {
				t.Fatal(result)
			}
		})
	}
}

func TestAppendCallbackPanicsPreserveDispositionAndOtherDeliveries(t *testing.T) {
	for _, path := range []string{"single", "batch", "unit commit"} {
		for _, outcome := range []string{"committed", "constraints", "transport"} {
			t.Run(path+"/"+outcome, func(t *testing.T) {
				handler := func(context.Context, any) (any, error) {
					return notificationResponse(path == "single", outcome, metadata.CorrelationID{})
				}
				sequence, _ := sequenceFixture(t, map[string]rpcHandler{"Append": handler, "AppendManyForEventSources": handler})
				remove := sequence.OnAppend(func(eventsequences.AppendNotification) { panic("subscriber failed") })
				defer remove()
				var received []eventsequences.AppendNotification
				defer sequence.OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })()
				result, err := notificationAppend(testContext(t), sequence, path)
				var failure *eventsequences.AppendCallbackPanicError
				if !errors.As(err, &failure) || failure.Value != "subscriber failed" || len(received) != 1 {
					t.Fatalf("result=%+v error=%v notifications=%d", result, err, len(received))
				}
				want := eventsequences.Committed
				switch outcome {
				case "constraints":
					want = eventsequences.Rejected
				case "transport":
					want = eventsequences.Unknown
				}
				if result.Disposition != want || received[0].Result.Disposition != want || errors.As(received[0].Err, &failure) {
					t.Fatalf("callback failure altered append: %+v %v", received[0], err)
				}
				var unknown *eventsequences.OutcomeUnknownError
				if errors.As(err, &unknown) != (want == eventsequences.Unknown) {
					t.Fatal(err)
				}
				remove()
				if _, err = notificationAppend(testContext(t), sequence, path); errors.As(err, &failure) {
					t.Fatal("panic poisoned later operations", err)
				}
			})
		}
	}
}

func TestAppendNotificationsExcludePreparationRollbackAndEventlessCompletion(t *testing.T) {
	sequence, calls := sequenceFixture(t, map[string]rpcHandler{"AppendManyForEventSources": func(context.Context, any) (any, error) {
		return batchSuccess(metadata.CorrelationID{}, true, 0), nil
	}})
	var count int
	defer sequence.OnAppend(func(eventsequences.AppendNotification) { count++ })()
	sequence.OnAppend(nil)()
	ctx := testContext(t)
	if _, err := sequence.Append(ctx, "A", struct{ Unknown bool }{}); err == nil {
		t.Fatal("unknown event accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := sequence.Append(cancelled, "A", opened{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, complete := range []string{"empty", "rollback", "cancelled", "eventless"} {
		unit, owner, err := transactions.Begin(ctx, sequence)
		if err != nil {
			t.Fatal(err)
		}
		source := events.SourceID("A")
		check := eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent(), Filter: eventsequences.ScopeFilter{SourceID: &source}}}
		switch complete {
		case "rollback", "cancelled":
			err = unit.Stage(ctx, []eventsequences.Entry{{Source: source, Event: opened{}}}, check)
		case "eventless":
			err = unit.Stage(ctx, nil, check)
		}
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatal("staging notified")
		}
		switch complete {
		case "rollback":
			err = owner.Rollback()
		case "cancelled":
			_, err = owner.Commit(cancelled)
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			err = nil
		default:
			_, err = owner.Commit(ctx)
		}
		if err != nil || count != 0 {
			t.Fatalf("%s err=%v notifications=%d", complete, err, count)
		}
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}

func TestUnitCommitNotifiesSnapshotBeforeCompletionEvenIfCallbackPanics(t *testing.T) {
	sequence, calls := sequenceFixture(t, map[string]rpcHandler{"AppendManyForEventSources": func(_ context.Context, raw any) (any, error) {
		return batchSuccess(wire.Correlation(raw.(*sequences.AppendManyForEventSourcesRequest).CorrelationId), true, 1), nil
	}})
	ctx := testContext(t)
	unit, owner, err := transactions.Begin(ctx, sequence)
	if err != nil {
		t.Fatal(err)
	}
	entries := []eventsequences.Entry{{Source: "original", Event: opened{}}}
	if err := unit.Stage(ctx, entries, eventsequences.LabeledScope{Label: "original", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}); err != nil {
		t.Fatal(err)
	}
	entries[0].Source, entries[0].Event = "mutated", changed{}
	var order []string
	defer sequence.OnAppend(func(n eventsequences.AppendNotification) {
		order = append(order, "append")
		if unit.State() != transactions.Completing || n.CorrelationID != unit.CorrelationID() || n.Events[0].Source != "original" || n.Events[0].EventType.ID != "opened" {
			t.Errorf("bad commit notification: %+v state=%v", n, unit.State())
		}
		if _, err := owner.Commit(ctx); !errors.Is(err, transactions.ErrCompleting) {
			t.Error("recursive commit must fail without deadlocking", err)
		}
		panic("failed notification")
	})()
	if err := unit.OnCompleted(func(u *transactions.UnitOfWork) {
		order = append(order, "completed")
		if u.State() != transactions.Committed {
			t.Error(u.State())
		}
	}); err != nil {
		t.Fatal(err)
	}
	result, err := owner.Commit(ctx)
	var failure *eventsequences.AppendCallbackPanicError
	if result.Disposition != eventsequences.Committed || !errors.As(err, &failure) || !unit.IsCompleted() {
		t.Fatalf("result=%+v err=%v state=%v", result, err, unit.State())
	}
	if _, err := owner.Commit(ctx); !errors.Is(err, transactions.ErrCompleted) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"append", "completed"}) || calls.Load() != 1 {
		t.Fatalf("order=%v calls=%d", order, calls.Load())
	}
}
