// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
)

func TestAppendNotificationsRemainHandleLocal(t *testing.T) {
	first, _ := sequenceFixture(t, map[string]rpcHandler{"Append": successfulNotificationHandler})
	second, _ := sequenceFixture(t, map[string]rpcHandler{"Append": successfulNotificationHandler})
	var count int
	defer first.OnAppend(func(eventsequences.AppendNotification) { count++ })()
	if _, err := notificationAppend(testContext(t), second, "single"); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("another handle with identical coordinates notified", count)
	}
	if _, err := notificationAppend(testContext(t), first, "single"); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal(count)
	}
}

func TestAppendNotificationRetainsCommittedDispositionWithOperationError(t *testing.T) {
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{"Append": func(context.Context, any) (any, error) {
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true}}, nil
	}})
	var n eventsequences.AppendNotification
	defer sequence.OnAppend(func(notification eventsequences.AppendNotification) { n = notification })()
	result, err := sequence.Append(testContext(t), "A", opened{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.Exact(1)}))
	if !errors.Is(err, faults.ErrUnsupported) || n.Err != err || n.Result.Disposition != eventsequences.Committed || result.Disposition != eventsequences.Committed || n.Events[0].Position == nil {
		t.Fatalf("notification=%+v result=%+v err=%v", n, result, err)
	}
}

func TestAppendCancellationNotifiesUnknownAfterDispatch(t *testing.T) {
	entered := make(chan struct{})
	sequence, _ := sequenceFixture(t, map[string]rpcHandler{"Append": func(ctx context.Context, _ any) (any, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	parent := testContext(t)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	correlation, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	ctx = metadata.WithCorrelation(ctx, correlation)
	var received eventsequences.AppendNotification
	defer sequence.OnAppend(func(n eventsequences.AppendNotification) { received = n })()
	done := make(chan error, 1)
	go func() {
		_, err := notificationAppend(ctx, sequence, "single")
		done <- err
	}()
	select {
	case <-entered:
	case <-parent.Done():
		t.Fatal(parent.Err())
	}
	cancel()
	select {
	case err := <-done:
		var unknown *eventsequences.OutcomeUnknownError
		if !errors.As(err, &unknown) || received.Err != err || received.Result.Disposition != eventsequences.Unknown || received.CorrelationID != correlation || received.Events[0].Position != nil {
			t.Fatalf("notification=%+v err=%v", received, err)
		}
	case <-parent.Done():
		t.Fatal(parent.Err())
	}
}
