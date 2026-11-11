// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/internal/connection"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReducerStreamBacksOffResetsAfterDeliveryAndKeepsGrowingAcrossALostSession(t *testing.T) {
	delays := make(chan time.Duration, 8)
	retry := func(c *clientConfig) {
		c.observerRetryWait = func(ctx context.Context, delay time.Duration) error {
			delays <- delay
			return ctx.Err()
		}
		c.observerBackoff = func() *connection.ObservationBackoff {
			return connection.NewObservationBackoff(connection.ObservationBaseDelay, connection.ObservationMaxDelay, connection.ObservationHealthyStream, time.Now, func() float64 { return 1 })
		}
	}
	client, store, ctx, server, _ := reducerFixture(t, foldRegistry(t, sumFold), retry)
	first := receiveOpening(t, ctx, server.registrations)
	expect := func(want time.Duration) *contracts.RegisterReducer {
		t.Helper()
		if delay := receiveOpening(t, ctx, delays); delay != want {
			t.Fatalf("retry delay = %s, want %s", delay, want)
		}
		return receiveOpening(t, ctx, server.registrations)
	}
	server.end <- nil
	expect(2 * time.Second)
	server.end <- status.Error(codes.Unavailable, "busy")
	expect(4 * time.Second)

	server.operations <- &contracts.ReduceOperationMessage{Partition: "source", InitialState: `{"id":"source","amount":1}`, Events: []*contracts.AppendedEvent{foldEvent("FoldDeleted", 0, `{}`)}}
	if result := receiveOpening(t, ctx, server.results); result.State != contracts.ObservationState_Success {
		t.Fatalf("delivery result = %v", result)
	}
	server.end <- nil
	expect(2 * time.Second)

	server.end <- status.Error(codes.Internal, "ClientIsNotConnected")
	renewed := receiveOpening(t, ctx, server.registrations)
	if renewed.ConnectionId == first.ConnectionId {
		t.Fatal("reducer reregistered under the lost connection ID")
	}
	// The backoff belongs to the reducer, not the connection: the growth since
	// the last delivery carries over to the new connection.
	server.end <- nil
	expect(4 * time.Second)

	if err := store.UnregisterReducer(ctx, "fold"); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	// Close has joined every worker, so a retry of the lost session would be here.
	select {
	case delay := <-delays:
		t.Fatalf("a stream was retried after %s that should not have been", delay)
	default:
	}
}
