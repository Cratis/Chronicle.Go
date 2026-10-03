// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// lostAcknowledgmentConnection is an offline fixture, not a production transport.
// Normally obtain the sequence from store.EventLog().
type lostAcknowledgmentConnection struct{}

func (lostAcknowledgmentConnection) Invoke(_ context.Context, method string, _, _ any, _ ...grpc.CallOption) error {
	if method != sequences.EventSequences_Append_FullMethodName {
		return fmt.Errorf("unexpected RPC: %s", method)
	}
	return status.Error(codes.Unavailable, "append acknowledgment lost")
}

func (lostAcknowledgmentConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("unexpected stream")
}

func ExampleSequence_OnAppend() {
	type OrderPlaced struct{ Product string }
	definition, err := events.Define[OrderPlaced](events.WithID("OrderPlaced"))
	if err != nil {
		panic(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		panic(err)
	}
	sequence, err := eventsequences.New("orders", "Default", events.EventLog, catalog, lostAcknowledgmentConnection{})
	if err != nil {
		panic(err)
	}
	correlation, err := metadata.NewCorrelationID()
	if err != nil {
		panic(err)
	}
	origin := eventsequences.NewOrigin()
	ctx := eventsequences.WithOrigin(metadata.WithCorrelation(context.Background(), correlation), origin)

	// A command adapter subscribes on the same handle handed to its handlers.
	// Other concurrent commands may use this handle, so filter and synchronize.
	var mu sync.Mutex
	var attempts []eventsequences.AppendNotification
	unsubscribe := sequence.OnAppend(func(n eventsequences.AppendNotification) {
		if n.Origin != origin {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, n)
	})
	defer unsubscribe()

	_, err = sequence.Append(ctx, "order-42", OrderPlaced{Product: "book"},
		eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(err, &unknown) {
		panic(fmt.Sprintf("expected an unknown outcome, got %v", err))
	}

	// All command-owned appends have returned; dispose before releasing state.
	// To track ONLY immediate writes, also dispose before committing the unit.
	unsubscribe()
	mu.Lock()
	defer mu.Unlock()
	for _, n := range attempts {
		fmt.Println(n.Operation.Store(), n.Operation.Namespace(), n.Events[0].Source)
		fmt.Println("outcome unknown:", n.Result.Disposition == eventsequences.Unknown)
	}
	// Output:
	// orders Default order-42
	// outcome unknown: true
}
