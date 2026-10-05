// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc"
)

// emptyScopeConnection is an executable fixture for an empty kernel tail.
// Applications normally obtain the sequence from EventStore.EventLog().
type emptyScopeConnection struct{ calls int }

func (*emptyScopeConnection) ConcurrencyPolicy() eventsequences.ConcurrencyPolicy {
	return eventsequences.ConcurrencyPolicy{CheckFirstAppendIntoAScope: true}
}
func (c *emptyScopeConnection) Invoke(_ context.Context, method string, _ any, response any, _ ...grpc.CallOption) error {
	if method != sequences.EventSequences_TailSequenceNumber_FullMethodName {
		return fmt.Errorf("unexpected RPC: %s", method)
	}
	c.calls++
	*response.(*sequences.QueryResult_EventSequenceTailResponse) = sequences.QueryResult_EventSequenceTailResponse{
		IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: uint64(events.Unavailable)},
	}
	return nil
}
func (*emptyScopeConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("unexpected stream")
}

func ExampleSequence_ResolveScope() {
	catalog, err := events.NewCatalog()
	if err != nil {
		panic(err)
	}
	connection := &emptyScopeConnection{}
	sequence, err := eventsequences.New("orders", "Default", events.EventLog, catalog, connection)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	source, stream := events.SourceID("order-42"), events.StreamID("sales")
	scope, err := sequence.ResolveScope(ctx, eventsequences.ScopeFilter{SourceID: &source, StreamID: &stream})
	if err != nil {
		panic(err)
	}
	unit, owner, err := transactions.Begin(ctx, sequence)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := owner.Rollback(); err != nil {
			panic(err)
		}
	}()
	// Stage a resolved, independently labeled check. No append or second tail read.
	if err := unit.Stage(ctx, nil, eventsequences.LabeledScope{Label: string(source), Scope: scope}); err != nil {
		panic(err)
	}
	fmt.Println(scope.Expectation == eventsequences.NoMatchingEvent(), connection.calls)
	// Output: true 1
}
