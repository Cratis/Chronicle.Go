// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func ExampleBegin() {
	// This local staging example makes no RPCs. For persistence, obtain the
	// sequence from an initialized EventStore and call owner.Commit(ctx).
	definition, err := events.Define[changed]()
	if err != nil {
		panic(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		panic(err)
	}
	conn, err := grpc.NewClient("localhost:35000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}()
	sequence, err := eventsequences.New("example", "Default", events.EventLog, catalog, conn)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	unit, owner, err := transactions.Begin(ctx, sequence)
	if err != nil {
		panic(err)
	}
	ctx = transactions.WithUnitOfWork(ctx, unit)
	participant, ok := transactions.FromContext(ctx)
	if !ok {
		panic("no unit of work")
	}
	if err := participant.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: changed{Value: "staged"}}}); err != nil {
		panic(err)
	}
	fmt.Println("Pending events:", len(unit.GetEvents()))
	if err := owner.Rollback(); err != nil {
		panic(err)
	}
	fmt.Println("Completed:", unit.IsCompleted())
	fmt.Println("Pending events:", len(unit.GetEvents()))
	// Output:
	// Pending events: 1
	// Completed: true
	// Pending events: 0
}
