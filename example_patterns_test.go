// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/patterns"
)

// This example compiles but requires a running local development kernel to run.
// Development defaults trust the disposable server certificate and use its
// development credentials; use verified TLS and your TokenSource in production.
func ExampleEventStore_Patterns() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := chronicle.NewClient(chronicle.WithConnectionString("chronicle://localhost:35000"), chronicle.WithDevelopmentDefaults())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			fmt.Println(err)
		}
	}()
	store, err := client.EventStore(ctx, "orders", chronicle.WithNamespace("tenant-a"))
	if err != nil {
		fmt.Println(err)
		return
	}
	moment := time.Date(2024, 1, 15, 9, 30, 0, 0, time.FixedZone("", 2*3600))
	facets := patterns.NewFacetSet(map[patterns.FacetName]patterns.FacetValue{patterns.AggregateType: "Order"})
	result, err := store.Patterns().GetPatternsAt(ctx, "demo-scope", &moment, facets, patterns.QueryOptions{})
	if err != nil {
		fmt.Println(err)
		return
	}
	if len(result.Data) == 0 {
		fmt.Println("No established behavior; do not claim to know.")
		return
	}
	for _, answer := range result.Data {
		fmt.Println(answer.Action(), answer.Confidence, answer.Occurrences)
	}
}
