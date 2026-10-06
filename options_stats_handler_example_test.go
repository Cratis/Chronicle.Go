// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"google.golang.org/grpc/stats"
)

type exampleStatsHandler struct{}

func (exampleStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (exampleStatsHandler) HandleRPC(context.Context, stats.RPCStats) {}
func (exampleStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (exampleStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func ExampleWithGRPCStatsHandler() {
	// Supply an application-owned, concurrency-safe handler. Chronicle reuses it
	// across owned connection generations and never closes its resources.
	client, err := chronicle.NewClient(chronicle.WithGRPCStatsHandler(exampleStatsHandler{}))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("configured without dialing")
	if err := client.Close(); err != nil {
		fmt.Println(err)
	}
	// Output: configured without dialing
}
