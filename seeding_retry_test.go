// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"sync/atomic"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestSeedTransientFailureRetriesInBackgroundWithoutRepreparing(t *testing.T) {
	requests := make(chan *contracts.SeedEventsRequest, 4)
	var sends, preparations atomic.Int32
	kernel := &supervisedKernel{seeding: &seedKernel{send: func(_ context.Context, request *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
		requests <- request
		if sends.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "try later")
		}
		return &contracts.CommandResult{IsAuthorized: true}, nil
	}}}
	registry := seedRegistry(t, func(b *seeding.Builder) error {
		preparations.Add(1)
		seeding.For(b, "one", lifecycleEvent{})
		return nil
	})
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), seedPolicy())
	if _, err := client.EventStore(ctx, "store"); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	first := receiveSeed(t, ctx, requests)
	second := receiveSeed(t, ctx, requests)
	if !proto.Equal(first, second) {
		t.Fatal("retry changed snapshot")
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if sends.Load() != 2 || preparations.Load() != 1 {
		t.Fatal(sends.Load(), preparations.Load())
	}
}
