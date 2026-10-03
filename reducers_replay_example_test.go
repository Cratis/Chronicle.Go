// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reducers"
)

type ReplayAdjustment struct{ Amount int }
type ReplayBalance struct{ Amount int }

func ExampleRegisterReducerHandlers_replay() {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ReplayAdjustment](registry); err != nil {
		panic(err)
	}
	model, err := chronicle.RegisterReadModel[ReplayBalance](registry)
	if err != nil {
		panic(err)
	}
	fold := func(_ context.Context, event ReplayAdjustment, current *ReplayBalance, _ events.Context) (*ReplayBalance, error) {
		amount := event.Amount
		if current != nil {
			amount += current.Amount
		}
		return &ReplayBalance{Amount: amount}, nil
	}
	callbacks := reducers.ReplayCallbacks{
		BeginReplay: func(ctx context.Context) error { return ctx.Err() },
		EndReplay:   func(ctx context.Context) error { return ctx.Err() },
		BeginReplayPartition: func(ctx context.Context, partition events.SourceID) error {
			// The opaque partition key is available without an event payload.
			_ = partition
			return ctx.Err()
		},
		EndReplayPartition: func(ctx context.Context, partition events.SourceID) error { return ctx.Err() },
	}
	if err := chronicle.RegisterReducerHandlers(registry, model, "balance", []reducers.Handler{reducers.On(fold)}, reducers.WithReplayCallbacks(callbacks), reducers.WithVersion("1")); err != nil {
		panic(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		panic(err)
	}
	if err := client.Close(); err != nil {
		panic(err)
	}
	// Offline startup validation, not a kernel replay or completion witness.
	fmt.Println("Replay callbacks validated")
	// Output: Replay callbacks validated
}
