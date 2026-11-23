// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
)

type pinReactorA struct{ handled *atomic.Int32 }

func (r *pinReactorA) Anything(context.Context, *ReactorInput, events.Context, reactors.Delivery) (ReactorOutput, error) {
	r.handled.Add(1)
	return ReactorOutput{}, nil
}

type pinReactorB struct{ handled *atomic.Int32 }

func (r *pinReactorB) Anything(context.Context, *ReactorInput, events.Context, reactors.Delivery) (ReactorOutput, error) {
	r.handled.Add(1)
	return ReactorOutput{}, nil
}

// Pins today's behavior: against a kernel that does not advertise multiplexed
// observation, every reactor observes on its own Reactors/Observe stream and is
// delivered to independently.
func TestReactorsObserveOnOneStreamEachWhenTheKernelAdvertisesNoMultiplexing(t *testing.T) {
	registry := reactorRegistry(t)
	var handledA, handledB atomic.Int32
	if err := chronicle.RegisterReactor[*pinReactorA](registry, func() *pinReactorA { return &pinReactorA{&handledA} }, reactors.WithID("pin-a")); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactor[*pinReactorB](registry, func() *pinReactorB { return &pinReactorB{&handledB} }, reactors.WithID("pin-b")); err != nil {
		t.Fatal(err)
	}
	kernel := &reactorKernel{}
	_, _, ctx := reactorClient(t, kernel, registry)
	sessions := []*observerSession{receive(t, ctx, kernel.sessions), receive(t, ctx, kernel.sessions)}
	ids := []string{sessions[0].registration.Reactor.ReactorId, sessions[1].registration.Reactor.ReactorId}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"pin-a", "pin-b"}) || kernel.active.Load() != 2 {
		t.Fatalf("expected one stream per reactor, got %v with %d streams", ids, kernel.active.Load())
	}
	for _, session := range sessions {
		session.batches <- batch(0)
		if result := receive(t, ctx, session.results); result.State != contracts.ObservationState_Success {
			t.Fatal(result)
		}
	}
	if handledA.Load() != 1 || handledB.Load() != 1 {
		t.Fatalf("handled %d/%d", handledA.Load(), handledB.Load())
	}
}
