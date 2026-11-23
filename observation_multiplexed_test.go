// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"slices"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	multiplexedcontracts "github.com/cratis/chronicle.go/contracts/observation/multiplexed"
	"github.com/cratis/chronicle.go/reactors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// muxKernel is the kernel's MultiplexedObservation service.
type muxKernel struct {
	multiplexedcontracts.UnimplementedMultiplexedObservationServer
	calls atomic.Int32
	up    chan *multiplexedcontracts.MultiplexedStreamMessage
	down  chan *multiplexedcontracts.MultiplexedServerMessage
}

func newMuxKernel() *muxKernel {
	return &muxKernel{up: make(chan *multiplexedcontracts.MultiplexedStreamMessage, 32), down: make(chan *multiplexedcontracts.MultiplexedServerMessage, 32)}
}

func (k *muxKernel) Observe(stream grpc.BidiStreamingServer[multiplexedcontracts.MultiplexedClientMessage, multiplexedcontracts.MultiplexedServerMessage]) error {
	k.calls.Add(1)
	go func() {
		for {
			message, err := stream.Recv()
			if err != nil {
				return
			}
			for _, m := range message.Messages {
				k.up <- m
			}
		}
	}()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case message := <-k.down:
			if err := stream.Send(message); err != nil {
				return err
			}
		}
	}
}

func twoReactors(t *testing.T, handled *atomic.Int32) *chronicle.Registry {
	t.Helper()
	registry := reactorRegistry(t)
	var unused atomic.Int32
	if err := chronicle.RegisterReactor[*pinReactorA](registry, func() *pinReactorA { return &pinReactorA{handled} }, reactors.WithID("pin-a")); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactor[*pinReactorB](registry, func() *pinReactorB { return &pinReactorB{&unused} }, reactors.WithID("pin-b")); err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestReactorsShareOneObservationStreamWhenTheKernelAdvertisesIt(t *testing.T) {
	var handled atomic.Int32
	mux := newMuxKernel()
	kernel := &reactorKernel{multiplexed: mux}
	kernel.capabilities = []string{"multiplexed-observation"}
	_, _, ctx := reactorClient(t, kernel, twoReactors(t, &handled))
	registrations := map[string]uint64{}
	for range 2 {
		message := receive(t, ctx, mux.up)
		if message.Kind != multiplexedcontracts.MultiplexedObserverKind_Reactor {
			t.Fatal(message)
		}
		registrations[message.Reactor.Content.Value0.Reactor.ReactorId] = message.StreamId
	}
	ids := []string{}
	for id := range registrations {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"pin-a", "pin-b"}) || registrations["pin-a"] == registrations["pin-b"] {
		t.Fatal(registrations)
	}
	if mux.calls.Load() != 1 || kernel.active.Load() != 0 || len(kernel.sessions) != 0 {
		t.Fatalf("shared calls %d, per-observer streams %d", mux.calls.Load(), kernel.active.Load())
	}
	// A batch for one stream reaches its reactor and its result goes back on the same stream.
	observed := &multiplexedcontracts.EventsToObserve{}
	if err := proto.Unmarshal(mustMarshal(t, batch(0)), observed); err != nil {
		t.Fatal(err)
	}
	observed.DeliveryId = 11
	mux.down <- &multiplexedcontracts.MultiplexedServerMessage{StreamId: registrations["pin-a"], Sequence: 1, Reactor: observed}
	result := receive(t, ctx, mux.up)
	if result.StreamId != registrations["pin-a"] || result.Reactor.Content.Value1.DeliveryId != 11 || result.Reactor.Content.Value1.State != multiplexedcontracts.ObservationState_Success {
		t.Fatal(result)
	}
	if handled.Load() != 1 {
		t.Fatalf("handled %d", handled.Load())
	}
}

func mustMarshal(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReactorsObserveOnTheirOwnStreamsWhenMultiplexingIsTurnedOff(t *testing.T) {
	var handled atomic.Int32
	mux := newMuxKernel()
	kernel := &reactorKernel{multiplexed: mux}
	kernel.capabilities = []string{"multiplexed-observation"}
	_, _, ctx := reactorClient(t, kernel, twoReactors(t, &handled), chronicle.WithMultiplexedObservation(false))
	receive(t, ctx, kernel.sessions)
	receive(t, ctx, kernel.sessions)
	if mux.calls.Load() != 0 || kernel.active.Load() != 2 {
		t.Fatalf("shared calls %d, per-observer streams %d", mux.calls.Load(), kernel.active.Load())
	}
}

func TestReactorsObserveOnTheirOwnStreamsWhenTheKernelDoesNotAdvertiseMultiplexing(t *testing.T) {
	var handled atomic.Int32
	mux := newMuxKernel()
	kernel := &reactorKernel{multiplexed: mux}
	_, _, ctx := reactorClient(t, kernel, twoReactors(t, &handled))
	receive(t, ctx, kernel.sessions)
	receive(t, ctx, kernel.sessions)
	if mux.calls.Load() != 0 || kernel.active.Load() != 2 {
		t.Fatalf("shared calls %d, per-observer streams %d", mux.calls.Load(), kernel.active.Load())
	}
}
