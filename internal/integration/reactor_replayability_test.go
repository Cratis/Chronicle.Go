//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type KernelWideOnceInput struct{ Value string }
type KernelWideOnceReactor struct{ deliveries chan events.Context }

func (r *KernelWideOnceReactor) On(_ KernelWideOnceInput, ec events.Context) { r.deliveries <- ec }

func TestKernelReactorWideOnceOnlyRefusesReplayJob(t *testing.T) {
	for _, once := range []bool{false, true} {
		name := "default replayable"
		if once {
			name = "reactor-wide once only"
		}
		t.Run(name, func(t *testing.T) {
			f := newKernelFixture(t)
			r := integrationRegistry[KernelWideOnceInput](t)
			deliveries := make(chan events.Context, 8)
			var options []reactors.Option
			options = append(options, reactors.WithID("wide-once"))
			if once {
				options = append(options, reactors.OnceOnly())
			}
			// Deliberately no method-level OnceOnly: a handler skip cannot prove
			// that the kernel accepted a non-replayable registration.
			if err := chronicle.RegisterReactor[*KernelWideOnceReactor](r, func() *KernelWideOnceReactor { return &KernelWideOnceReactor{deliveries} }, options...); err != nil {
				t.Fatal(err)
			}
			client := f.client(r)
			store, err := client.EventStore(f.ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { recordReplayDiagnostics(t, f, client, store) })
			ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
			defer cancel()
			// Open only sends registration; it does not acknowledge subscription.
			// Appending before Active can invoke startup catch-up. Its interleaving
			// completion can race Replay's job-ID receipt (Chronicle#4514). This
			// replayability spec must start from a subscribed, live observer.
			awaitActiveReplayObserver(t, ctx, store, !once, nil)
			last := appendSuccessfully(t, ctx, store, "source", KernelWideOnceInput{Value: "live"})
			select {
			case ec := <-deliveries:
				if ec.ObservationState&events.ObservationReplay != 0 {
					t.Fatal("live delivery marked as replay")
				}
			case <-ctx.Done():
				t.Fatal("no live delivery", ctx.Err())
			}
			awaitActiveReplayObserver(t, ctx, store, !once, last.Position)
			observers := contracts.NewObserversClient(f.conn)
			info, err := observers.GetObserverInformation(ctx, &contracts.GetObserverInformationRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "wide-once", EventSequenceId: string(events.EventLog)}, grpc.ForceCodec(replayDiagnosticCodec{t}))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("before replay observer (raw proto3 defaults)=%v", info)
			response, err := observers.Replay(ctx, &contracts.Replay{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "wide-once", EventSequenceId: string(events.EventLog)}, grpc.ForceCodec(replayDiagnosticCodec{t}))
			if err != nil {
				t.Fatal(err)
			}
			if response == nil {
				t.Fatal("nil replay response")
			}
			t.Logf("replay response JobId=%q", response.JobId)
			job, err := uuid.Parse(response.JobId)
			if err != nil {
				t.Fatalf("invalid job ID %q: %v", response.JobId, err)
			}
			// Observer.Replay returns JobId.NotSet (Guid.Empty) for IsReplayable=false.
			if once {
				if job != uuid.Nil {
					t.Fatalf("non-replayable reactor started replay job %s", job)
				}
				// A second live delivery verifies the observer remains usable after refusal.
				appendSuccessfully(t, ctx, store, "source", KernelWideOnceInput{Value: "after refusal"})
			} else if job == uuid.Nil {
				t.Fatal("default replayable reactor refused replay")
			}
			select {
			case ec := <-deliveries:
				if replay := ec.ObservationState&events.ObservationReplay != 0; replay == once {
					t.Fatalf("delivery replay=%v, once=%v", replay, once)
				}
			case <-ctx.Done():
				t.Fatal("no delivery after replay request", ctx.Err())
			}
		})
	}
}
