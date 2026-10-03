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
	"github.com/cratis/chronicle.go/observation"
	"google.golang.org/protobuf/proto"
)

// Record the actual response bytes: re-marshaling proto3 would lose the explicit
// false / absent-true distinction in protobuf-net's observer information.
type replayDiagnosticCodec struct{ t *testing.T }

func (c replayDiagnosticCodec) Name() string { return "proto" }
func (c replayDiagnosticCodec) Marshal(value any) ([]byte, error) {
	data, err := proto.Marshal(value.(proto.Message))
	c.t.Logf("RPC request %T protobuf=%x", value, data)
	return data, err
}
func (c replayDiagnosticCodec) Unmarshal(data []byte, value any) error {
	c.t.Logf("RPC response %T protobuf=%x", value, data)
	return proto.Unmarshal(data, value.(proto.Message))
}

// Wait for server state, not a sleep or a sent registration. No replay is retried
// and an empty job ID remains a hard failure. This isolates the wire-policy spec
// from startup catch-up without pretending to fix the kernel's receipt race.
func awaitActiveReplayObserver(t *testing.T, ctx context.Context, store *chronicle.EventStore, replayable bool, last *events.SequenceNumber) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := store.Observers().Get(ctx, "wide-once", events.EventLog)
		if err != nil {
			t.Fatal(err)
		}
		if info != nil && info.IsSubscribed() && info.RunningState() == observation.Active && (last == nil || info.LastHandled() == *last) {
			if info.IsReplayable() != replayable {
				t.Fatalf("server replay policy=%t want=%t", info.IsReplayable(), replayable)
			}
			t.Logf("active subscription: replayable=%t last=%d next=%d handled=%d", info.IsReplayable(), info.LastHandled(), info.Next(), info.HandledEventCount())
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("observer did not reach the required active state", ctx.Err())
		case <-ticker.C:
		}
	}
}

func recordReplayDiagnostics(t *testing.T, f *kernelFixture, client *chronicle.Client, store *chronicle.EventStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(f.ctx), 5*time.Second)
	defer cancel()
	artifacts, err := client.Artifacts(f.storeName)
	if err != nil {
		t.Logf("artifacts: %v", err)
	} else {
		t.Logf("artifacts store=%s models=%d projections=%d reducers=%d reactors=%d", f.storeName, len(artifacts.ReadModels.Descriptors()), len(artifacts.Projections), len(artifacts.Reducers), len(artifacts.Reactors))
		for _, p := range artifacts.Reactors {
			t.Logf("plan id=%s sequence=%s replayable=%t events=%v", p.Identifier(), p.EventSequence(), p.IsReplayable(), p.EventTypes())
		}
	}
	outcome, err := store.WaitForRegistration(ctx)
	t.Logf("registration generation=%d pass=%d err=%v", outcome.Generation, outcome.Pass, err)
	info, err := store.Observers().Get(ctx, observation.ID("wide-once"), events.EventLog)
	if err != nil || info == nil {
		t.Logf("observer snapshot: %v", err)
	} else {
		t.Logf("observer snapshot id=%s sequence=%s replayable=%t subscribed=%t running=%v last=%d next=%d tail=%d handled=%d", info.ID(), info.Sequence(), info.IsReplayable(), info.IsSubscribed(), info.RunningState(), info.LastHandled(), info.Next(), info.Tail(), info.HandledEventCount())
	}
	connected, err := contracts.NewObserversClient(f.conn).GetConnectedClientsForObserver(ctx, &contracts.GetConnectedClientsForObserverRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "wide-once", EventSequenceId: "event-log"})
	t.Logf("connected clients=%v err=%v", connected, err)
	jobs, err := store.Jobs().List(ctx)
	t.Logf("jobs count=%d err=%v", len(jobs), err)
	for _, job := range jobs {
		t.Logf("job id=%s type=%s status=%v changes=%v", job.ID(), job.Type(), job.Status(), job.StatusChanges())
	}
}
