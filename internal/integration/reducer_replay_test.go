//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type kernelReducerReplayNotice struct {
	state string
	key   events.SourceID
}
type KernelReplayReducer struct {
	notices chan kernelReducerReplayNotice
	closed  *atomic.Int32
}

func (*KernelReplayReducer) Change(e ReducedAmountChanged, current *ReducedBalance, ec events.Context) *ReducedBalance {
	amount := e.Amount
	if current != nil {
		amount += current.Amount
	}
	return &ReducedBalance{ID: string(ec.SourceID), Amount: amount}
}
func (r *KernelReplayReducer) notify(ctx context.Context, state string, key events.SourceID) error {
	select {
	case r.notices <- kernelReducerReplayNotice{state, key}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *KernelReplayReducer) BeginReplay(ctx context.Context) error {
	return r.notify(ctx, "begin", "")
}
func (r *KernelReplayReducer) EndReplay(ctx context.Context) error { return r.notify(ctx, "end", "") }
func (r *KernelReplayReducer) BeginReplayPartition(ctx context.Context, key events.SourceID) error {
	return r.notify(ctx, "partition-begin", key)
}
func (r *KernelReplayReducer) EndReplayPartition(ctx context.Context, key events.SourceID) error {
	return r.notify(ctx, "partition-end", key)
}
func (r *KernelReplayReducer) Close() error { r.closed.Add(1); return nil }

func TestKernelReducerReplayLifecyclePreservesNormalFolding(t *testing.T) {
	f := newKernelFixture(t)
	r := integrationRegistry[ReducedAmountChanged](t)
	model, err := chronicle.RegisterReadModel[ReducedBalance](r)
	if err != nil {
		t.Fatal(err)
	}
	notices := make(chan kernelReducerReplayNotice, 64)
	var created, closed atomic.Int32
	if err := chronicle.RegisterReducer[*KernelReplayReducer](r, model, func() *KernelReplayReducer {
		created.Add(1)
		return &KernelReplayReducer{notices, &closed}
	}, reducers.WithID("replay-balance"), reducers.WithVersion("replay-hooks-1")); err != nil {
		t.Fatal(err)
	}
	client := f.client(r)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	observers := store.Observers()
	waitOperations(t, f.ctx, func() bool {
		info, err := observers.Get(f.ctx, "replay-balance", events.EventLog)
		if err != nil {
			t.Fatal(err)
		}
		return info != nil && info.IsSubscribed()
	})
	reader := readmodels.For(store.ReadModels(), model)
	for _, key := range []events.SourceID{"left", "right"} {
		appended, err := store.EventLog().AppendWithMetadata(f.ctx, key, ReducedAmountChanged{3})
		if err != nil || appended.Result().Err() != nil {
			t.Fatal(err, appended.Result().Err())
		}
		completed, err := appended.WaitForCompletion(f.ctx, observers, 5*time.Second)
		if err != nil || !completed.IsSuccess() {
			t.Fatal(completed, err)
		}
		waitReduced(t, f.ctx, reader, string(key), true, 3)
	}
	handle, err := observers.Replay(f.ctx, "replay-balance", events.EventLog)
	if err != nil {
		t.Fatal(err)
	}
	var delivered []kernelReducerReplayNotice
	seenBegin, seenEnd := false, false
	for !seenEnd {
		select {
		case notice := <-notices:
			delivered = append(delivered, notice)
			if notice.state == "begin" {
				seenBegin = true
			}
			if notice.state == "end" {
				if !seenBegin {
					t.Fatal("replay ended before beginning", delivered)
				}
				seenEnd = true
			}
		case <-f.ctx.Done():
			diagnostic, cancel := context.WithTimeout(context.WithoutCancel(f.ctx), 5*time.Second)
			info, infoErr := observers.Get(diagnostic, "replay-balance", events.EventLog)
			cancel()
			t.Fatalf("replay hooks missing: job=%s notices=%v observer=%v error=%v: %v", handle.ID(), delivered, info, infoErr, f.ctx.Err())
		}
	}
	// Lifecycle is not exactly-once delivery or proof of durable job success.
	// Assert partition coordinates only for partition notifications the kernel
	// actually emits during this reducer-wide replay.
	for _, notice := range delivered {
		if notice.state == "partition-begin" || notice.state == "partition-end" {
			if notice.key != "left" && notice.key != "right" {
				t.Fatal("partition key changed", delivered)
			}
		}
	}
	for _, key := range []string{"left", "right"} {
		waitReduced(t, f.ctx, reader, key, true, 3)
	}
	appended, err := store.EventLog().AppendWithMetadata(f.ctx, "left", ReducedAmountChanged{4})
	if err != nil || appended.Result().Err() != nil {
		t.Fatal(err, appended.Result().Err())
	}
	completed, err := appended.WaitForCompletion(f.ctx, observers, 5*time.Second)
	if err != nil || !completed.IsSuccess() {
		t.Fatal(completed, err)
	}
	waitReduced(t, f.ctx, reader, "left", true, 7)
	if err := store.UnregisterReducer(f.ctx, "replay-balance"); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if created.Load() != closed.Load() || created.Load() < 4 {
		t.Fatalf("artifacts=%d/%d", created.Load(), closed.Load())
	}
	t.Logf("accepted replay job=%s lifecycle=%v artifacts=%d/%d; normal post-replay fold=7", handle.ID(), delivered, created.Load(), closed.Load())
}
