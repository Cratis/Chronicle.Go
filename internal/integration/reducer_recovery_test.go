//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type RecoveryAmountAdded struct {
	Amount int `json:"amount"`
}
type RecoveryBalance struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

// Chronicle#4540 (fixed in 19.32.3): a reducer batch that fails part-way is
// retried from the start of the batch, so successful prefix folds that were
// never persisted are recomputed. One batch adds 1, 2 and 3; the fold of 3
// fails once. Recovery must restart at 1 and persist 6, not resume at 2 and
// persist 5.
func TestKernelReducerFailedPrefixRecovery(t *testing.T) {
	f := newKernelFixture(t)
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[RecoveryAmountAdded](r); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[RecoveryBalance](r)
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu     sync.Mutex
		calls  []int
		failed atomic.Bool
	)
	if err = chronicle.RegisterReducerHandlers(r, model, "go-prefix-recovery", []reducers.Handler{
		reducers.On(func(_ context.Context, e RecoveryAmountAdded, current *RecoveryBalance, ec events.Context) (*RecoveryBalance, error) {
			mu.Lock()
			calls = append(calls, e.Amount)
			mu.Unlock()
			if e.Amount == 3 && failed.CompareAndSwap(false, true) {
				return nil, errors.New("deliberate one-time reducer failure")
			}
			next := &RecoveryBalance{ID: string(ec.SourceID), Amount: e.Amount}
			if current != nil {
				next.Amount += current.Amount
			}
			return next, nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(r).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.WaitForRegistration(f.ctx); err != nil {
		t.Fatal(err)
	}
	result, err := store.EventLog().AppendMany(f.ctx, "account", []any{RecoveryAmountAdded{1}, RecoveryAmountAdded{2}, RecoveryAmountAdded{3}})
	if err != nil || result.Err() != nil {
		t.Fatalf("append batch: %+v %v", result, err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !failed.Load() {
		select {
		case <-ctx.Done():
			t.Fatal("reducer never reached the failing fold")
		case <-ticker.C:
		}
	}
	// Request recovery explicitly once the kernel has recorded the failure;
	// its own retry policy may also run first. Either path must recompute 1.
	for retried := false; ; {
		if !retried {
			partitions, err := store.Observers().FailedPartitions(ctx, "go-prefix-recovery")
			if err != nil {
				t.Fatal(err)
			}
			for _, partition := range partitions {
				if partition.Partition() == "account" {
					if _, err := store.Observers().RetryPartition(ctx, "go-prefix-recovery", events.EventLog, "account"); err != nil {
						t.Fatal(err)
					}
					retried = true
				}
			}
		}
		value, err := readmodels.For(store.ReadModels(), model).Get(ctx, "account")
		if err != nil {
			t.Fatal(err)
		}
		if value.Exists && value.Value.Amount == 6 {
			break
		}
		select {
		case <-ctx.Done():
			mu.Lock()
			defer mu.Unlock()
			t.Fatalf("recovered balance %+v; folds %v", value, calls)
		case <-ticker.C:
		}
	}
	// Every recovery attempt reloads the persisted state, so later deliveries
	// must not fold the batch onto 6 again. Observe a bounded settling window.
	settle, stop := context.WithTimeout(f.ctx, 3*time.Second)
	defer stop()
	for settled := false; !settled; {
		select {
		case <-settle.Done():
			settled = true
		case <-ticker.C:
			value, err := readmodels.For(store.ReadModels(), model).Get(f.ctx, "account")
			if err != nil || !value.Exists || value.Value.Amount != 6 {
				t.Fatalf("balance changed after recovery: %+v %v", value, err)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	// The first attempt must have folded the prefix in the same batch, or the
	// witness does not exercise a mid-batch failure.
	if len(calls) < 4 || calls[0] != 1 || calls[1] != 2 || calls[2] != 3 {
		t.Fatalf("not a single mid-batch failure: folds %v", calls)
	}
	t.Logf("folds %v", calls)
	if calls[3] != 1 {
		t.Fatalf("recovery resumed at %d instead of the start of the failed batch: folds %v", calls[3], calls)
	}
}
