//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type ReducedAmountChanged struct {
	Amount int `json:"amount"`
}
type ReducedAccountDeleted struct{}
type ReducedBalance struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}
type BalanceReducer struct{ deletions *atomic.Uint32 }

// Reducer required: negative balances recover at half rate, a prior-state branch
// which projection counter operators cannot express.
func (BalanceReducer) Change(event ReducedAmountChanged, current *ReducedBalance, ec events.Context) (*ReducedBalance, error) {
	if event.Amount == 999 {
		return nil, errors.New("deliberate reducer integration failure")
	}
	amount := event.Amount
	if current != nil {
		if current.Amount < 0 && amount > 0 {
			amount /= 2
		}
		amount += current.Amount
	}
	return &ReducedBalance{string(ec.SourceID), amount}, nil
}
func (r BalanceReducer) Delete(ReducedAccountDeleted, *ReducedBalance) *ReducedBalance {
	r.deletions.Add(1)
	return nil
}

func reducerIntegrationRegistry(t *testing.T, passive bool) (*chronicle.Registry, readmodels.Model[ReducedBalance], *atomic.Uint32) {
	t.Helper()
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ReducedAmountChanged](r); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ReducedAccountDeleted](r); err != nil {
		t.Fatal(err)
	}
	var options []readmodels.ModelOption
	if passive {
		options = append(options, readmodels.Passive())
	}
	model, err := chronicle.RegisterReadModel[ReducedBalance](r, options...)
	if err != nil {
		t.Fatal(err)
	}
	deletions := &atomic.Uint32{}
	if err := chronicle.RegisterReducer[BalanceReducer](r, model, func() BalanceReducer { return BalanceReducer{deletions} }, reducers.WithID("go-balance"), reducers.WithVersion("1")); err != nil {
		t.Fatal(err)
	}
	return r, model, deletions
}
func waitReduced(t *testing.T, ctx context.Context, reader *readmodels.Reader[ReducedBalance], source string, exists bool, amount int) {
	t.Helper()
	if last, err := pollReduced(ctx, reader, source, exists, amount); err != nil {
		t.Fatalf("last model %+v (want exists=%v amount=%d): %v", last, exists, amount, err)
	}
}
func pollReduced(ctx context.Context, reader *readmodels.Reader[ReducedBalance], source string, exists bool, amount int) (readmodels.Instance[ReducedBalance], error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var last readmodels.Instance[ReducedBalance]
	for {
		value, err := reader.Get(ctx, readmodels.Key(source))
		if err != nil {
			return last, fmt.Errorf("read reduced model: %w", err)
		}
		last = value
		if value.Exists == exists && (!exists || (value.Value.Amount == amount && value.Value.ID == source)) {
			return value, nil
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-ticker.C:
		}
	}
}
func TestKernelReducerMaterializationDeletionAndFailedPartition(t *testing.T) {
	f := newKernelFixture(t)
	r, model, deletions := reducerIntegrationRegistry(t, false)
	client := f.client(r)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	appendSuccessfully(t, f.ctx, store, "balance", ReducedAmountChanged{-10})
	appendSuccessfully(t, f.ctx, store, "balance", ReducedAmountChanged{8})
	waitReduced(t, f.ctx, reader, "balance", true, -6)
	appendSuccessfully(t, f.ctx, store, "balance", ReducedAccountDeleted{})
	if last, err := pollReduced(f.ctx, reader, "balance", false, 0); err != nil {
		// Inspect while the client is still connected, using a fresh, bounded
		// diagnostic context rather than the expired materialization poll.
		ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
		defer cancel()
		info, infoErr := contracts.NewObserversClient(f.conn).GetObserverInformation(ctx, &contracts.GetObserverInformationRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), EventSequenceId: "event-log", ObserverId: "go-balance"})
		failures, failuresErr := contracts.NewFailedPartitionsClient(f.conn).GetFailedPartitions(ctx, &contracts.GetFailedPartitionsRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "go-balance"})
		history, historyErr := store.EventLog().ReadSource(ctx, "balance", eventsequences.SourceFilter{})
		if infoErr == nil && failuresErr == nil && historyErr == nil && f.ctx.Err() == nil &&
			strandedReducerDeletion(err, last, deletions.Load(), info, failures, history) {
			t.Skip("kernel reuses a finishing catch-up job and never delivers deletion event 2: https://github.com/Cratis/Chronicle/issues/4548")
		}
		t.Fatalf("store %s last %+v error %v observer %v (%v), failures %v (%v), history %v (%v), deletion calls %d", f.storeName, last, err, info, infoErr, failures, failuresErr, history, historyErr, deletions.Load())
	}
	appendSuccessfully(t, f.ctx, store, "balance", ReducedAmountChanged{3})
	waitReduced(t, f.ctx, reader, "balance", true, 3)
	appendSuccessfully(t, f.ctx, store, "bad", ReducedAmountChanged{999})
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	failures := contracts.NewFailedPartitionsClient(f.conn)
	for {
		response, err := failures.GetFailedPartitions(ctx, &contracts.GetFailedPartitionsRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: "go-balance"})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, partition := range response.GetItems() {
			if partition.Partition == "bad" {
				for _, attempt := range partition.Attempts {
					for _, message := range attempt.Messages {
						if strings.Contains(message, "deliberate reducer integration failure") {
							found = true
						}
					}
				}
			}
		}
		if found {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("kernel did not record reducer failure", ctx.Err())
		case <-ticker.C:
		}
	}
	if err := store.UnregisterReducer(f.ctx, "go-balance"); err != nil {
		t.Fatal(err)
	}
}

// strandedReducerDeletion recognizes only the reproduced 19.29.4 catch-up race.
// A delivered deletion, a failed/disconnected observer, a different watermark,
// missing history, or an ordinary RPC failure must remain a test failure.
func strandedReducerDeletion(err error, last readmodels.Instance[ReducedBalance], deletions uint32, info *contracts.ObserverInformation, failures *contracts.IEnumerable_FailedPartition, history []events.Appended) bool {
	if !errors.Is(err, context.DeadlineExceeded) || deletions != 0 ||
		!last.Exists || last.Value != (ReducedBalance{ID: "balance", Amount: -6}) || last.LastHandled == nil || *last.LastHandled != 1 ||
		info == nil || info.Id != "go-balance" || info.Type != contracts.ObserverType_Reducer ||
		info.EventSequenceId != "event-log" || info.Owner != contracts.ObserverOwner_Client ||
		info.RunningState != contracts.ObserverRunningState_Active || !info.IsSubscribed ||
		info.LastHandledEventSequenceNumber != 1 || info.NextEventSequenceNumber != 2 ||
		info.TailEventSequenceNumber != 2 || info.HandledEventCount != 2 ||
		failures == nil || len(failures.Items) != 0 || len(history) != 3 {
		return false
	}
	for i, id := range []events.TypeID{"ReducedAmountChanged", "ReducedAmountChanged", "ReducedAccountDeleted"} {
		ec := history[i].Context
		if ec.SequenceNumber != events.SequenceNumber(i) || ec.SourceID != "balance" || ec.EventType != (events.TypeRef{ID: id, Generation: 1}) {
			return false
		}
	}
	var content map[string]json.RawMessage
	return json.Unmarshal(history[2].Content, &content) == nil && content != nil && len(content) == 0
}

func TestKernelPassiveReducerReadsFoldLocallyWithAbsenceDeletionAndFailure(t *testing.T) {
	f := newKernelFixture(t)
	r, model, _ := reducerIntegrationRegistry(t, true)
	client := f.client(r)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	absent, err := reader.Get(f.ctx, "unseeded")
	if err != nil || absent.Exists {
		t.Fatal(absent, err)
	}
	appendSuccessfully(t, f.ctx, store, "balance", ReducedAmountChanged{-10})
	appendSuccessfully(t, f.ctx, store, "balance", ReducedAmountChanged{8})
	// Passive reads require no materialization poll.
	state, err := reader.Get(f.ctx, "balance")
	if err != nil || !state.Exists || state.Value.Amount != -6 {
		t.Fatal(state, err)
	}
	appendSuccessfully(t, f.ctx, store, "balance", ReducedAccountDeleted{})
	absent, err = reader.Get(f.ctx, "balance")
	if err != nil || absent.Exists {
		t.Fatal(absent, err)
	}
	appendSuccessfully(t, f.ctx, store, "balance", ReducedAmountChanged{999})
	state, err = reader.Get(f.ctx, "balance")
	if err == nil || state.Exists {
		t.Fatal(state, err)
	}
}
