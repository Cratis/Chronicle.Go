//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
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
type BalanceReducer struct{}

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
func (BalanceReducer) Delete(ReducedAccountDeleted, *ReducedBalance) *ReducedBalance { return nil }

func reducerIntegrationRegistry(t *testing.T, passive bool) (*chronicle.Registry, readmodels.Model[ReducedBalance]) {
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
	if err := chronicle.RegisterReducer[BalanceReducer](r, model, func() BalanceReducer { return BalanceReducer{} }, reducers.WithID("go-balance"), reducers.WithVersion("1")); err != nil {
		t.Fatal(err)
	}
	return r, model
}
func waitReduced(t *testing.T, ctx context.Context, reader *readmodels.Reader[ReducedBalance], source string, exists bool, amount int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var last readmodels.Instance[ReducedBalance]
	for {
		value, err := reader.Get(ctx, readmodels.Key(source))
		if err != nil {
			t.Fatalf("read failed; last model %+v (want exists=%v amount=%d): %v", last, exists, amount, err)
		}
		last = value
		if value.Exists == exists && (!exists || (value.Value.Amount == amount && value.Value.ID == source)) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("folded model did not converge: %+v: %v", value, ctx.Err())
		case <-ticker.C:
		}
	}
}
func TestKernelReducerMaterializationDeletionAndFailedPartition(t *testing.T) {
	f := newKernelFixture(t)
	r, model := reducerIntegrationRegistry(t, false)
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
	waitReduced(t, f.ctx, reader, "balance", false, 0)
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
func TestKernelPassiveReducerReadsFoldLocallyWithAbsenceDeletionAndFailure(t *testing.T) {
	f := newKernelFixture(t)
	r, model := reducerIntegrationRegistry(t, true)
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
