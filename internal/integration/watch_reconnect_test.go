//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestKernelWatchInterruptionRequiresExplicitRewatch(t *testing.T) {
	fixture := newKernelFixture(t)
	uri, err := chronicle.ParseConnectionString(fixture.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	relay := newRelay(t, uri.Addresses()[0].String())
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[WatchedPersonNamed](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[WatchedPerson](registry)
	if err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry, chronicle.WithConnectionString("chronicle://"+relay.listener.Addr().String()))
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	watch, err := reader.Watch(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := watch.Close(); err != nil {
			t.Error(err)
		}
	}()
	appendSuccessfully(t, fixture.ctx, store, "person", WatchedPersonNamed{Name: "before"})
	if change, err := watch.Recv(); err != nil || change.Value.Name != "before" {
		t.Fatalf("before: %+v %v", change, err)
	}
	relay.cut()
	if _, err := watch.Recv(); !errors.Is(err, readmodels.ErrInterrupted) {
		t.Fatalf("transport cut did not terminate watch: %v", err)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		outcome, err := store.WaitForRegistration(fixture.ctx)
		if err == nil && outcome.IsSuccess() && outcome.Generation > before.Generation {
			break
		}
		select {
		case <-ticker.C:
		case <-fixture.ctx.Done():
			t.Fatal(fixture.ctx.Err())
		}
	}
	if _, err := watch.Recv(); !errors.Is(err, readmodels.ErrInterrupted) {
		t.Fatal("old subscription silently resumed")
	}
	next, err := reader.Watch(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := next.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Refetch separately: readiness alone does not supply a current snapshot.
	instance, err := reader.Get(fixture.ctx, "person")
	if err != nil || !instance.Exists || instance.Value.Name != "before" {
		t.Fatalf("refetch: %+v %v", instance, err)
	}
	appendSuccessfully(t, fixture.ctx, store, "person", WatchedPersonNamed{Name: "after"})
	change, err := next.Recv()
	if err != nil || change.Value.Name != "after" {
		t.Fatalf("new subscription: %+v %v", change, err)
	}
}
