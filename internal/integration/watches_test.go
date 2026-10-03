//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type WatchedPersonNamed struct {
	Name string `json:"name"`
}
type WatchedPerson struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(WatchedPersonNamed)"`
}
type KernelModelReactor struct{ received chan WatchedPerson }

func (r *KernelModelReactor) Added(model WatchedPerson)    { r.received <- model }
func (r *KernelModelReactor) Modified(model WatchedPerson) { r.received <- model }

func TestKernelProjectionWatchAndReadModelReactor(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[WatchedPersonNamed](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[WatchedPerson](registry)
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan WatchedPerson, 8)
	failures := make(chan error, 8)
	if err := chronicle.RegisterReadModelReactor[*KernelModelReactor](registry, model, func() *KernelModelReactor { return &KernelModelReactor{received} }, reactors.WithReadModelErrorHandler(func(_ context.Context, err error) { failures <- err })); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	ctx, cancel := context.WithTimeout(fixture.ctx, 20*time.Second)
	defer cancel()
	store, err := client.EventStore(ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	sub, err := reader.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()
	source := events.SourceID(uuid.NewString())
	for _, name := range []string{"Ada", "Grace"} {
		appendSuccessfully(t, ctx, store, source, WatchedPersonNamed{Name: name})
		change, err := sub.Recv()
		if err != nil {
			t.Fatalf("watch after append: %v", err)
		}
		if change.Key != readmodels.Key(source) || !change.HasValue || change.Value.Name != name || change.Context.Namespace != chronicle.DefaultNamespace {
			t.Fatalf("watch: %+v", change)
		}
		select {
		case value := <-received:
			if value.Name != name || value.ID != string(source) {
				t.Fatalf("reactor: %+v", value)
			}
		case err := <-failures:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("reactor did not receive projection change")
		}
	}
	window, err := reader.Materialized().ObserveInstances(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := window.Close(); err != nil {
			t.Error(err)
		}
	}()
	values, err := window.Recv()
	if err != nil || len(values) != 1 || values[0].Name != "Grace" {
		t.Fatalf("initial window: %+v %v", values, err)
	}
	page, err := reader.Materialized().GetInstances(ctx, nil)
	if err != nil || len(page) != 1 || page[0].ID != string(source) {
		t.Fatalf("page: %+v %v", page, err)
	}
}
