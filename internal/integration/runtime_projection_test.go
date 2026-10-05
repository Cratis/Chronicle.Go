//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type RuntimeItemCreated struct {
	Name string `json:"name"`
}
type RuntimeOriginal struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(RuntimeItemCreated)"`
}
type RuntimeAdded struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(RuntimeItemCreated)"`
}

func TestKernelRuntimeProjectionAdditionRetainsDefinitionsAcrossReconnectAndNamespace(t *testing.T) {
	f := newKernelFixture(t)
	uri, err := chronicle.ParseConnectionString(f.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	relay := newRelay(t, uri.Addresses()[0].String())
	registry := chronicle.NewRegistry()
	if _, err = chronicle.RegisterEvent[RuntimeItemCreated](registry); err != nil {
		t.Fatal(err)
	}
	var classifications atomic.Int32
	classification := readmodels.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		classifications.Add(1)
		return compliance.Classification{}, nil
	}))
	original, err := chronicle.RegisterReadModel[RuntimeOriginal](registry, classification)
	if err != nil {
		t.Fatal(err)
	}
	client := f.client(registry, chronicle.WithConnectionString("chronicle://"+relay.listener.Addr().String()))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Ready(f.ctx); err != nil {
		t.Fatal(err)
	}
	old := store.ReadModels()
	added, err := readmodels.Define[RuntimeAdded](classification)
	if err != nil {
		t.Fatal(err)
	}
	calls := classifications.Load()
	result, err := store.RegisterProjection(f.ctx, projections.ModelBound(added))
	if err != nil || !result.Published || !result.Outcome.IsSuccess() {
		t.Fatalf("runtime registration not acknowledged: %+v %v", result, err)
	}
	appendAndRead := func(target *chronicle.EventStore, name string) {
		t.Helper()
		source := uuid.NewString()
		appendSuccessfully(t, f.ctx, target, events.SourceID(source), RuntimeItemCreated{Name: name})
		awaitProjection(t, f.ctx, readmodels.For(target.ReadModels(), added), readmodels.Key(source), func(model RuntimeAdded) bool { return model.Name == name })
		awaitProjection(t, f.ctx, readmodels.For(target.ReadModels(), original), readmodels.Key(source), func(model RuntimeOriginal) bool { return model.Name == name })
	}
	appendAndRead(store, "after-add")
	if len(old.Catalog().Descriptors()) != 1 {
		t.Fatal("old reader catalog mutated")
	}
	relay.cut()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		outcome, waitErr := store.WaitForRegistration(f.ctx)
		if waitErr == nil && outcome.IsSuccess() && outcome.Generation > result.Outcome.Generation {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		case <-ticker.C:
		}
	}
	appendAndRead(store, "after-reconnect")
	namespace, err := client.EventStore(f.ctx, f.storeName, chronicle.WithNamespace("rt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(namespace.ReadModels().Catalog().Descriptors()) != 2 {
		t.Fatal("new namespace lost addition")
	}
	appendAndRead(namespace, "namespace")
	if classifications.Load() != calls {
		t.Fatal("registration/reconnect repeated classification callbacks")
	}
}
