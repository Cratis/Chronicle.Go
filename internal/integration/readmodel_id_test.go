//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type NamingFixAccountOpened struct {
	ID   string
	Name string
}

type NamingFixAccount struct {
	ID   string
	Name string
}

func TestKernelProjectionReadsUntaggedIDWithDefaultNaming(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[NamingFixAccountOpened](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[NamingFixAccount](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(projections.ModelBound(model, projections.FromEvent(event))); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := uuid.NewString()
	appendSuccessfully(t, fixture.ctx, store, events.SourceID(source), NamingFixAccountOpened{ID: source, Name: "Ada"})
	result := awaitProjection(t, fixture.ctx, readmodels.For(store.ReadModels(), model), readmodels.Key(source), func(value NamingFixAccount) bool { return value.Name == "Ada" })
	if result.Value.ID != source {
		raw, err := contracts.NewReadModelsClient(fixture.conn).GetInstanceByKey(fixture.ctx, &contracts.GetInstanceByKeyRequest{EventStore: string(fixture.storeName), Namespace: string(chronicle.DefaultNamespace), ReadModelIdentifier: string(model.Identifier()), EventSequenceId: "event-log", ReadModelKey: source})
		t.Fatalf("read-back ID = %q, want %q; raw=%+v error=%v", result.Value.ID, source, raw, err)
	}
}
