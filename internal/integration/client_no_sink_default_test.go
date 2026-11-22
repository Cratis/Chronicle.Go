// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//go:build integration

package integration_test

import (
	"context"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

func TestKernelGlobalNoneReadsActiveProjectionBySource(t *testing.T) {
	f := newKernelFixture(t)
	r, model := immediateProjectionRegistry(t)
	client := f.client(r, chronicle.WithDefaultSinkType(readmodels.NoSink), chronicle.WithNamingPolicy(serialization.CamelCase))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	if store.ReadModels().Catalog().Descriptors()[0].Sink().Type != readmodels.NoSink {
		t.Fatal("None default not registered")
	}
	assertRegisteredNoneProjection(t, f, model.Identifier())
	reader := readmodels.For(store.ReadModels(), model)
	appendSuccessfully(t, f.ctx, store, events.SourceID("first"), SinkDefaultsCreated{Name: "one"})
	appendSuccessfully(t, f.ctx, store, events.SourceID("second"), SinkDefaultsCreated{Name: "two"})
	assertImmediateSources(t, f.ctx, reader, "one")
	appendSuccessfully(t, f.ctx, store, events.SourceID("first"), SinkDefaultsCreated{Name: "updated"})
	assertImmediateSources(t, f.ctx, reader, "updated")
}

func assertImmediateSources(t *testing.T, ctx context.Context, reader *readmodels.Reader[SinkDefaultsAccount], first string) {
	t.Helper()
	for source, name := range map[string]string{"first": first, "second": "two"} {
		result, err := reader.Get(ctx, readmodels.Key(source))
		if err != nil || !result.Exists {
			t.Fatalf("source=%s result=%#v error=%v", source, result, err)
		}
		if result.Value.ID != source || result.Value.Name != name {
			t.Fatalf("source=%s state=%#v", source, result.Value)
		}
	}
}

func assertRegisteredNoneProjection(t *testing.T, f *kernelFixture, model readmodels.Identifier) {
	t.Helper()
	definitions, err := modelcontracts.NewReadModelsClient(f.conn).GetDefinitions(f.ctx, &modelcontracts.GetDefinitionsRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	var registered *modelcontracts.ReadModelDefinition
	for _, definition := range definitions.GetReadModels() {
		if definition.GetType().GetIdentifier() == string(model) {
			registered = definition
		}
	}
	if registered == nil {
		t.Fatal("registered model missing", definitions)
	}
	if registered.GetSink().GetTypeId() != string(readmodels.NoSink) {
		t.Fatal("registered sink is not None", registered)
	}
	producers, err := projectioncontracts.NewProjectionsClient(f.conn).GetAllDefinitions(f.ctx, &projectioncontracts.GetAllDefinitionsRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	var producer *projectioncontracts.ProjectionDefinition
	for _, definition := range producers.GetItems() {
		if definition.GetReadModel() == string(model) {
			producer = definition
		}
	}
	if producer == nil {
		t.Fatal("registered projection missing", producers)
	}
	if !producer.GetIsActive() || !producer.GetIsRewindable() {
		t.Fatal("None changed authored producer flags", producers)
	}
}

func TestKernelGlobalNoneKeepsExplicitMongoDBOverride(t *testing.T) {
	f := newKernelFixture(t)
	r, model := immediateProjectionRegistry(t, readmodels.WithSink(readmodels.Sink{Type: readmodels.MongoDB}))
	client := f.client(r, chronicle.WithDefaultSinkType(readmodels.NoSink), chronicle.WithNamingPolicy(serialization.CamelCase))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	if store.ReadModels().Catalog().Descriptors()[0].Sink().Type != readmodels.MongoDB {
		t.Fatal("explicit materialized sink lost")
	}
	appendSuccessfully(t, f.ctx, store, events.SourceID("source"), SinkDefaultsCreated{Name: "materialized"})
	awaitProjection(t, f.ctx, reader, readmodels.Key("source"), func(value SinkDefaultsAccount) bool {
		return value.Name == "materialized"
	})
}

func TestKernelGlobalNoneKeepsPassiveReducerLocal(t *testing.T) {
	f := newKernelFixture(t)
	r, model, _ := reducerIntegrationRegistry(t, true)
	client := f.client(r, chronicle.WithDefaultSinkType(readmodels.NoSink))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, f.ctx, store, events.SourceID("account"), ReducedAmountChanged{-100})
	appendSuccessfully(t, f.ctx, store, events.SourceID("account"), ReducedAmountChanged{40})
	reader := readmodels.For(store.ReadModels(), model)
	result, err := reader.Get(f.ctx, readmodels.Key("account"))
	if err != nil || !result.Exists {
		t.Fatalf("passive None fold=%#v error=%v", result, err)
	}
	if result.Value.Amount != -80 {
		t.Fatal("passive None fold differs", result.Value)
	}
}

func immediateProjectionRegistry(t *testing.T, options ...readmodels.ModelOption) (*chronicle.Registry, readmodels.Model[SinkDefaultsAccount]) {
	t.Helper()
	r := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[SinkDefaultsCreated](r)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[SinkDefaultsAccount](r, options...)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(model, projections.FromEvent(event))); err != nil {
		t.Fatal(err)
	}
	return r, model
}
