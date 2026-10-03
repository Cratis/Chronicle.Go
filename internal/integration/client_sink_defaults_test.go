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
	"github.com/cratis/chronicle.go/serialization"
)

type SinkDefaultsCreated struct{ Name string }
type SinkDefaultsAccount struct{ ID, Name string }

func TestKernelClientDefaultSinkMongoAndExplicitMongoOverrideSQL(t *testing.T) {
	for _, explicitMongo := range []bool{false, true} {
		name := "unchanged_mongo_default"
		if explicitMongo {
			name = "sql_default_explicit_mongo_override"
		}
		t.Run(name, func(t *testing.T) {
			f := newKernelFixture(t)
			r := chronicle.NewRegistry()
			ev, err := chronicle.RegisterEvent[SinkDefaultsCreated](r)
			if err != nil {
				t.Fatal(err)
			}
			var modelOptions []readmodels.ModelOption
			var clientOptions []chronicle.ClientOption
			if explicitMongo {
				modelOptions = append(modelOptions, readmodels.WithSink(readmodels.Sink{Type: readmodels.MongoDB}))
				clientOptions = append(clientOptions, chronicle.WithDefaultSinkType(readmodels.SQL))
			}
			model, err := chronicle.RegisterReadModel[SinkDefaultsAccount](r, modelOptions...)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.AddProjection(projections.ModelBound(model, projections.FromEvent(ev))); err != nil {
				t.Fatal(err)
			}
			clientOptions = append(clientOptions, chronicle.WithNamingPolicy(serialization.CamelCase))
			client := f.client(r, clientOptions...)
			_, offline, err := client.Catalogs(f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			store, err := client.EventStore(f.ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			if offline.Descriptors()[0].Sink().Type != readmodels.MongoDB || store.ReadModels().Catalog().Descriptors()[0].Sink() != offline.Descriptors()[0].Sink() {
				t.Fatal("final catalog mismatch")
			}
			definitions, err := contracts.NewReadModelsClient(f.conn).GetDefinitions(f.ctx, &contracts.GetDefinitionsRequest{EventStore: string(f.storeName)})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, definition := range definitions.ReadModels {
				if definition.GetType().GetIdentifier() == string(model.Identifier()) {
					found = true
					if definition.GetSink().GetTypeId() != "MongoDB" {
						t.Fatal("server sink precedence differs", definition.GetSink())
					}
				}
			}
			if !found {
				t.Fatal("model definition absent")
			}
			appendSuccessfully(t, f.ctx, store, events.SourceID("account"), SinkDefaultsCreated{Name: "materialized"})
			got := awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), model), "account", func(m SinkDefaultsAccount) bool { return m.Name == "materialized" })
			if got.Value.ID != "account" {
				t.Fatal("original typed handle lost key")
			}
		})
	}
	// This witness materializes MongoDB only. SQL/InMemory wire selection does
	// not qualify a live backend; no such provider is provisioned by this fixture.
}
