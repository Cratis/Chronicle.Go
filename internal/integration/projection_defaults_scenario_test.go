//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

func TestKernelReadModelScenarioMaterializedDefaultsMatchKernel(t *testing.T) {
	t.Run("create_rename_remove_recreate", func(t *testing.T) {
		f := newKernelFixture(t)
		ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
		defer cancel()
		r, m, _ := defaultsRegistry(t)
		s, err := chronicletest.OpenReadModelScenario[DefaultsAccount](ctx, chronicletest.Config{Registry: r, Engine: chronicletest.Kernel, Store: f.storeName, ConnectionString: f.endpoint, Development: true, Naming: serialization.CamelCase}, chronicletest.ReadModelOptions[DefaultsAccount]{Materialized: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		// Independent production client, same real definition and events. Neither
		// reader polls sink content or supplies an initial state overlay.
		store, err := f.client(r, chronicle.WithNamingPolicy(serialization.CamelCase)).EventStore(ctx, f.storeName)
		if err != nil {
			t.Fatal(err)
		}
		reader := readmodels.For(store.ReadModels(), m)
		for i, event := range []any{DefaultsCreated{Name: "created"}, DefaultsRenamed{Name: "renamed"}, DefaultsRemoved{}, DefaultsCreated{Name: "recreated"}} {
			if err := s.Given(ctx, "account", event); err != nil {
				t.Fatal(err)
			}
			got, err := s.InstanceFor(ctx, "account")
			if err != nil {
				t.Fatal(err)
			}
			production, err := reader.Get(ctx, "account")
			if err != nil {
				t.Fatal(err)
			}
			if got.Exists != production.Exists || !reflect.DeepEqual(got.Value, production.Value) {
				t.Fatalf("step %d: scenario=%+v production=%+v", i, got, production)
			}
			if i == 2 {
				if got.Exists {
					t.Fatal("removal left state")
				}
			} else {
				balance := int32(42)
				if i == 1 {
					balance = 43
				}
				if !got.Exists || got.Value.Balance != balance || got.Value.Address.City != "Oslo" || got.Value.Items == nil || len(got.Value.Items) != 0 || got.Value.Note != nil {
					t.Fatalf("defaults at step %d: %+v", i, got)
				}
			}
			missing, err := s.InstanceFor(ctx, "never-created")
			if err != nil || missing.Exists {
				t.Fatalf("never-created: %+v %v", missing, err)
			}
			productionMissing, err := reader.Get(ctx, "never-created")
			if err != nil || productionMissing.Exists {
				t.Fatalf("production never-created: %+v %v", productionMissing, err)
			}
		}
	})
	for _, aggregateOnly := range []bool{false, true} {
		name := "AutoMap"
		if aggregateOnly {
			name = "aggregate-only"
		}
		t.Run(name, func(t *testing.T) {
			f := newKernelFixture(t)
			ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
			defer cancel()
			r := chronicle.NewRegistry()
			e, err := chronicle.RegisterEvent[DefaultsCreated](r)
			if err != nil {
				t.Fatal(err)
			}
			m, err := chronicle.RegisterReadModel[DefaultsAutoAccount](r)
			if err != nil {
				t.Fatal(err)
			}
			b := projections.NewBuilder("scenario-default-automap", m, projections.WithInitialValues(DefaultsAutoAccount{Name: "default", Balance: 41}))
			projections.From(b, e, func(from *projections.FromBuilder[DefaultsAutoAccount, DefaultsCreated]) {
				if aggregateOnly {
					projections.Increment(from, projections.Path[DefaultsAutoAccount, int32]("Balance"))
				}
			})
			d, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
			if err = r.AddProjection(d); err != nil {
				t.Fatal(err)
			}
			s, err := chronicletest.OpenReadModelScenario[DefaultsAutoAccount](ctx, chronicletest.Config{Registry: r, Engine: chronicletest.Kernel, Store: f.storeName, ConnectionString: f.endpoint, Development: true, Naming: serialization.CamelCase}, chronicletest.ReadModelOptions[DefaultsAutoAccount]{Materialized: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err = s.Given(ctx, "account", DefaultsCreated{Name: "event name"}); err != nil {
				t.Fatal(err)
			}
			got, err := s.Instance(ctx)
			if err != nil {
				t.Fatal(err)
			}
			store, err := f.client(r, chronicle.WithNamingPolicy(serialization.CamelCase)).EventStore(ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			production, err := readmodels.For(store.ReadModels(), m).Get(ctx, "account")
			if err != nil {
				t.Fatal(err)
			}
			wantName, wantBalance := "event name", int32(41)
			if aggregateOnly {
				wantName, wantBalance = "default", 42
			}
			if !got.Exists || got.Value.Name != wantName || got.Value.Balance != wantBalance || !reflect.DeepEqual(got.Value, production.Value) {
				t.Fatalf("scenario=%+v production=%+v", got, production)
			}
		})
	}
}
