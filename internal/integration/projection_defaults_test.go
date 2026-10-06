//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	jobcontracts "github.com/cratis/chronicle.go/contracts/jobs"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type DefaultsCreated struct{ Name string }
type DefaultsRenamed struct{ Name string }
type DefaultsRemoved struct{}
type DefaultsAccount struct {
	ID      string
	Name    string
	Balance int32
	Items   []string
	Note    *string
	Address DefaultsAddress
}
type DefaultsAddress struct{ City string }

func defaultsRegistry(t *testing.T) (*chronicle.Registry, readmodels.Model[DefaultsAccount], projections.Declaration) {
	t.Helper()
	registry := chronicle.NewRegistry()
	created, err := chronicle.RegisterEvent[DefaultsCreated](registry)
	if err != nil {
		t.Fatal(err)
	}
	renamed, err := chronicle.RegisterEvent[DefaultsRenamed](registry)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := chronicle.RegisterEvent[DefaultsRemoved](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[DefaultsAccount](registry)
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("default-account", model,
		projections.WithInitialValues(DefaultsAccount{Name: "default", Balance: 41, Items: []string{}, Address: DefaultsAddress{City: "Oslo"}}),
		projections.WithInitialValue(projections.Path[DefaultsAccount, *string]("Note"), nil),
		projections.WithLabels("accounts", "accounts", "metadata-only"))
	projections.From(builder, created, func(from *projections.FromBuilder[DefaultsAccount, DefaultsCreated]) {
		projections.Increment(from, projections.Path[DefaultsAccount, int32]("Balance"))
		projections.Map(from, projections.Path[DefaultsAccount, string]("Name"), projections.Path[DefaultsCreated, string]("Name"))
	})
	projections.From(builder, renamed, func(from *projections.FromBuilder[DefaultsAccount, DefaultsRenamed]) {
		projections.Increment(from, projections.Path[DefaultsAccount, int32]("Balance"))
		projections.Map(from, projections.Path[DefaultsAccount, string]("Name"), projections.Path[DefaultsRenamed, string]("Name"))
	})
	builder.Configure(projections.RemovedWith(removed))
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	return registry, model, declaration
}

func TestKernelProjectionDefaultsMaterializationRemovalAndReplay(t *testing.T) {
	f := newKernelFixture(t)
	registry, model, _ := defaultsRegistry(t)
	store, err := f.client(registry, chronicle.WithNamingPolicy(serialization.CamelCase)).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	source := events.SourceID(uuid.NewString())
	absent, err := reader.Get(f.ctx, readmodels.Key(source))
	if err != nil || absent.Exists {
		t.Fatalf("registration created state: %+v %v", absent, err)
	}
	appendSuccessfully(t, f.ctx, store, source, DefaultsCreated{Name: "created"})
	first := awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m DefaultsAccount) bool { return m.Name == "created" })
	if first.Value.Balance != 42 || first.Value.Items == nil || len(first.Value.Items) != 0 || first.Value.Note != nil || first.Value.Address.City != "Oslo" {
		t.Fatalf("initial values: %+v", first)
	}
	appendSuccessfully(t, f.ctx, store, source, DefaultsRenamed{Name: "renamed"})
	second := awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m DefaultsAccount) bool { return m.Name == "renamed" })
	if second.Value.Balance != 43 {
		t.Fatal("mapping discarded initial state")
	}
	appendSuccessfully(t, f.ctx, store, source, DefaultsRemoved{})
	awaitProjectionAbsent(t, f.ctx, reader, readmodels.Key(source))
	appendSuccessfully(t, f.ctx, store, source, DefaultsCreated{Name: "recreated"})
	recreated := awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m DefaultsAccount) bool { return m.Name == "recreated" })
	if recreated.Value.Balance != 42 || recreated.Value.Address.City != "Oslo" {
		t.Fatal("recreation lost defaults")
	}
	t.Run("replay_completion", func(t *testing.T) {
		// Subscribe and receive the initial job snapshot before requesting replay,
		// so a short-lived terminal transition can be captured by its exact ID.
		ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
		defer cancel()
		stream, err := jobcontracts.NewJobsClient(f.conn).ObserveJobs(ctx, &jobcontracts.ObserveJobsRequest{EventStore: string(f.storeName), Namespace: string(chronicle.DefaultNamespace)})
		if err != nil {
			t.Fatal(err)
		}
		initial, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if err = wire.CheckEnvelope(initial); err != nil {
			t.Fatal(err)
		}
		handle, err := store.Observers().Replay(ctx, "default-account", events.EventLog)
		if err != nil {
			t.Fatal(err)
		}
		completed := false
		for !completed {
			snapshot, err := stream.Recv()
			if err != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) && f.ctx.Err() == nil {
					job, readErr := handle.Get(f.ctx)
					if readErr != nil {
						t.Fatal(readErr)
					}
					if job == nil {
						t.Skip("replay job disappeared without observable successful terminal status; unchanged read-back is not replay-completion evidence")
					}
				}
				t.Fatal("replay completion unavailable", err)
			}
			if err := wire.CheckEnvelope(snapshot); err != nil {
				t.Fatal(err)
			}
			for _, job := range snapshot.Data {
				if job == nil || job.Id == nil || uuid.UUID(wire.Correlation(job.Id)) != handle.ID() {
					continue
				}
				switch job.Status {
				case jobcontracts.JobStatus_CompletedWithFailures, jobcontracts.JobStatus_JOB_STATUS_Failed, jobcontracts.JobStatus_JOB_STATUS_Stopped:
					t.Fatalf("replay job %s status=%s", handle.ID(), job.Status)
				case jobcontracts.JobStatus_JOB_STATUS_CompletedSuccessfully:
					completed = true
				}
				for _, change := range job.StatusChanges {
					if change != nil && change.Status == jobcontracts.JobStatus_JOB_STATUS_CompletedSuccessfully {
						completed = true
					}
				}
			}
		}
		t.Logf("replay job %s: observed CompletedSuccessfully", handle.ID())
		replayed := awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m DefaultsAccount) bool { return m.Name == "recreated" && m.Balance == 42 })
		if !reflect.DeepEqual(recreated.Value, replayed.Value) {
			t.Fatalf("completed replay changed state: %+v", replayed)
		}
	})
}

type DefaultsAutoAccount struct {
	ID      string
	Name    string
	Balance int32
}

func TestKernelProjectionDefaultsAutoMapAndAggregateOnly(t *testing.T) {
	for _, aggregateOnly := range []bool{false, true} {
		name := "ordinary_automap"
		if aggregateOnly {
			name = "aggregate_only"
		}
		t.Run(name, func(t *testing.T) {
			f := newKernelFixture(t)
			registry := chronicle.NewRegistry()
			created, err := chronicle.RegisterEvent[DefaultsCreated](registry)
			if err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[DefaultsAutoAccount](registry)
			if err != nil {
				t.Fatal(err)
			}
			builder := projections.NewBuilder("default-automap", model, projections.WithInitialValues(DefaultsAutoAccount{Name: "default", Balance: 41}))
			projections.From(builder, created, func(from *projections.FromBuilder[DefaultsAutoAccount, DefaultsCreated]) {
				if aggregateOnly {
					projections.Increment(from, projections.Path[DefaultsAutoAccount, int32]("Balance"))
				}
			})
			declaration, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.AddProjection(declaration); err != nil {
				t.Fatal(err)
			}
			store, err := f.client(registry, chronicle.WithNamingPolicy(serialization.CamelCase)).EventStore(f.ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			source := events.SourceID(uuid.NewString())
			appendSuccessfully(t, f.ctx, store, source, DefaultsCreated{Name: "event name"})
			reader := readmodels.For(store.ReadModels(), model)
			got := awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m DefaultsAutoAccount) bool {
				if aggregateOnly {
					return m.Balance == 42
				}
				return m.Name == "event name"
			})
			wantName, wantBalance := "event name", int32(41)
			if aggregateOnly {
				// The kernel suppresses name AutoMap for aggregate-only mappings,
				// as it does for the equivalent C# definition.
				wantName, wantBalance = "default", 42
			}
			if got.Value.Name != wantName || got.Value.Balance != wantBalance {
				t.Fatalf("aggregateOnly=%v model=%+v", aggregateOnly, got.Value)
			}
		})
	}
}

func TestKernelReadModelScenarioNonMaterializedRefusesProjectionDefaults(t *testing.T) {
	f := newKernelFixture(t)
	registry, _, declaration := defaultsRegistry(t)
	scenario, err := chronicletest.OpenReadModelScenario[DefaultsAccount](f.ctx, chronicletest.Config{Registry: registry, Store: f.storeName, Engine: chronicletest.Kernel, ConnectionString: f.endpoint, Development: true}, chronicletest.ReadModelOptions[DefaultsAccount]{Projection: &declaration})
	if scenario != nil || !errors.Is(err, chronicletest.ErrFidelityUnavailable) {
		t.Fatalf("unfaithful scenario accepted: %v %v", scenario, err)
	}
}
