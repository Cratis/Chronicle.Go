//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/jobs"
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
	job, err := store.Observers().Replay(f.ctx, "default-account", events.EventLog)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.Jobs().WaitForTerminalOrAbsent(f.ctx, job.ID(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if terminal != nil && terminal.Status() != jobs.CompletedSuccessfully {
		t.Fatalf("replay status=%d", terminal.Status())
	}
	// Job absence is not success evidence. Read-back proves the replay result.
	replayed := awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m DefaultsAccount) bool { return m.Name == "renamed" && m.Balance == 43 })
	if !reflect.DeepEqual(second.Value, replayed.Value) {
		t.Fatalf("replay changed state: %+v", replayed)
	}
	appendSuccessfully(t, f.ctx, store, source, DefaultsRemoved{})
	awaitProjectionAbsent(t, f.ctx, reader, readmodels.Key(source))
	appendSuccessfully(t, f.ctx, store, source, DefaultsCreated{Name: "recreated"})
	recreated := awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m DefaultsAccount) bool { return m.Name == "recreated" })
	if recreated.Value.Balance != 42 || recreated.Value.Address.City != "Oslo" {
		t.Fatal("recreation lost defaults")
	}
}

func TestKernelReadModelScenarioRefusesUnfaithfulProjectionDefaults(t *testing.T) {
	f := newKernelFixture(t)
	registry, _, declaration := defaultsRegistry(t)
	scenario, err := chronicletest.OpenReadModelScenario[DefaultsAccount](f.ctx, chronicletest.Config{Registry: registry, Store: f.storeName, Engine: chronicletest.Kernel, ConnectionString: f.endpoint, Development: true}, chronicletest.ReadModelOptions[DefaultsAccount]{Projection: &declaration})
	if scenario != nil || !errors.Is(err, chronicletest.ErrFidelityUnavailable) {
		t.Fatalf("unfaithful scenario accepted: %v %v", scenario, err)
	}
}
