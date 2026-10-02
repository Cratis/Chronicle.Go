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
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type ProjectionAccountOpened struct {
	FullName    string `json:"fullName"`
	ProductName string `json:"productName"`
	Local       string `json:"local"`
	Excluded    string `json:"excluded"`
}
type ProjectionAccountRenamed struct {
	Name string `json:"name"`
}
type ProjectionAccount struct {
	ID          uuid.UUID `json:"id" chronicle:"key"`
	Name        string    `json:"name" chronicle:"set(ProjectionAccountOpened,from=fullName);set(ProjectionAccountRenamed)"`
	ProductName string    `json:"productName"`
	Occurred    time.Time `json:"occurred" chronicle:"context(ProjectionAccountOpened)"`
	State       string    `json:"state" chronicle:"value(ProjectionAccountOpened,value=\"active\")"`
	Number      int32     `json:"number" chronicle:"value(ProjectionAccountOpened,value=42)"`
	Enabled     bool      `json:"enabled" chronicle:"value(ProjectionAccountOpened,value=true)"`
	Note        *string   `json:"note" chronicle:"value(ProjectionAccountOpened,value=\"present\");value(ProjectionAccountRenamed,value=null)"`
	Local       string    `json:"local" chronicle:"not-projected"`
	Excluded    string    `json:"excluded" chronicle:"no-auto"`
}
type PassiveProjectionAccount ProjectionAccount

func TestKernelProjectionMaterializesModelBoundAndPassiveReads(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ProjectionAccountOpened](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ProjectionAccountRenamed](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[ProjectionAccount](registry)
	if err != nil {
		t.Fatal(err)
	}
	passive, err := chronicle.RegisterReadModel[PassiveProjectionAccount](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(projections.ModelBound(passive, projections.Passive())); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := uuid.New()
	occurred := time.Date(2026, time.March, 12, 10, 30, 0, 0, time.UTC)
	result, err := store.EventLog().Append(fixture.ctx, events.SourceID(source.String()), ProjectionAccountOpened{FullName: "Ada", ProductName: "Chronicle", Local: "must not map", Excluded: "must not map"}, eventsequences.WithOccurred(occurred))
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Err(); err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	first := awaitProjection(t, fixture.ctx, reader, readmodels.Key(source.String()), func(value ProjectionAccount) bool { return value.Name == "Ada" && value.Note != nil })
	if first.Value.ID != source || first.Value.ProductName != "Chronicle" || first.Value.State != "active" || first.Value.Number != 42 || !first.Value.Enabled || !first.Value.Occurred.Equal(occurred) || first.Value.Local != "" || first.Value.Excluded != "" || *first.Value.Note != "present" {
		t.Fatalf("materialized mappings: %+v", first)
	}
	immediate, err := readmodels.For(store.ReadModels(), passive).Get(fixture.ctx, readmodels.Key(source.String()))
	if err != nil || !immediate.Exists || immediate.Value.Name != "Ada" || immediate.Value.ProductName != "Chronicle" || immediate.Value.State != "active" {
		t.Fatalf("passive: %+v %v", immediate, err)
	}
	result, err = store.EventLog().Append(fixture.ctx, events.SourceID(source.String()), ProjectionAccountRenamed{Name: "Grace"})
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Err(); err != nil {
		t.Fatal(err)
	}
	second := awaitProjection(t, fixture.ctx, reader, readmodels.Key(source.String()), func(value ProjectionAccount) bool { return value.Name == "Grace" && value.Note == nil })
	if second.Value.ProductName != "Chronicle" || second.Value.State != "active" {
		t.Fatal("unmapped state was not preserved")
	}
	other, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace("isolated"))
	if err != nil {
		t.Fatal(err)
	}
	absent, err := readmodels.For(other.ReadModels(), model).Get(fixture.ctx, readmodels.Key(source.String()))
	if err != nil || absent.Exists {
		t.Fatalf("namespace leak: %+v %v", absent, err)
	}
}

// Projection sinks are eventually consistent. Poll observable state, bounded by
// both the parent and a 15-second deadline; never infer readiness from a blind sleep.
func awaitProjection[T any](t *testing.T, parent context.Context, reader *readmodels.Reader[T], key readmodels.Key, ready func(T) bool) readmodels.Instance[T] {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		instance, err := reader.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if instance.Exists && ready(instance.Value) {
			return instance
		}
		select {
		case <-ctx.Done():
			t.Fatalf("projection did not materialize: %+v: %v", instance, ctx.Err())
		case <-ticker.C:
		}
	}
}
