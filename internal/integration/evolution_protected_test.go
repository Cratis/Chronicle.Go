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
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type ProtectedPersonV1 struct {
	Owner  string `chronicle:"subject"`
	Name   string `chronicle:"pii"`
	Secret string `chronicle:"encrypted(scope=namespace)"`
}
type ProtectedPerson struct {
	Owner     string `chronicle:"subject"`
	FirstName string `chronicle:"pii"`
	LastName  string `chronicle:"pii"`
	Secret    string `chronicle:"encrypted(scope=namespace)"`
}
type ProtectedPersonModel struct {
	ID        string `json:"id" chronicle:"key"`
	Owner     string `chronicle:"subject;set(ProtectedPerson)"`
	FirstName string `chronicle:"pii;set(ProtectedPerson)"`
	LastName  string `chronicle:"pii;set(ProtectedPerson)"`
}

// Characterizes the kernel since Chronicle#4623: migrations transform plaintext
// and protect every generation for the original subject, and the kernel releases
// protected values in every generation of GenerationalContent, so the SDK decodes
// alternate generations and admits historical protected observers. Released
// values stay released after erasure of the subject's PII.
func TestKernelProtectedEventGenerationMigrationProfile(t *testing.T) {
	f := newKernelFixture(t)
	subject := uuid.NewString()
	appendHistoricalProtectedPerson(t, f, subject)
	registry, model := protectedPersonRegistry(t)
	client := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	catalog := store.EventTypes()
	readSource := func(source events.SourceID) events.Appended {
		return readProtectedPersonSource(t, f.ctx, store, source)
	}
	awaitProtectedPersonMigration(t, f.ctx, catalog, readSource, subject)
	appendSuccessfully(t, f.ctx, store, "new", ProtectedPerson{Owner: subject, FirstName: "Grace", LastName: "Hopper", Secret: "current-secret"})
	assertProtectedPersonCurrent(t, catalog, readSource("new"))
	reader := readmodels.For(store.ReadModels(), model)
	assertProtectedPersonReplay(t, f.ctx, reader, subject)
	assertProtectedPersonOtherGenerations(t, catalog, readSource, subject)
	if err := store.Compliance().ErasePII(f.ctx, subject); err != nil {
		t.Fatal(err)
	}
	assertErasedProtectedPeople(t, catalog, readSource)
	assertErasedProtectedPersonReplay(t, f.ctx, reader)
}

func appendHistoricalProtectedPerson(t *testing.T, f *kernelFixture, subject string) {
	t.Helper()
	first := f.client(integrationRegistry[ProtectedPersonV1](t, events.WithID("protected-person")), chronicle.WithEventTypeGenerationValidation(true))
	firstStore, err := first.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, f.ctx, firstStore, "old", ProtectedPersonV1{Owner: subject, Name: "Ada Lovelace", Secret: "migrated-secret"})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
}

func protectedPersonRegistry(t *testing.T) (*chronicle.Registry, readmodels.Model[ProtectedPersonModel]) {
	t.Helper()
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[ProtectedPerson](registry, events.WithID("protected-person"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	old, err := chronicle.RegisterEventGeneration[ProtectedPersonV1](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = chronicle.RegisterEventMigration(registry, current, old, events.Migration[ProtectedPerson, ProtectedPersonV1]{
		Upcast: func(b *events.MigrationBuilder[ProtectedPerson, ProtectedPersonV1]) {
			b.Split("FirstName", "Name", " ", 0).Split("LastName", "Name", " ", 1)
		},
		Downcast: func(b *events.MigrationBuilder[ProtectedPersonV1, ProtectedPerson]) {
			b.Combine("Name", " ", "FirstName", "LastName")
		},
	}); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[ProtectedPersonModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	return registry, model
}

func readProtectedPersonSource(t *testing.T, ctx context.Context, store *chronicle.EventStore, source events.SourceID) events.Appended {
	t.Helper()
	history, err := store.EventLog().ReadSource(ctx, source, eventsequences.SourceFilter{})
	if err != nil || len(history) != 1 {
		t.Fatalf("read %s: %d %v", source, len(history), err)
	}
	return history[0]
}

func awaitProtectedPersonMigration(t *testing.T, parent context.Context, catalog *events.Catalog, readSource func(events.SourceID) events.Appended, subject string) {
	t.Helper()
	// Historical backfill is a kernel job; wait for the migrated generation.
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		upcast, err := events.Decode[ProtectedPerson](catalog, readSource("old"))
		if err == nil && upcast == (ProtectedPerson{Owner: subject, FirstName: "Ada", LastName: "Lovelace", Secret: "migrated-secret"}) {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("protected historical upcast: %+v %v", upcast, err)
		}
	}
}

func assertProtectedPersonCurrent(t *testing.T, catalog *events.Catalog, appended events.Appended) {
	t.Helper()
	latest, err := events.Decode[ProtectedPerson](catalog, appended)
	if err != nil {
		t.Fatalf("protected current read: %+v %v", latest, err)
	}
	if latest.FirstName != "Grace" || latest.Secret != "current-secret" {
		t.Fatalf("protected current read: %+v %v", latest, err)
	}
}

func assertProtectedPersonReplay(t *testing.T, ctx context.Context, reader *readmodels.Reader[ProtectedPersonModel], subject string) {
	t.Helper()
	replayed, err := reader.GetAll(ctx, new(events.Count(2)))
	if err != nil || len(replayed.Instances) != 2 {
		t.Fatalf("protected migrated replay: %+v %v", replayed, err)
	}
	for _, instance := range replayed.Instances {
		nameReleased := instance.Value.FirstName == "Ada" || instance.Value.FirstName == "Grace"
		if instance.Value.Owner != subject || !nameReleased {
			t.Fatalf("protected migrated replay value: %+v", instance.Value)
		}
	}
}

func assertProtectedPersonOtherGenerations(t *testing.T, catalog *events.Catalog, readSource func(events.SourceID) events.Appended, subject string) {
	t.Helper()
	for source, want := range map[events.SourceID]ProtectedPersonV1{
		"old": {Owner: subject, Name: "Ada Lovelace", Secret: "migrated-secret"},
		"new": {Owner: subject, Name: "Grace Hopper", Secret: "current-secret"},
	} {
		appended := readSource(source)
		if _, ok := appended.GenerationalContent[1]; !ok {
			t.Fatalf("kernel witness is missing generation 1 alternate content for %s", source)
		}
		got, err := events.Decode[ProtectedPersonV1](catalog, appended)
		if err != nil || got != want {
			t.Fatalf("protected alternate-generation decode for %s: %+v %v, want %+v", source, got, err, want)
		}
	}
}

func assertErasedProtectedPeople(t *testing.T, catalog *events.Catalog, readSource func(events.SourceID) events.Appended) {
	t.Helper()
	for _, source := range []events.SourceID{"old", "new"} {
		erased, err := events.Decode[ProtectedPerson](catalog, readSource(source))
		if err != nil {
			t.Fatalf("erasure of migrated %s: %+v %v", source, erased, err)
		}
		piiErased := erased.FirstName == "" && erased.LastName == ""
		secretReleased := erased.Secret == "migrated-secret" || erased.Secret == "current-secret"
		if !piiErased || !secretReleased {
			t.Fatalf("erasure of migrated %s: %+v %v", source, erased, err)
		}
	}
}

func assertErasedProtectedPersonReplay(t *testing.T, ctx context.Context, reader *readmodels.Reader[ProtectedPersonModel]) {
	t.Helper()
	replayed, err := reader.GetAll(ctx, new(events.Count(2)))
	if err != nil || len(replayed.Instances) != 2 {
		t.Fatalf("erased migrated replay: %+v %v", replayed, err)
	}
	if replayed.Instances[0].Value.FirstName != "" || replayed.Instances[1].Value.FirstName != "" {
		t.Fatalf("erased migrated replay: %+v %v", replayed, err)
	}
}
