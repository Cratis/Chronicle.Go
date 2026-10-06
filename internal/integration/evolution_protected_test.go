//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
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

type protectedPersonDelivery struct {
	Source events.SourceID
	Name   string
}
type HistoricalProtectedPersonReactor struct{ deliveries chan protectedPersonDelivery }

func (r *HistoricalProtectedPersonReactor) Observe(ctx context.Context, e ProtectedPersonV1, ec events.Context) error {
	select {
	case r.deliveries <- protectedPersonDelivery{ec.SourceID, e.Name}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Characterizes kernel 19.32.3. Chronicle#4456 (fixed in 19.32.2) makes
// migrations transform plaintext and protect every generation for the original
// subject: the event's current representation and projections built from it
// release correctly, before and after erasure. The kernel still releases only
// the current representation, though: other generations in GenerationalContent
// reach clients as ciphertext, both on reads and on observer delivery to a
// historical-generation handler. Reading a protected event at another
// generation therefore stays unsupported; this test fails once that changes.
func TestKernelProtectedEventGenerationMigrationProfile(t *testing.T) {
	f := newKernelFixture(t)
	first := f.client(integrationRegistry[ProtectedPersonV1](t, events.WithID("protected-person")), chronicle.WithEventTypeGenerationValidation(true))
	firstStore, err := first.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	subject := uuid.NewString()
	appendSuccessfully(t, f.ctx, firstStore, "old", ProtectedPersonV1{Owner: subject, Name: "Ada Lovelace", Secret: "migrated-secret"})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
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
	deliveries := make(chan protectedPersonDelivery, 8)
	if err := chronicle.RegisterReactor[*HistoricalProtectedPersonReactor](registry, func() *HistoricalProtectedPersonReactor { return &HistoricalProtectedPersonReactor{deliveries} }, reactors.WithID("protected-person-v1")); err != nil {
		t.Fatal(err)
	}
	client := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	catalog := store.EventTypes()
	readSource := func(source events.SourceID) events.Appended {
		t.Helper()
		history, err := store.EventLog().ReadSource(f.ctx, source, eventsequences.SourceFilter{})
		if err != nil || len(history) != 1 {
			t.Fatalf("read %s: %d %v", source, len(history), err)
		}
		return history[0]
	}
	// Historical backfill is a kernel job; wait for the migrated generation.
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		upcast, err := events.Decode[ProtectedPerson](catalog, readSource("old"))
		if err == nil && upcast.FirstName == "Ada" && upcast.LastName == "Lovelace" && upcast.Secret == "migrated-secret" && upcast.Owner == subject {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("protected historical upcast: %+v %v", upcast, err)
		}
	}
	appendSuccessfully(t, f.ctx, store, "new", ProtectedPerson{Owner: subject, FirstName: "Grace", LastName: "Hopper", Secret: "current-secret"})
	if latest, err := events.Decode[ProtectedPerson](catalog, readSource("new")); err != nil || latest.FirstName != "Grace" || latest.Secret != "current-secret" {
		t.Fatalf("protected current read: %+v %v", latest, err)
	}
	replayed, err := readmodels.For(store.ReadModels(), model).GetAll(f.ctx, new(events.Count(2)))
	if err != nil || len(replayed.Instances) != 2 {
		t.Fatalf("protected migrated replay: %+v %v", replayed, err)
	}
	for _, instance := range replayed.Instances {
		if instance.Value.Owner != subject || (instance.Value.FirstName != "Ada" && instance.Value.FirstName != "Grace") {
			t.Fatalf("protected migrated replay value: %+v", instance.Value)
		}
	}
	// Other generations are not released: neither the downcast of the new
	// event, the original generation of the old one, nor observer delivery.
	for _, source := range []events.SourceID{"old", "new"} {
		other, err := events.Decode[ProtectedPersonV1](catalog, readSource(source))
		if err != nil || other.Name == "" || strings.Contains(other.Name, " ") || other.Secret == "migrated-secret" || other.Secret == "current-secret" {
			t.Fatalf("Chronicle now releases generational content for %s; lift the protected-migration limit: %v", source, err)
		}
	}
	for seen := map[events.SourceID]bool{}; len(seen) < 2; {
		select {
		case delivered := <-deliveries:
			if delivered.Name == "" || strings.Contains(delivered.Name, " ") {
				t.Fatalf("Chronicle now releases historical-generation delivery for %s; lift the protected-migration limit", delivered.Source)
			}
			seen[delivered.Source] = true
		case <-f.ctx.Done():
			t.Fatalf("historical reactor deliveries: %v", seen)
		}
	}
	if err := store.Compliance().ErasePII(f.ctx, subject); err != nil {
		t.Fatal(err)
	}
	for _, source := range []events.SourceID{"old", "new"} {
		erased, err := events.Decode[ProtectedPerson](catalog, readSource(source))
		if err != nil || erased.FirstName != "" || erased.LastName != "" || (erased.Secret != "migrated-secret" && erased.Secret != "current-secret") {
			t.Fatalf("erasure of migrated %s: %+v %v", source, erased, err)
		}
	}
	replayed, err = readmodels.For(store.ReadModels(), model).GetAll(f.ctx, new(events.Count(2)))
	if err != nil || len(replayed.Instances) != 2 || replayed.Instances[0].Value.FirstName != "" || replayed.Instances[1].Value.FirstName != "" {
		t.Fatalf("erased migrated replay: %+v %v", replayed, err)
	}
}
