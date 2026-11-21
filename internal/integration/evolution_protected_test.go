//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
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

type HistoricalProtectedPersonReactor struct{}

func (*HistoricalProtectedPersonReactor) Observe(ProtectedPersonV1) {
	panic("historical protected handler must not run")
}

// Characterizes kernel 19.32.3. Chronicle#4456 (fixed in 19.32.2) makes
// migrations transform plaintext and protect every generation for the original
// subject: the event's current representation and projections built from it
// release correctly, before and after erasure. The kernel still releases only
// the current representation, though: other generations in GenerationalContent
// reach clients as ciphertext (Chronicle#4623,
// https://github.com/Cratis/Chronicle/issues/4623). The SDK now refuses protected
// alternate-generation decoding and historical protected observers (Chronicle.Go#90).
// Remove the guard per Chronicle.Go#96 once the kernel releases other generations;
// this test checks the raw kernel content independently of the SDK refusal.
func TestKernelProtectedEventGenerationMigrationProfile(t *testing.T) {
	f := newKernelFixture(t)
	subject := uuid.NewString()
	appendHistoricalProtectedPerson(t, f, subject)
	assertHistoricalProtectedPersonReactorRefused(t)
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
	assertProtectedPersonOtherGenerations(t, catalog, readSource)
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

func assertHistoricalProtectedPersonReactorRefused(t *testing.T) {
	t.Helper()
	// Use a separate registry: the real kernel client must have no historical reactor.
	registry, _ := protectedPersonRegistry(t)
	err := chronicle.RegisterReactor[*HistoricalProtectedPersonReactor](registry, func() *HistoricalProtectedPersonReactor { return &HistoricalProtectedPersonReactor{} }, reactors.WithID("protected-person-v1"))
	if err == nil {
		client, clientErr := chronicle.NewClient(chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry), chronicle.WithEventTypeGenerationValidation(true))
		if client != nil {
			if closeErr := client.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}
		err = clientErr
	}
	if !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("historical protected reactor: %v, want ErrUnsupported", err)
	}
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

func assertProtectedPersonOtherGenerations(t *testing.T, catalog *events.Catalog, readSource func(events.SourceID) events.Appended) {
	t.Helper()
	for _, source := range []events.SourceID{"old", "new"} {
		appended := readSource(source)
		assertProtectedPersonAlternateContent(t, source, appended)
		if _, err := events.Decode[ProtectedPersonV1](catalog, appended); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("protected alternate-generation decode for %s: %v, want ErrUnsupported", source, err)
		}
	}
}

func assertProtectedPersonAlternateContent(t *testing.T, source events.SourceID, appended events.Appended) {
	t.Helper()
	if len(appended.GenerationalContent) == 0 {
		t.Fatalf("kernel witness has no alternate generational content for %s", source)
	}
	if source == "new" {
		if _, ok := appended.GenerationalContent[1]; !ok {
			t.Fatal("kernel witness is missing generation 1 alternate content for new")
		}
	}
	// The historical event may expose a different alternate generation. Inspect
	// every supplied representation, not an SDK-decoded or invented fallback.
	for generation, content := range appended.GenerationalContent {
		assertProtectedPersonGenerationCiphertext(t, source, generation, content)
	}
}

func assertProtectedPersonGenerationCiphertext(t *testing.T, source events.SourceID, generation events.Generation, content json.RawMessage) {
	t.Helper()
	if len(content) == 0 {
		t.Fatalf("kernel witness has empty alternate generation %d for %s", generation, source)
	}
	for _, plaintext := range []string{"Ada Lovelace", "Grace Hopper", "migrated-secret", "current-secret", `"Ada"`, `"Lovelace"`, `"Grace"`, `"Hopper"`} {
		if strings.Contains(string(content), plaintext) {
			t.Fatalf("Chronicle now releases generational content for %s; lift the protected-migration limit: generation %d", source, generation)
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
