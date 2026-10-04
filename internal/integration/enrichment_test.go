//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/transactions"
)

type EnrichmentInput struct {
	Value  string `json:"value"`
	Marker string `json:"marker"`
}
type EnrichmentOutput struct {
	Value  string `json:"value"`
	Marker string `json:"marker"`
}

func TestKernelEnrichmentAuditAppendUnitReturnedEffectAndRevision(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := integrationRegistry[EnrichmentInput](t)
	if _, err := chronicle.RegisterEvent[EnrichmentOutput](registry); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactorHandlers(registry, "enrichment-returned", []reactors.Handler{
		reactors.Returning(func(context.Context, EnrichmentInput) (eventsequences.Entry, error) {
			return eventsequences.Entry{Source: "returned", Event: EnrichmentOutput{Value: "returned"}}, nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	correlation, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	var enrichments atomic.Int32
	first := func(_ context.Context, _ events.TypeRef, content *events.EventContent) error {
		enrichments.Add(1)
		return content.Set("marker", "first")
	}
	second := func(_ context.Context, _ events.TypeRef, content *events.EventContent) error {
		raw, ok, err := content.Get("marker")
		if err != nil {
			return err
		}
		if !ok || string(raw) != `"first"` {
			t.Error("provider order lost")
		}
		return content.Set("marker", "first-second")
	}
	client := fixture.client(registry,
		chronicle.WithEventEnrichers(first, second),
		chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) {
			return identities.Identity{Subject: "enrichment-actor"}, true, nil
		}),
		chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) { return correlation, true, nil }),
		chronicle.WithCausationProvider(func(context.Context) ([]metadata.Causation, bool, error) {
			return []metadata.Causation{{Occurred: time.Now().UTC(), Type: "enrichment-command"}}, true, nil
		}),
		chronicle.WithRootCausation(metadata.RootCausation{ProgramIdentifier: "enrichment-test"}),
	)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	original := appendSuccessfully(t, fixture.ctx, store, "immediate", EnrichmentOutput{Value: "before"})
	unit, owner, err := transactions.Begin(fixture.ctx, store.EventLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := unit.Stage(fixture.ctx, []eventsequences.Entry{{Source: "unit", Event: EnrichmentOutput{Value: "staged"}}}); err != nil {
		t.Fatal(err)
	}
	stagedCount := enrichments.Load()
	if result, err := owner.Commit(fixture.ctx); err != nil || result.Err() != nil {
		t.Fatal(result, err)
	}
	if enrichments.Load() != stagedCount {
		t.Fatal("Commit reran enrichment")
	}
	appendSuccessfully(t, fixture.ctx, store, "trigger", EnrichmentInput{Value: "trigger"})
	awaitHistoryMutation(t, fixture.ctx, store.EventLog(), "returned", func(history []events.Appended) bool { return len(history) == 1 })
	for _, source := range []events.SourceID{"immediate", "unit", "returned"} {
		stored := fixture.read(source)
		if len(stored) != 1 {
			t.Fatalf("source %s event count = %d", source, len(stored))
		}
		event := stored[0]
		var content EnrichmentOutput
		if err := json.Unmarshal([]byte(event.Content), &content); err != nil {
			t.Fatal(err)
		}
		if content.Marker != "first-second" || event.Context.CausedBy.Subject != "enrichment-actor" || wire.Correlation(event.Context.CorrelationId) != correlation {
			t.Fatalf("stored content/audit mismatch for %s", source)
		}
		causes := event.Context.Causation
		if len(causes) != 2 || causes[0].Type != "Root" || causes[0].Properties["programIdentifier"] != "enrichment-test" || causes[1].Type != "enrichment-command" {
			t.Fatalf("stored causes mismatch for %s", source)
		}
	}
	beforeRevision := enrichments.Load()
	if err := store.EventLog().Revise(fixture.ctx, *original.Position, EnrichmentOutput{Value: "revised"}); err != nil {
		t.Fatal(err)
	}
	awaitHistoryMutation(t, fixture.ctx, store.EventLog(), "immediate", func(history []events.Appended) bool { return len(history) == 1 && len(history[0].Revisions) == 1 })
	stored := fixture.read("immediate")[0]
	var revised EnrichmentOutput
	if err := json.Unmarshal([]byte(stored.Content), &revised); err != nil {
		t.Fatal(err)
	}
	if revised.Value != "revised" || revised.Marker != "first-second" || stored.Revisions[0].CausedBy.Subject != "enrichment-actor" || enrichments.Load() != beforeRevision+1 {
		t.Fatal("revision content/audit or once-only enrichment mismatch")
	}
	revisionID, err := metadata.ParseCorrelationID(stored.Revisions[0].CorrelationId)
	if err != nil || revisionID == (metadata.CorrelationID{}) || revisionID == correlation {
		t.Fatal("revision must retain the pinned kernel's fresh correlation", err)
	}
	system, err := sequences.NewEventSequencesClient(fixture.conn).ForEventSourceIdAndEventTypes(fixture.ctx, &sequences.ForEventSourceIdAndEventTypesRequest{EventStore: string(fixture.storeName), Namespace: string(chronicle.DefaultNamespace), EventSequenceId: string(events.SystemSequence), EventSourceId: string(events.EventLog)})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(system); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, event := range system.Data {
		if wire.Correlation(event.Context.CorrelationId) != revisionID {
			continue
		}
		matched++
		causes := event.Context.Causation
		if event.Context.CausedBy.Subject != "enrichment-actor" || len(causes) != 2 || causes[0].Type != "Root" || causes[1].Type != "enrichment-command" {
			t.Fatal("revision system-event audit mismatch")
		}
	}
	if matched != 1 {
		t.Fatalf("revision audit system-event matches = %d", matched)
	}
	t.Log("observed stored protobuf audit and content for immediate, unit, returned effect, and revision; revision keeps the kernel-generated correlation")
}
