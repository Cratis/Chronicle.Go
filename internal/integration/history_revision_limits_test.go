//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/google/uuid"
)

type RevisionV1 struct{ Value string }
type RevisionV2 struct {
	Value string
	Added string
}

func TestKernelRevisionUsesRegisteredGeneration(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[RevisionV2](registry, events.WithID("revision-generations"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = chronicle.RegisterEventGeneration[RevisionV1](registry, current, 1); err != nil {
		t.Fatal(err)
	}
	client := f.client(registry)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appended := appendSuccessfully(t, f.ctx, store, "source", RevisionV1{"old"})
	if err := store.EventLog().Revise(f.ctx, *appended.Position, RevisionV2{"new", "generation-two"}); err != nil {
		t.Fatal(err)
	}
	history := awaitHistoryMutation(t, f.ctx, store.EventLog(), "source", func(h []events.Appended) bool { return len(h) == 1 && len(h[0].Revisions) == 1 })
	if history[0].Revisions[0].Generation != 2 || !strings.Contains(string(history[0].Revisions[0].Content), "generation-two") {
		t.Fatalf("revision generation: %+v", history[0])
	}
	decoded, err := events.Decode[RevisionV2](store.EventTypes(), history[0])
	if err != nil || decoded.Added != "generation-two" {
		t.Fatalf("decode revised generation: %+v %v", decoded, err)
	}
}

// This probes the real contract with synthetic data. A known leak is a cited
// skip, never evidence that protected revision or erasure has been implemented.
func TestKernelProtectedRevisionDoesNotSurviveErasure(t *testing.T) {
	f := newKernelFixture(t)
	client := f.client(integrationRegistry[CompliancePersonRegistered](t))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	subject := uuid.NewString()
	appended := appendSuccessfully(t, f.ctx, store, "source", CompliancePersonRegistered{Owner: subject, Name: "synthetic-original"})
	if err = store.EventLog().Revise(f.ctx, *appended.Position, CompliancePersonRegistered{Owner: subject, Name: "synthetic-revision-pii"}); err != nil {
		t.Fatal(err)
	}
	awaitHistoryMutation(t, f.ctx, store.EventLog(), "source", func(h []events.Appended) bool { return len(h) == 1 && len(h[0].Revisions) == 1 })
	if err = store.Compliance().ErasePII(f.ctx, subject); err != nil {
		t.Fatal(err)
	}
	system, err := store.EventSequence(events.SystemSequence)
	if err != nil {
		t.Fatal(err)
	}
	requests, err := system.ReadSource(f.ctx, events.SourceID(events.EventLog), eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	foundRevision := false
	for _, request := range requests {
		if request.Context.EventType.ID != "EventRevised" {
			continue
		}
		foundRevision = true
		if strings.Contains(string(request.Content), "synthetic-revision-pii") {
			t.Skip("19.29.4 stores revised PII in the system request after erasure; C#-equivalent Revise wire: https://github.com/Cratis/Chronicle/issues/4525")
		}
	}
	if !foundRevision {
		t.Fatal("missing system revision request; cannot verify erasure")
	}
	history, err := store.EventLog().ReadSource(f.ctx, "source", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range history {
		if strings.Contains(string(event.Content), "synthetic-revision-pii") {
			t.Fatal("revised PII survived erasure")
		}
		for _, revision := range event.Revisions {
			if strings.Contains(string(revision.Content), "synthetic-revision-pii") {
				t.Fatal("historical revision PII survived erasure")
			}
		}
	}
}
