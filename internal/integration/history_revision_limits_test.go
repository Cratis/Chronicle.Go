//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"errors"
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

// The SDK rejects unsafe revisions instead of exercising the kernel leak.
// Kernel defect evidence remains documented separately under Chronicle#4525.
func TestKernelProtectedRevisionRejected(t *testing.T) {
	f := newKernelFixture(t)
	client := f.client(integrationRegistry[CompliancePersonRegistered](t))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	subject := uuid.NewString()
	appended := appendSuccessfully(t, f.ctx, store, "source", CompliancePersonRegistered{Owner: subject, Name: "synthetic-original"})
	err = store.EventLog().Revise(f.ctx, *appended.Position, CompliancePersonRegistered{Owner: subject, Name: "synthetic-revision-pii"})
	var unknown *eventsequences.MutationOutcomeUnknownError
	if !errors.Is(err, chronicle.ErrUnsupported) || errors.As(err, &unknown) {
		t.Fatalf("protected revision must be rejected before dispatch: %v", err)
	}
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
	for _, request := range requests {
		if request.Context.EventType.ID == "EventRevised" {
			t.Fatal("rejected revision created a system request")
		}
	}
	history, err := store.EventLog().ReadSource(f.ctx, "source", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || len(history[0].Revisions) != 0 {
		t.Fatalf("rejected revision changed history: %+v", history)
	}
	if strings.Contains(string(history[0].Content), "synthetic-revision-pii") {
		t.Fatal("rejected replacement reached history")
	}
}
