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

type ProtectedRevisionRecorded struct {
	Owner           string `json:"owner" chronicle:"subject"`
	Personal        string `json:"personal" chronicle:"pii"`
	NamespaceSecret string `json:"namespaceSecret" chronicle:"encrypted(scope=namespace)"`
	GlobalSecret    string `json:"globalSecret" chronicle:"encrypted(scope=global)"`
}

// Chronicle#4525 (fixed in 19.32.2): the kernel protects revised content and
// the system revision request with the original event's subject, so the SDK
// admits protected revisions. The subject deliberately differs from the source.
func TestKernelProtectedRevisionIsProtectedWithOriginalSubject(t *testing.T) {
	f := newKernelFixture(t)
	client := f.client(integrationRegistry[ProtectedRevisionRecorded](t))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	subject, source := uuid.NewString(), events.SourceID(uuid.NewString())
	appended := appendSuccessfully(t, f.ctx, store, source, ProtectedRevisionRecorded{Owner: subject, Personal: "synthetic-original-pii", NamespaceSecret: "synthetic-original-namespace", GlobalSecret: "synthetic-original-global"})
	revised := ProtectedRevisionRecorded{Owner: subject, Personal: "synthetic-revision-pii", NamespaceSecret: "synthetic-revision-namespace", GlobalSecret: "synthetic-revision-global"}
	if err = store.EventLog().Revise(f.ctx, *appended.Position, revised); err != nil {
		t.Fatal(err)
	}
	history := awaitHistoryMutation(t, f.ctx, store.EventLog(), source, func(h []events.Appended) bool { return len(h) == 1 && len(h[0].Revisions) == 1 })
	decoded, err := events.Decode[ProtectedRevisionRecorded](store.EventTypes(), history[0])
	if err != nil || decoded != revised {
		t.Fatalf("protected revision did not release: %+v %v", decoded, err)
	}
	plaintext := []string{"synthetic-revision-pii", "synthetic-revision-namespace", "synthetic-revision-global"}
	assertRevisionRequestsProtected := func() {
		t.Helper()
		system, err := store.EventSequence(events.SystemSequence)
		if err != nil {
			t.Fatal(err)
		}
		requests, err := system.ReadSource(f.ctx, events.SourceID(events.EventLog), eventsequences.SourceFilter{})
		if err != nil {
			t.Fatal(err)
		}
		found := 0
		for _, request := range requests {
			if request.Context.EventType.ID != "EventRevised" {
				continue
			}
			found++
			for _, value := range plaintext {
				if strings.Contains(string(request.Content), value) {
					t.Fatalf("system revision request stores plaintext %q", value)
				}
			}
		}
		if found != 1 {
			t.Fatalf("expected exactly one system revision request, found %d", found)
		}
	}
	assertRevisionRequestsProtected()
	// At rest: the original, the revision and the system request hold no
	// plaintext for any profile. The unprotected subject is the scan control.
	persisted := []string{"synthetic-original-pii", "synthetic-original-namespace", "synthetic-original-global"}
	persisted = append(persisted, plaintext...)
	assertStoredWithout(t, string(f.storeName), subject, persisted...)
	if err = store.Compliance().ErasePII(f.ctx, subject); err != nil {
		t.Fatal(err)
	}
	assertRevisionRequestsProtected()
	history, err = store.EventLog().ReadSource(f.ctx, source, eventsequences.SourceFilter{})
	if err != nil || len(history) != 1 || len(history[0].Revisions) != 1 {
		t.Fatalf("history after erasure: %+v %v", history, err)
	}
	for _, content := range [][]byte{history[0].Content, history[0].OriginalContent, history[0].Revisions[0].Content} {
		if strings.Contains(string(content), "synthetic-revision-pii") || strings.Contains(string(content), "synthetic-original-pii") {
			t.Fatalf("erased subject's PII survives in history: %s", content)
		}
	}
	erased, err := events.Decode[ProtectedRevisionRecorded](store.EventTypes(), history[0])
	if err != nil || erased.Personal != "" || erased.NamespaceSecret != revised.NamespaceSecret || erased.GlobalSecret != revised.GlobalSecret {
		t.Fatalf("erasure must shred only the subject's PII: %+v %v", erased, err)
	}
}
