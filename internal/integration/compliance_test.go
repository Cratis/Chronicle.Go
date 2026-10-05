//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type CompliancePersonRegistered struct {
	Owner string `json:"owner" chronicle:"subject"`
	Name  string `json:"name" chronicle:"pii;compliance-details(value=\"person contact\")"`
}
type CompliancePerson struct {
	ID    string `json:"id" chronicle:"key"`
	Owner string `json:"owner" chronicle:"subject;set(CompliancePersonRegistered)"`
	Name  string `json:"name" chronicle:"pii;set(CompliancePersonRegistered)"`
}

func TestKernelComplianceSubjectKeyLifecycle(t *testing.T) {
	fixture := newKernelFixture(t)
	fixture.storeName = chronicle.StoreName("go-pii-" + uuid.NewString())
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CompliancePersonRegistered](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[CompliancePerson](registry)
	if err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	primary, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	otherNamespace, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace("other-tenant"))
	if err != nil {
		t.Fatal(err)
	}
	otherStore, err := client.EventStore(fixture.ctx, chronicle.StoreName(string(fixture.storeName)+"-copy"))
	if err != nil {
		t.Fatal(err)
	}
	subject, source := uuid.NewString(), events.SourceID(uuid.NewString())
	appendPerson := func(store *chronicle.EventStore) error {
		result, err := store.EventLog().Append(fixture.ctx, source, CompliancePersonRegistered{Owner: subject, Name: "fixture-person"})
		if err != nil {
			return err
		}
		return result.Err()
	}
	readPerson := func(store *chronicle.EventStore, want string) {
		t.Helper()
		history, err := store.EventLog().ReadHistory(fixture.ctx, source, eventsequences.SourceFilter{})
		if err != nil {
			t.Fatal("event history failed")
		}
		if len(history.Events) == 0 {
			t.Fatal("event history is empty")
		}
		value, err := events.Decode[CompliancePersonRegistered](store.EventTypes(), history.Events[0])
		if err != nil || value.Name != want {
			t.Fatal("event release/erasure did not match the expected lifecycle")
		}
	}
	for _, store := range []*chronicle.EventStore{primary, otherNamespace, otherStore} {
		if err := appendPerson(store); err != nil {
			t.Fatalf("initial append failed: %T", err)
		}
		readPerson(store, "fixture-person")
		awaitProjection(t, fixture.ctx, readmodels.For(store.ReadModels(), model), readmodels.Key(source), func(value CompliancePerson) bool { return value.Name == "fixture-person" })
	}
	if err := primary.Compliance().ErasePII(fixture.ctx, subject); err != nil {
		t.Fatal(err)
	}
	if err := primary.Compliance().ErasePII(fixture.ctx, subject); err != nil {
		t.Fatal("repeated erasure failed")
	}
	for _, store := range []*chronicle.EventStore{primary, otherStore} {
		readPerson(store, "")
		value, err := readmodels.For(store.ReadModels(), model).Get(fixture.ctx, readmodels.Key(source))
		if err != nil || !value.Exists || value.Value.Name != "" {
			t.Fatal("read model was not shredded")
		}
		if err := appendPerson(store); err == nil {
			t.Fatal("PII write resurrected an erased subject key")
		}
	}
	readPerson(otherNamespace, "fixture-person")
	if err := appendPerson(otherNamespace); err != nil {
		t.Fatal("erasure crossed the namespace boundary")
	}
	if err := primary.Compliance().AllowNewEncryptionKeyFor(fixture.ctx, subject); err != nil {
		t.Fatal(err)
	}
	for _, store := range []*chronicle.EventStore{primary, otherStore} {
		if err := appendPerson(store); err != nil {
			t.Fatal("authorized replacement key could not be created")
		}
		readPerson(store, "") // A new key must never recover erased ciphertext.
	}
}

type ConfidentialValueStored struct {
	Personal        string `json:"personal" chronicle:"pii"`
	SubjectSecret   string `json:"subjectSecret" chronicle:"encrypted"`
	NamespaceSecret string `json:"namespaceSecret" chronicle:"encrypted(scope=namespace)"`
}

func TestKernelConfidentialityIsIndependentOfPIIErasure(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := integrationRegistry[ConfidentialValueStored](t)
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := events.SourceID(uuid.NewString())
	result, err := store.EventLog().Append(fixture.ctx, source, ConfidentialValueStored{Personal: "fixture-person", SubjectSecret: "fixture-subject", NamespaceSecret: "fixture-namespace"})
	if err != nil || result.Err() != nil {
		t.Fatal("confidentiality append failed")
	}
	if err := store.Compliance().ErasePII(fixture.ctx, string(source)); err != nil {
		t.Fatal(err)
	}
	history, err := store.EventLog().ReadSource(fixture.ctx, source, eventsequences.SourceFilter{})
	if err != nil || len(history) != 1 {
		t.Fatal("confidentiality read failed")
	}
	value, err := events.Decode[ConfidentialValueStored](store.EventTypes(), history[0])
	if err != nil || value.Personal != "" || value.SubjectSecret != "fixture-subject" || value.NamespaceSecret != "fixture-namespace" {
		t.Fatal("PII erasure affected confidentiality or failed to erase personal data")
	}
}
