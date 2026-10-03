//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type UnsupportedMapProtection struct {
	Contacts map[string]struct {
		Email string `chronicle:"pii"`
	}
}
type ClassifiedNames []string
type UnsupportedArrayProtection struct{ Rows []ClassifiedNames }
type SupportedCollectionProtection struct {
	Map       map[string]string        `json:"map" chronicle:"pii"`
	Coarse    [][]string               `json:"coarse" chronicle:"pii"`
	Leaves    [][]conceptfixtures.Name `json:"leaves"`
	SecretMap map[string]string        `json:"secretMap" chronicle:"encrypted(scope=namespace)"`
}

func TestKernelComplianceSupportedCollectionsAndRegistrationRefusal(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	_, err := chronicle.RegisterEvent[UnsupportedMapProtection](registry)
	var declaration *declarations.DeclarationError
	if !errors.As(err, &declaration) {
		t.Fatalf("map protection reached registration: %v", err)
	}
	for _, metadata := range []compliance.Classification{{PII: true}, {Encrypted: true}} {
		_, err = chronicle.RegisterEvent[UnsupportedArrayProtection](registry, events.WithProtection(compliance.For[ClassifiedNames](metadata)))
		if !errors.As(err, &declaration) {
			t.Fatalf("array item protection reached registration: %v", err)
		}
	}
	if _, err := chronicle.RegisterEvent[SupportedCollectionProtection](registry, events.WithProtection(compliance.For[conceptfixtures.Name](compliance.Classification{PII: true}))); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []events.TypeID{"UnsupportedMapProtection", "UnsupportedArrayProtection"} {
		if _, exists := store.EventTypes().LookupID(id); exists {
			t.Fatal("rejected type entered registration catalog")
		}
	}
	source := events.SourceID(uuid.NewString())
	input := SupportedCollectionProtection{Map: map[string]string{"email": "fixture"}, Coarse: [][]string{{"fixture"}}, Leaves: [][]conceptfixtures.Name{{"fixture"}}, SecretMap: map[string]string{"token": "fixture-secret"}}
	appendSuccessfully(t, f.ctx, store, source, input)
	read := func() SupportedCollectionProtection {
		t.Helper()
		history, err := store.EventLog().ReadSource(f.ctx, source, eventsequences.SourceFilter{})
		if err != nil || len(history) != 1 {
			t.Fatalf("history: %v", err)
		}
		value, err := events.Decode[SupportedCollectionProtection](store.EventTypes(), history[0])
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if !reflect.DeepEqual(read(), input) {
		t.Fatal("supported collection shape did not round-trip")
	}
	if err := store.Compliance().ErasePII(f.ctx, string(source)); err != nil {
		t.Fatal(err)
	}
	value := read()
	if len(value.Map) != 0 || len(value.Coarse) != 0 || !reflect.DeepEqual(value.Leaves, [][]conceptfixtures.Name{{""}}) || !reflect.DeepEqual(value.SecretMap, input.SecretMap) {
		t.Fatal("erasure retained PII or changed confidentiality")
	}
	result, err := store.EventLog().Append(f.ctx, source, input)
	if err == nil && result.Err() == nil {
		t.Fatal("supported collection write bypassed erasure fence")
	}
	_ = read() // Refused append did not persist a second event.
}

type RecursiveProtection struct {
	Name string
	Next *RecursiveProtection
}

func TestKernelRecursiveOverrideReleaseWithPopulatedEdges(t *testing.T) {
	f := newKernelFixture(t)
	protection := compliance.Property("Next.Name", compliance.Classification{PII: true})
	registry := integrationRegistry[RecursiveProtection](t, events.WithProtection(protection))
	model, err := chronicle.RegisterReadModel[RecursiveProtection](registry, readmodels.WithProtection(protection))
	if err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := events.SourceID(uuid.NewString())
	input := RecursiveProtection{Name: "root", Next: &RecursiveProtection{Name: "private", Next: &RecursiveProtection{Name: "plain", Next: &RecursiveProtection{Name: "leaf"}}}}
	appendSuccessfully(t, f.ctx, store, source, input)
	for _, erased := range []bool{false, true} {
		if erased {
			if err := store.Compliance().ErasePII(f.ctx, string(source)); err != nil {
				t.Fatal(err)
			}
		}
		history, err := store.EventLog().ReadSource(f.ctx, source, eventsequences.SourceFilter{})
		if err != nil || len(history) != 1 {
			t.Fatalf("recursive history: %v", err)
		}
		var document map[string]json.RawMessage
		if err := json.Unmarshal(history[0].Content, &document); err != nil {
			t.Fatal(err)
		}
		document["__subject"], err = json.Marshal(string(source))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		released, err := store.ReadModels().Release(f.ctx, model.Identifier(), raw)
		if err != nil {
			t.Fatal(err)
		}
		var value RecursiveProtection
		if err := json.Unmarshal(released, &value); err != nil {
			t.Fatal(err)
		}
		want := "private"
		if erased {
			want = ""
		}
		if value.Name != "root" || value.Next == nil || value.Next.Name != want || value.Next.Next == nil || value.Next.Next.Name != "plain" || value.Next.Next.Next == nil || value.Next.Next.Next.Name != "leaf" {
			t.Fatalf("recursive synthetic fixture (erased=%t): %s", erased, released)
		}
	}
}

func TestKernelRawDocumentReleasePreservesBookkeeping(t *testing.T) {
	f := newKernelFixture(t)
	registry := integrationRegistry[CompliancePersonRegistered](t)
	model, err := chronicle.RegisterReadModel[CompliancePerson](registry)
	if err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source, owner := events.SourceID(uuid.NewString()), uuid.NewString()
	appendSuccessfully(t, f.ctx, store, source, CompliancePersonRegistered{Owner: owner, Name: "fixture-person"})
	reader := readmodels.For(store.ReadModels(), model)
	awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(value CompliancePerson) bool { return value.Name == "fixture-person" })
	instance, err := reader.Get(f.ctx, readmodels.Key(source))
	if err != nil || !instance.Exists {
		t.Fatalf("projected instance: %v", err)
	}
	// Reconstruct a raw sink envelope from the materialized document. The release
	// RPC must accept already released values while excluding all bookkeeping.
	document := map[string]any{"id": string(source), "owner": owner, "name": instance.Value.Name, "__subject": owner, "__subjects": map[string]string{"name": owner}, "_id": "sink-key", "__initialized": true, "__lastHandledEventSequenceNumber": 42}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	released, err := store.ReadModels().Release(f.ctx, model.Identifier(), raw)
	if err != nil {
		t.Fatal(err)
	}
	var before, after any
	if err := json.Unmarshal(raw, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(released, &after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("release changed raw document lineage or bookkeeping")
	}
}
