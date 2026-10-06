//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type PlacementContact struct {
	Email string `json:"email" chronicle:"pii"`
}
type PlacementSecret struct {
	Token string `json:"token" chronicle:"encrypted(scope=namespace)"`
}
type PlacementToken string
type ClassifiedNames []string
type ClassifiedLabels map[string]string
type PlacementRecorded struct {
	Contacts map[string]PlacementContact     `json:"contacts"`
	Names    map[string]conceptfixtures.Name `json:"names"`
	Rows     []ClassifiedNames               `json:"rows"`
	Labels   []ClassifiedLabels              `json:"labels"`
	Secrets  map[string]PlacementSecret      `json:"secrets"`
	Tokens   map[string]PlacementToken       `json:"tokens"`
}

// Model-bound declarations inside map value structs are projection
// declarations, so the projected model classifies map values by type.
type PlacementModel struct {
	ID     string                          `json:"id" chronicle:"key"`
	Names  map[string]conceptfixtures.Name `json:"names" chronicle:"set(PlacementRecorded)"`
	Rows   []ClassifiedNames               `json:"rows" chronicle:"set(PlacementRecorded)"`
	Tokens map[string]PlacementToken       `json:"tokens" chronicle:"set(PlacementRecorded)"`
}
type SupportedCollectionProtection struct {
	Map       map[string]string        `json:"map" chronicle:"pii"`
	Coarse    [][]string               `json:"coarse" chronicle:"pii"`
	Leaves    [][]conceptfixtures.Name `json:"leaves"`
	SecretMap map[string]string        `json:"secretMap" chronicle:"encrypted(scope=namespace)"`
}

func TestKernelComplianceCoarseCollections(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[SupportedCollectionProtection](registry, events.WithProtection(compliance.For[conceptfixtures.Name](compliance.Classification{PII: true}))); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
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

// Chronicle#4551 and #4552 (fixed in 19.32.2): metadata on a dictionary's value
// schema applies to every value, and classified collection-valued array
// elements are protected as a whole. Events and a projected read model are
// read before and after erasure; namespace encryption survives erasure.
func TestKernelComplianceProtectionBeneathMapsAndCollectionElements(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	pii := compliance.Classification{PII: true}
	namespace := compliance.Classification{Encrypted: true, Scope: compliance.Namespace}
	if _, err := chronicle.RegisterEvent[PlacementRecorded](registry, events.WithProtection(compliance.For[conceptfixtures.Name](pii), compliance.For[ClassifiedNames](pii), compliance.For[ClassifiedLabels](pii), compliance.For[PlacementToken](namespace))); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[PlacementModel](registry, readmodels.WithProtection(compliance.For[conceptfixtures.Name](pii), compliance.For[ClassifiedNames](pii), compliance.For[PlacementToken](namespace)))
	if err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := events.SourceID(uuid.NewString())
	input := PlacementRecorded{
		Contacts: map[string]PlacementContact{"home": {Email: "fixture-map-object"}},
		Names:    map[string]conceptfixtures.Name{"first": "fixture-map-concept"},
		Rows:     []ClassifiedNames{{"fixture-array-row"}},
		Labels:   []ClassifiedLabels{{"label": "fixture-array-map"}},
		Secrets:  map[string]PlacementSecret{"api": {Token: "fixture-map-secret"}},
		Tokens:   map[string]PlacementToken{"api": "fixture-map-token"},
	}
	appendSuccessfully(t, f.ctx, store, source, input)
	read := func() PlacementRecorded {
		t.Helper()
		history, err := store.EventLog().ReadSource(f.ctx, source, eventsequences.SourceFilter{})
		if err != nil || len(history) != 1 {
			t.Fatalf("history: %v", err)
		}
		value, err := events.Decode[PlacementRecorded](store.EventTypes(), history[0])
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if got := read(); !reflect.DeepEqual(got, input) {
		t.Fatalf("protected placements did not round-trip: %+v", got)
	}
	reader := readmodels.For(store.ReadModels(), model)
	projected := awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(v PlacementModel) bool { return len(v.Names) == 1 })
	if !reflect.DeepEqual(projected.Value.Names, input.Names) || !reflect.DeepEqual(projected.Value.Rows, input.Rows) || !reflect.DeepEqual(projected.Value.Tokens, input.Tokens) {
		t.Fatalf("projected placements: %+v", projected.Value)
	}
	if err := store.Compliance().ErasePII(f.ctx, string(source)); err != nil {
		t.Fatal(err)
	}
	erased := read()
	if erased.Contacts["home"].Email != "" || erased.Names["first"] != "" || len(erased.Rows) != 1 || len(erased.Rows[0]) != 0 || len(erased.Labels) != 1 || len(erased.Labels[0]) != 0 || erased.Secrets["api"].Token != "fixture-map-secret" || erased.Tokens["api"] != "fixture-map-token" {
		t.Fatalf("erasure beneath maps/elements: %+v", erased)
	}
	value, err := reader.Get(f.ctx, readmodels.Key(source))
	if err != nil || !value.Exists || value.Value.Names["first"] != "" || len(value.Value.Rows) != 1 || len(value.Value.Rows[0]) != 0 || value.Value.Tokens["api"] != "fixture-map-token" {
		t.Fatalf("projected erasure: %+v %v", value, err)
	}
	result, err := store.EventLog().Append(f.ctx, source, input)
	if err == nil && result.Err() == nil {
		t.Fatal("PII beneath a map bypassed the erasure fence")
	}
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
