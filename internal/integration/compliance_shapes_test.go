//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/google/uuid"
)

type ComplianceAddress struct {
	Street string `json:"street"`
	Number string `json:"number"`
}
type ComplianceCompositeRecorded struct {
	Address  ComplianceAddress      `json:"address" chronicle:"pii"`
	Names    []conceptfixtures.Name `json:"names"`
	Coarse   []string               `json:"coarse" chronicle:"pii"`
	Optional *conceptfixtures.Name  `json:"optional"`
}

func TestKernelComplianceNestedConceptsAndCollections(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := integrationRegistry[ComplianceCompositeRecorded](t, events.WithProtection(compliance.For[conceptfixtures.Name](compliance.Classification{PII: true})))
	client := fixture.client(registry)
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := events.SourceID(uuid.NewString())
	name := conceptfixtures.Name("fixture")
	result, err := store.EventLog().Append(fixture.ctx, source, ComplianceCompositeRecorded{Address: ComplianceAddress{Street: "fixture", Number: "42"}, Names: []conceptfixtures.Name{name}, Coarse: []string{"fixture"}, Optional: &name})
	if err != nil || result.Err() != nil {
		t.Fatal("classified composite append failed")
	}
	read := func() ComplianceCompositeRecorded {
		t.Helper()
		history, err := store.EventLog().ReadSource(fixture.ctx, source, eventsequences.SourceFilter{})
		if err != nil || len(history) != 1 {
			t.Fatal("classified composite history failed")
		}
		value, err := events.Decode[ComplianceCompositeRecorded](store.EventTypes(), history[0])
		if err != nil {
			t.Fatal("classified composite shape was not restored")
		}
		return value
	}
	value := read()
	if value.Address.Number != "42" || value.Address.Street != "fixture" || len(value.Names) != 1 || value.Names[0] != name || len(value.Coarse) != 1 || value.Optional == nil || *value.Optional != name {
		t.Fatal("classified composite did not round-trip")
	}
	if err := store.Compliance().ErasePII(fixture.ctx, string(source)); err != nil {
		t.Fatal(err)
	}
	value = read()
	if value.Address.Number != "" || value.Address.Street != "" || len(value.Coarse) != 0 || len(value.Names) != 1 || value.Names[0] != "" || (value.Optional != nil && *value.Optional != "") {
		t.Fatal("erased composite retained protected data")
	}
}

type ComplianceNumberRecorded struct {
	Number int32 `json:"number" chronicle:"pii"`
}

func TestKernelComplianceNumericScalar(t *testing.T) {
	fixture := newKernelFixture(t)
	client := fixture.client(integrationRegistry[ComplianceNumberRecorded](t))
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := events.SourceID(uuid.NewString())
	result, err := store.EventLog().Append(fixture.ctx, source, ComplianceNumberRecorded{Number: 42})
	if err != nil || result.Err() != nil {
		t.Fatal("numeric PII append failed")
	}
	history, err := store.EventLog().ReadSource(fixture.ctx, source, eventsequences.SourceFilter{})
	if err != nil || len(history) != 1 {
		t.Fatal("numeric PII history failed")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(history[0].Content, &raw); err != nil {
		t.Fatal(err)
	}
	// Chronicle#4550: 19.29.4 released protected numbers as JSON strings.
	if string(raw["number"]) != `42` {
		t.Fatalf("protected number released as %s, want JSON number 42", raw["number"])
	}
	value, err := events.Decode[ComplianceNumberRecorded](store.EventTypes(), history[0])
	if err != nil || value.Number != 42 {
		t.Fatal("numeric PII shape did not round-trip")
	}
}

type GlobalConfidentialValueStored struct {
	Personal     string `json:"personal" chronicle:"pii"`
	GlobalSecret string `json:"globalSecret" chronicle:"encrypted(scope=global)"`
}

// Chronicle#4549: 19.29.4 MongoDB rejected the global key store coordinate.
// The exact EncryptedGlobal schema is covered by
// TestClassificationSchemaMatchesCSharpCategoriesAndPropagation.
func TestKernelGlobalConfidentiality(t *testing.T) {
	fixture := newKernelFixture(t)
	client := fixture.client(integrationRegistry[GlobalConfidentialValueStored](t))
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := events.SourceID(uuid.NewString())
	result, err := store.EventLog().Append(fixture.ctx, source, GlobalConfidentialValueStored{Personal: "fixture-person", GlobalSecret: "fixture-global"})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Err(); err != nil {
		t.Fatal("global confidentiality append failed: ", err)
	}
	read := func() GlobalConfidentialValueStored {
		t.Helper()
		history, err := store.EventLog().ReadSource(fixture.ctx, source, eventsequences.SourceFilter{})
		if err != nil || len(history) != 1 {
			t.Fatal("global confidentiality read failed", err)
		}
		value, err := events.Decode[GlobalConfidentialValueStored](store.EventTypes(), history[0])
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if value := read(); value.Personal != "fixture-person" || value.GlobalSecret != "fixture-global" {
		t.Fatal("global confidentiality did not round-trip")
	}
	if err := store.Compliance().ErasePII(fixture.ctx, string(source)); err != nil {
		t.Fatal(err)
	}
	if value := read(); value.Personal != "" || value.GlobalSecret != "fixture-global" {
		t.Fatal("PII erasure affected global confidentiality or failed to erase personal data")
	}
}
