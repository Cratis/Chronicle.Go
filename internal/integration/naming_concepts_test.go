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
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/fundamentals.go/concepts"
	"github.com/cratis/fundamentals.go/correlation"
)

type SharedScalarEvent struct {
	URLValue string
	ID       concepts.UUID
	Date     concepts.DateOnly
	Time     concepts.TimeOnly
	Duration concepts.TimeSpan
	Author   conceptfixtures.AuthorID
	Name     conceptfixtures.Name
	Number   conceptfixtures.Number
}

func TestKernelFundamentalsScalarsAndDefaultNaming(t *testing.T) {
	f := newKernelFixture(t)
	registry := integrationRegistry[SharedScalarEvent](t)
	client := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	id, err := concepts.ParseUUID("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	want := SharedScalarEvent{URLValue: "https://example.test", ID: id, Author: conceptfixtures.AuthorID(id), Name: "Ada", Number: 42}
	for text, target := range map[string]interface{ UnmarshalText([]byte) error }{"2026-01-02": &want.Date, "03:04:05.1234567": &want.Time, "1.02:03:04.1234567": &want.Duration} {
		if err := target.UnmarshalText([]byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	appendSuccessfully(t, correlation.WithID(f.ctx, id), store, "shared-values", want)
	persisted := f.read("shared-values")
	if len(persisted) != 1 {
		t.Fatalf("persisted=%d", len(persisted))
	}
	var got SharedScalarEvent
	if err := json.Unmarshal([]byte(persisted[0].Content), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v; JSON %s", got, want, persisted[0].Content)
	}
	if [16]byte(wire.Correlation(persisted[0].Context.CorrelationId)) != [16]byte(id) {
		t.Fatal("shared correlation lost")
	}
	var content map[string]json.RawMessage
	if err := json.Unmarshal([]byte(persisted[0].Content), &content); err != nil {
		t.Fatal(err)
	}
	if content["URLValue"] == nil || content["urlValue"] != nil {
		t.Fatalf("stored naming: %s", persisted[0].Content)
	}
	storedSchema := func() string {
		response, err := eventtypes.NewEventTypesClient(f.conn).AllEventTypeGenerations(f.ctx, &eventtypes.AllEventTypeGenerationsRequest{EventStore: string(f.storeName), EventTypeId: "SharedScalarEvent"})
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.CheckEnvelope(response); err != nil {
			t.Fatal(err)
		}
		if len(response.Data) != 1 {
			t.Fatalf("schemas=%d", len(response.Data))
		}
		return response.Data[0].Schema
	}
	before := storedSchema()
	var schema struct{ Properties map[string]json.RawMessage }
	if err := json.Unmarshal([]byte(before), &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Properties["URLValue"] == nil || schema.Properties["urlValue"] != nil {
		t.Fatalf("registered naming: %s", before)
	}
	// A policy change is a schema change, not permission to rewrite generation 1.
	legacy := f.client(registry, chronicle.WithNamingPolicy(serialization.LegacyGoCamelCase), chronicle.WithEventTypeGenerationValidation(true))
	_, err = legacy.EventStore(f.ctx, f.storeName)
	var envelope *chronicle.EnvelopeError
	if !errors.As(err, &envelope) {
		t.Fatalf("same-generation rename accepted: %v", err)
	}
	if after := storedSchema(); after != before {
		t.Fatal("renaming overwrote generation schema")
	}
}
