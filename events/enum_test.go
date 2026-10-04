// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type historyEnum int32
type enumCurrent struct{ Value historyEnum }
type enumPrevious struct{ Value historyEnum }

func TestEnumHistoricalTablesAndMigrationBoundary(t *testing.T) {
	oldCodecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[historyEnum]{Name: "One", Value: 1}))
	if err != nil {
		t.Fatal(err)
	}
	newCodecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[historyEnum]{Name: "Two", Value: 2}))
	if err != nil {
		t.Fatal(err)
	}
	providerCalls := 0
	current, err := events.Define[enumCurrent](events.WithGeneration(2), events.WithCodecs(newCodecs), events.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		providerCalls++
		return compliance.Classification{}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[enumPrevious](current, 1, events.WithCodecs(oldCodecs))
	if err != nil {
		t.Fatal(err)
	}
	frozenCalls := providerCalls
	oldNamed, err := previous.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	newNamed, err := current.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	if providerCalls != frozenCalls || frozenCalls == 0 {
		t.Fatal("enum naming reran classification provider")
	}
	catalog, err := events.NewCatalog(newNamed, oldNamed)
	if err != nil {
		t.Fatal(err)
	}
	value, err := events.Decode[enumPrevious](catalog, events.Appended{Context: events.Context{EventType: previous.Ref()}, Content: []byte(`{"value":"One"}`)})
	if err != nil || value.Value != 1 {
		t.Fatalf("historical decode: %v", err)
	}
	var wrong enumCurrent
	if err := current.Descriptor().Unmarshal([]byte(`{"Value":"One"}`), &wrong); err == nil {
		t.Fatal("historical name reinterpreted")
	}
	called := false
	migration := events.Migration[enumCurrent, enumPrevious]{
		Upcast: func(b *events.MigrationBuilder[enumCurrent, enumPrevious]) { called = true; b.DefaultValue("Value", 9) },
		Downcast: func(b *events.MigrationBuilder[enumPrevious, enumCurrent]) {
			called = true
			b.RenamedFrom("Value", "Value")
		},
	}
	if _, err := events.DefineMigration(current, previous, migration); !errors.Is(err, faults.ErrUnsupported) || called {
		t.Fatalf("enum migration did not refuse before callbacks: %v", err)
	}
	// An originally plain declaration cannot smuggle generic JSON into enum-aware
	// catalog endpoints with identical identities and Go types.
	plainCurrent, err := events.Define[enumCurrent](events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	plainPrevious, err := events.DefineGeneration[enumPrevious](plainCurrent, 1)
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := events.DefineMigration(plainCurrent, plainPrevious, migration)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.WithMigrations([]events.MigrationDeclaration{declaration}, true); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("catalog endpoint bypass: %v", err)
	}
}
