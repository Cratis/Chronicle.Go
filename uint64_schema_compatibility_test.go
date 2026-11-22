// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/events"
)

type legacyUnsignedEvent struct{ Value uint64 }
type currentUnsignedEvent struct {
	Value  uint64
	Native uint
}

// Fixed pre-#8 schemas: 0453ade's compileUnsigned, compileFields and finishSchema.
// Do not regenerate these from the current compiler: every registered generation
// must retain the legacy maximum, independently of runtime value admission.
const legacyUnsignedSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","properties":{"Value":{"format":"uint64","maximum":9223372036854775807,"minimum":0,"type":"integer"}},"required":["Value"],"title":"legacyUnsignedEvent","type":"object"}`
const currentUnsignedSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","properties":{"Native":{"format":"uint64","maximum":9223372036854775807,"minimum":0,"type":"integer"},"Value":{"format":"uint64","maximum":9223372036854775807,"minimum":0,"type":"integer"}},"required":["Value","Native"],"title":"currentUnsignedEvent","type":"object"}`

func TestUnsignedRegisteredGenerationSchemaRemainsStable(t *testing.T) {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[legacyUnsignedEvent](registry, events.WithID("unsigned-schema")); err != nil {
		t.Fatal(err)
	}
	registration := captureUnsignedRegistration(t, registry)
	if registration.Type.Generation != 1 || len(registration.Generations) != 1 {
		t.Fatalf("generation grouping = %v", registration)
	}
	assertUnsignedSchemaGolden(t, registration.Schema, legacyUnsignedSchema)
	assertUnsignedGenerationGolden(t, registration.Generations[0], 1, legacyUnsignedSchema)
}

func TestUnsignedHistoricalGenerationSchemasRemainStable(t *testing.T) {
	registration := captureUnsignedRegistration(t, unsignedHistoryRegistry(t))
	if registration.Type.Generation != 2 {
		t.Fatalf("current generation = %v", registration.Type)
	}
	if len(registration.Generations) != 2 {
		t.Fatalf("generation grouping = %v", registration.Generations)
	}
	if len(registration.Migrations) != 1 {
		t.Fatalf("migration grouping = %v", registration.Migrations)
	}
	assertUnsignedSchemaGolden(t, registration.Schema, currentUnsignedSchema)
	assertUnsignedGenerationGolden(t, registration.Generations[0], 1, legacyUnsignedSchema)
	assertUnsignedGenerationGolden(t, registration.Generations[1], 2, currentUnsignedSchema)
	if migration := registration.Migrations[0]; migration.FromGeneration != 1 || migration.ToGeneration != 2 {
		t.Fatalf("migration adjacency = %v", migration)
	}
}

func unsignedHistoryRegistry(t *testing.T) *chronicle.Registry {
	t.Helper()
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[currentUnsignedEvent](registry, events.WithID("unsigned-schema"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := chronicle.RegisterEventGeneration[legacyUnsignedEvent](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	migration := events.Migration[currentUnsignedEvent, legacyUnsignedEvent]{
		Upcast: func(b *events.MigrationBuilder[currentUnsignedEvent, legacyUnsignedEvent]) {
			b.RenamedFrom("Value", "Value").DefaultValue("Native", uint(0))
		},
		Downcast: func(b *events.MigrationBuilder[legacyUnsignedEvent, currentUnsignedEvent]) {
			b.RenamedFrom("Value", "Value")
		},
	}
	if err := chronicle.RegisterEventMigration(registry, current, previous, migration); err != nil {
		t.Fatal(err)
	}
	return registry
}

func captureUnsignedRegistration(t *testing.T, registry *chronicle.Registry) *eventtypes.EventTypeRegistration {
	t.Helper()
	requests := make(chan *eventtypes.RegisterEventTypesRequest, 1)
	client, _ := testClient(t, &fakeKernel{register: func(request *eventtypes.RegisterEventTypesRequest) { requests <- request }}, chronicle.WithRegistry(registry), chronicle.WithEventTypeGenerationValidation(true))
	if _, err := client.EventStore(testContext(t), "customers"); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if request.DisableValidation || len(request.Types) != 1 {
		t.Fatalf("registration = %v", request)
	}
	return request.Types[0]
}

func assertUnsignedGenerationGolden(t *testing.T, generation *eventtypes.EventTypeGenerationDefinition, want uint32, schema string) {
	t.Helper()
	if generation.Generation != want {
		t.Fatalf("generation = %d, want %d", generation.Generation, want)
	}
	assertUnsignedSchemaGolden(t, generation.Schema, schema)
}

func assertUnsignedSchemaGolden(t *testing.T, actual, expected string) {
	t.Helper()
	if actual != expected {
		t.Fatalf("schema = %s, want fixed pre-#8 schema %s", actual, expected)
	}
}
