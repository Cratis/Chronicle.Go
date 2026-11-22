//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/internal/wire"
)

type UnsignedSchemaUpgrade struct {
	Value   uint64
	Nested  struct{ Value uint64 }
	Concept conceptfixtures.Unsigned
}

// Literal pre-#8 schema, independent of the compiler used for SDK re-registration.
const unsignedUpgradeSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","properties":{"Concept":{"format":"uint64","maximum":9223372036854775807,"minimum":0,"type":"integer"},"Nested":{"properties":{"Value":{"format":"uint64","maximum":9223372036854775807,"minimum":0,"type":"integer"}},"required":["Value"],"type":"object"},"Value":{"format":"uint64","maximum":9223372036854775807,"minimum":0,"type":"integer"}},"required":["Value","Nested","Concept"],"title":"UnsignedSchemaUpgrade","type":"object"}`

func TestKernelUnsignedSchemaUpgradePreservesRegistrationAndWideValues(t *testing.T) {
	f := newKernelFixture(t)
	seedLegacyUnsignedSchema(t, f)
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[UnsignedSchemaUpgrade](registry, events.WithID("unsigned-upgrade"))
	if err != nil {
		t.Fatal(err)
	}
	if event.Descriptor().Schema() != unsignedUpgradeSchema {
		t.Fatalf("compiled schema = %s, want fixed legacy schema %s", event.Descriptor().Schema(), unsignedUpgradeSchema)
	}
	client := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	var want []UnsignedSchemaUpgrade
	for _, number := range []uint64{math.MaxInt64 + 1, math.MaxUint64} {
		value := UnsignedSchemaUpgrade{Value: number, Nested: struct{ Value uint64 }{number}, Concept: conceptfixtures.Unsigned{Value: number}}
		want = append(want, value)
		appendSuccessfully(t, f.ctx, store, "unsigned-upgrade", value)
	}
	assertUnsignedUpgradeEvents(t, f, want)
}

func seedLegacyUnsignedSchema(t *testing.T, f *kernelFixture) {
	t.Helper()
	ensured, err := eventstores.NewEventStoresClient(f.conn).EnsureEventStore(f.ctx, &eventstores.EnsureEventStoreRequest{Name: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(ensured); err != nil {
		t.Fatal(err)
	}
	registered, err := eventtypes.NewEventTypesClient(f.conn).RegisterEventTypes(f.ctx, &eventtypes.RegisterEventTypesRequest{
		EventStore: string(f.storeName), DisableValidation: false,
		Types: []*eventtypes.EventTypeRegistration{{
			Type: &eventtypes.EventType{Id: "unsigned-upgrade", Generation: 1}, Schema: unsignedUpgradeSchema,
			Generations: []*eventtypes.EventTypeGenerationDefinition{{Generation: 1, Schema: unsignedUpgradeSchema}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(registered); err != nil {
		t.Fatal(err)
	}
}

func assertUnsignedUpgradeEvents(t *testing.T, f *kernelFixture, want []UnsignedSchemaUpgrade) {
	t.Helper()
	persisted := f.read("unsigned-upgrade")
	if len(persisted) != len(want) {
		t.Fatalf("persisted events = %d, want %d", len(persisted), len(want))
	}
	for i, expected := range want {
		var got UnsignedSchemaUpgrade
		if err := json.Unmarshal([]byte(persisted[i].Content), &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("persisted event = %#v, want %#v; JSON %s", got, expected, persisted[i].Content)
		}
	}
}
