// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

type previousStatus int32
type currentStatus int32
type evolutionV1 struct {
	FullName string         `json:"full_name"`
	State    previousStatus `json:"old_state"`
	Contact  struct {
		Address string `json:"old_address"`
	} `json:"contact_info"`
}
type evolutionV2 struct {
	FirstName string
	LastName  string
	State     currentStatus `json:"state_code"`
	Kind      string
	Contact   struct {
		Email string `json:"email_address"`
	} `json:"contact"`
}
type evolutionV3 struct{ Name string }

func evolutionRegistry(t *testing.T) (*chronicle.Registry, events.Type[evolutionV2], events.Type[evolutionV1]) {
	t.Helper()
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[evolutionV2](registry, events.WithID("customer-evolution"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := chronicle.RegisterEventGeneration[evolutionV1](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	return registry, current, previous
}
func customerMigration() events.Migration[evolutionV2, evolutionV1] {
	return events.Migration[evolutionV2, evolutionV1]{
		MapValues: func(b *events.ValueMapBuilder[evolutionV2, evolutionV1]) {
			b.For("State", "State", events.ValueMapping{From: previousStatus(1), To: currentStatus(10)}, events.ValueMapping{From: previousStatus(2), To: currentStatus(10)}, events.ValueMapping{From: previousStatus(3), To: currentStatus(20)})
		},
		Upcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) {
			b.Split("FirstName", "FullName", " ", 0).Split("LastName", "FullName", " ", 1).DefaultValue("Kind", "customer").RenamedFrom("Contact.Email", "Contact.Address")
		},
		Downcast: func(b *events.MigrationBuilder[evolutionV1, evolutionV2]) {
			b.Combine("FullName", " ", "FirstName", "LastName").RenamedFrom("Contact.Address", "Contact.Email")
		},
	}
}
func catalogFor(t *testing.T, registry *chronicle.Registry, options ...chronicle.ClientOption) *events.Catalog {
	t.Helper()
	client, err := chronicle.NewClient(append([]chronicle.ClientOption{chronicle.WithRegistry(registry)}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	catalog, _, err := client.Catalogs("customers")
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
func equalEvolutionJSON(t *testing.T, actual, expected string) {
	t.Helper()
	decode := func(data string) any {
		t.Helper()
		if !json.Valid([]byte(data)) {
			t.Fatalf("invalid JSON: %s", data)
		}
		d := json.NewDecoder(strings.NewReader(data))
		d.UseNumber()
		if token, err := d.Token(); err != nil || token != json.Delim('{') {
			t.Fatalf("expected JSON object: %s (%v)", data, err)
		}
		// Only the top-level property order controls migration writes. Nested
		// expression objects retain ordinary semantic JSON comparison.
		var properties []any
		for d.More() {
			key, err := d.Token()
			if err != nil {
				t.Fatal(err)
			}
			var value any
			if err := d.Decode(&value); err != nil {
				t.Fatal(err)
			}
			properties = append(properties, key, value)
		}
		return properties
	}
	if !reflect.DeepEqual(decode(actual), decode(expected)) {
		t.Fatalf("JSON = %s, want %s", actual, expected)
	}
}
func TestHistoricalCatalogExactAndLatestLookup(t *testing.T) {
	registry, current, old := evolutionRegistry(t)
	catalog := catalogFor(t, registry)
	for _, handle := range []events.Descriptor{current.Descriptor(), old.Descriptor()} {
		found, ok := catalog.LookupRef(handle.Ref())
		if !ok || found.GoType() != handle.GoType() {
			t.Fatalf("exact lookup = %v %v", found, ok)
		}
	}
	latest, ok := catalog.LookupID(current.Ref().ID)
	if !ok || latest.Ref() != current.Ref() {
		t.Fatal("ID lookup did not choose current")
	}
	if _, ok := catalog.LookupRef(events.TypeRef{ID: current.Ref().ID, Generation: 3}); ok {
		t.Fatal("unknown generation guessed a codec")
	}
	if old.Ref().ID != current.Ref().ID || !old.Descriptor().IsHistorical() || current.Descriptor().IsHistorical() {
		t.Fatal("incorrect historical identity")
	}
	if _, err := chronicle.RegisterEvent[evolutionV3](registry, events.WithID(current.Ref().ID)); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("duplicate current: %v", err)
	}
	if _, err := chronicle.RegisterEventGeneration[evolutionV3](registry, current, 1); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("duplicate pair: %v", err)
	}
	if _, err := events.NewCatalog(old.Descriptor()); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("orphan: %v", err)
	}
	for _, generation := range []events.Generation{0, 2, 3} {
		if _, err := chronicle.RegisterEventGeneration[evolutionV3](registry, current, generation); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("generation %d: %v", generation, err)
		}
	}
	if _, err := chronicle.RegisterEventGeneration[evolutionV3](registry, old, 0); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("alias as current: %v", err)
	}
	if _, err := chronicle.RegisterEventGeneration[evolutionV3](chronicle.NewRegistry(), current, 1); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("foreign current: %v", err)
	}
	if _, err := events.DefineGeneration[evolutionV1](current, 1, events.WithID("different")); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("ID override: %v", err)
	}
	if _, err := events.DefineGeneration[evolutionV1](current, 1, events.WithGeneration(2)); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("generation override: %v", err)
	}
}
func TestMigrationRegistrationMatchesCSharpWire(t *testing.T) {
	registry, current, previous := evolutionRegistry(t)
	if err := chronicle.RegisterEventMigration(registry, current, previous, customerMigration()); err != nil {
		t.Fatal(err)
	}
	requests := make(chan *eventtypes.RegisterEventTypesRequest, 1)
	client, _ := testClient(t, &fakeKernel{register: func(request *eventtypes.RegisterEventTypesRequest) { requests <- request }}, chronicle.WithRegistry(registry), chronicle.WithEventTypeGenerationValidation(true), chronicle.WithNamingPolicy(serialization.CamelCase))
	if _, err := client.EventStore(testContext(t), "customers"); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if request.DisableValidation || len(request.Types) != 1 {
		t.Fatalf("registration = %v", request)
	}
	registration := request.Types[0]
	if registration.Type.Generation != 2 || len(registration.Generations) != 2 || len(registration.Migrations) != 1 {
		t.Fatalf("generation grouping = %v", registration)
	}
	for i, generation := range registration.Generations {
		if generation.Generation != uint32(i+1) || generation.Schema == "{}" {
			t.Fatal("real schemas were replaced by placeholders")
		}
	}
	if !strings.Contains(registration.Generations[0].Schema, `"full_name"`) || !strings.Contains(registration.Schema, `"firstName"`) {
		t.Fatal("wrong generation schema")
	}
	migration := registration.Migrations[0]
	if migration.FromGeneration != 1 || migration.ToGeneration != 2 {
		t.Fatal("wrong adjacency")
	}
	equalEvolutionJSON(t, migration.UpcastJmesPath, `{"state_code":{"$mapValues":{"source":"old_state","mappings":[{"from":1,"to":10},{"from":2,"to":10},{"from":3,"to":20}]}},"firstName":{"$split":{"source":"full_name","separator":" ","part":0}},"lastName":{"$split":{"source":"full_name","separator":" ","part":1}},"kind":{"$defaultValue":"customer"},"contact.email_address":{"$rename":"contact_info.old_address"}}`)
	equalEvolutionJSON(t, migration.DowncastJmesPath, `{"old_state":{"$mapValues":{"source":"state_code","mappings":[{"from":10,"to":1},{"from":20,"to":3}]}},"full_name":{"$combine":{"sources":["firstName","lastName"],"separator":" "}},"contact_info.old_address":{"$rename":"contact.email_address"}}`)
}
func TestDirectionalMigrationOverrideAndFrozenInputs(t *testing.T) {
	registry, current, previous := evolutionRegistry(t)
	literal := map[string]any{"value": "original"}
	var captured *events.MigrationBuilder[evolutionV2, evolutionV1]
	migration := customerMigration()
	migration.Upcast = func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) {
		captured = b
		b.DefaultValue("Kind", literal).MapValues("State", "State", events.ValueMapping{From: 1, To: 99})
	}
	migration.Downcast = func(b *events.MigrationBuilder[evolutionV1, evolutionV2]) {
		b.MapValues("State", "State", events.ValueMapping{From: 99, To: 2})
	}
	if err := chronicle.RegisterEventMigration(registry, current, previous, migration); err != nil {
		t.Fatal(err)
	}
	literal["value"] = "mutated"
	captured.DefaultValue("Kind", "retained builder mutation")
	catalog := catalogFor(t, registry)
	compiled := catalog.Migrations()[0]
	equalEvolutionJSON(t, compiled.UpcastJSON, `{"state_code":{"$mapValues":{"source":"old_state","mappings":[{"from":1,"to":99}]}},"Kind":{"$defaultValue":{"value":"original"}}}`)
	equalEvolutionJSON(t, compiled.DowncastJSON, `{"old_state":{"$mapValues":{"source":"state_code","mappings":[{"from":99,"to":2}]}}}`)
	copies := catalog.Migrations()
	copies[0].UpcastJSON = "changed"
	if catalog.Migrations()[0] != compiled {
		t.Fatal("catalog leaked mutable migration definitions")
	}
}
func TestMigrationPreservesExpressionInsertionOrder(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		upcast   func(*events.MigrationBuilder[evolutionV2, evolutionV1])
		downcast func(*events.MigrationBuilder[evolutionV1, evolutionV2])
		wantUp   string
		wantDown string
	}{
		{
			name: "rename child before parent",
			upcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) {
				b.RenamedFrom("Contact.Email", "FullName").RenamedFrom("Contact", "Contact")
			},
			downcast: func(b *events.MigrationBuilder[evolutionV1, evolutionV2]) {
				b.RenamedFrom("Contact.Address", "FirstName").RenamedFrom("Contact", "Contact")
			},
			wantUp:   `{"contact.email_address":{"$rename":"full_name"},"contact":{"$rename":"contact_info"}}`,
			wantDown: `{"contact_info.old_address":{"$rename":"FirstName"},"contact_info":{"$rename":"contact"}}`,
		},
		{
			name: "rename parent before child",
			upcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) {
				b.RenamedFrom("Contact", "Contact").RenamedFrom("Contact.Email", "FullName")
			},
			downcast: func(b *events.MigrationBuilder[evolutionV1, evolutionV2]) {
				b.RenamedFrom("Contact", "Contact").RenamedFrom("Contact.Address", "FirstName")
			},
			wantUp:   `{"contact":{"$rename":"contact_info"},"contact.email_address":{"$rename":"full_name"}}`,
			wantDown: `{"contact_info":{"$rename":"contact"},"contact_info.old_address":{"$rename":"FirstName"}}`,
		},
		{
			name: "default child before parent",
			upcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) {
				b.DefaultValue("Contact.Email", "specific").DefaultValue("Contact", map[string]any{"email_address": "whole"})
			},
			downcast: func(b *events.MigrationBuilder[evolutionV1, evolutionV2]) {
				b.DefaultValue("Contact.Address", "specific").DefaultValue("Contact", map[string]any{"old_address": "whole"})
			},
			wantUp:   `{"contact.email_address":{"$defaultValue":"specific"},"contact":{"$defaultValue":{"email_address":"whole"}}}`,
			wantDown: `{"contact_info.old_address":{"$defaultValue":"specific"},"contact_info":{"$defaultValue":{"old_address":"whole"}}}`,
		},
		{
			name: "default parent before child",
			upcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) {
				b.DefaultValue("Contact", map[string]any{"email_address": "whole"}).DefaultValue("Contact.Email", "specific")
			},
			downcast: func(b *events.MigrationBuilder[evolutionV1, evolutionV2]) {
				b.DefaultValue("Contact", map[string]any{"old_address": "whole"}).DefaultValue("Contact.Address", "specific")
			},
			wantUp:   `{"contact":{"$defaultValue":{"email_address":"whole"}},"contact.email_address":{"$defaultValue":"specific"}}`,
			wantDown: `{"contact_info":{"$defaultValue":{"old_address":"whole"}},"contact_info.old_address":{"$defaultValue":"specific"}}`,
		},
		{
			name: "repeated target keeps first position and last expression",
			upcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) {
				b.RenamedFrom("Contact.Email", "FullName").RenamedFrom("Contact", "Contact").DefaultValue("Contact.Email", "last")
			},
			downcast: func(b *events.MigrationBuilder[evolutionV1, evolutionV2]) {
				b.RenamedFrom("Contact.Address", "FirstName").RenamedFrom("Contact", "Contact").DefaultValue("Contact.Address", "last")
			},
			wantUp:   `{"contact.email_address":{"$defaultValue":"last"},"contact":{"$rename":"contact_info"}}`,
			wantDown: `{"contact_info.old_address":{"$defaultValue":"last"},"contact_info":{"$rename":"contact"}}`,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			registry, current, previous := evolutionRegistry(t)
			migration := events.Migration[evolutionV2, evolutionV1]{Upcast: scenario.upcast, Downcast: scenario.downcast}
			if err := chronicle.RegisterEventMigration(registry, current, previous, migration); err != nil {
				t.Fatal(err)
			}
			compiled := catalogFor(t, registry).Migrations()[0]
			equalEvolutionJSON(t, compiled.UpcastJSON, scenario.wantUp)
			equalEvolutionJSON(t, compiled.DowncastJSON, scenario.wantDown)
		})
	}
}

func TestMigrationInvalidGraphsFailBeforeConnection(t *testing.T) {
	for _, scenario := range []string{"missing chain", "duplicate", "missing target", "missing source", "gap", "different ID", "unregistered endpoint", "missing intermediate"} {
		t.Run(scenario, func(t *testing.T) {
			registry, current, old := evolutionRegistry(t)
			migration := customerMigration()
			switch scenario {
			case "missing chain":
			case "missing target":
				migration.Upcast = func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) { b.DefaultValue("Typo", 1) }
			case "missing source":
				migration.Upcast = func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) { b.RenamedFrom("FirstName", "Typo") }
			case "gap", "missing intermediate":
				registry = chronicle.NewRegistry()
				third, err := chronicle.RegisterEvent[evolutionV3](registry, events.WithGeneration(3))
				if err != nil {
					t.Fatal(err)
				}
				first, err := chronicle.RegisterEventGeneration[evolutionV1](registry, third, 1)
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "gap" {
					err = chronicle.RegisterEventMigration(registry, third, first, events.Migration[evolutionV3, evolutionV1]{Upcast: func(*events.MigrationBuilder[evolutionV3, evolutionV1]) {}, Downcast: func(*events.MigrationBuilder[evolutionV1, evolutionV3]) {}})
					if err != nil {
						t.Fatal(err)
					}
				}
			case "different ID":
				registry = chronicle.NewRegistry()
				var err error
				current, err = chronicle.RegisterEvent[evolutionV2](registry, events.WithGeneration(2))
				if err != nil {
					t.Fatal(err)
				}
				alternate, err := chronicle.RegisterEvent[evolutionV1](registry, events.WithID("different"))
				if err != nil {
					t.Fatal(err)
				}
				old = alternate
			case "unregistered endpoint":
				registry = chronicle.NewRegistry()
			}
			if scenario != "missing chain" && scenario != "gap" && scenario != "missing intermediate" {
				if err := chronicle.RegisterEventMigration(registry, current, old, migration); err != nil {
					t.Fatal(err)
				}
				if scenario == "duplicate" {
					if err := chronicle.RegisterEventMigration(registry, current, old, migration); err != nil {
						t.Fatal(err)
					}
				}
			}
			client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithEventTypeGenerationValidation(true))
			if client != nil {
				_ = client.Close()
			}
			if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("invalid migration accepted: %v", err)
			}
		})
	}
}
func TestMigrationRequiresBothDirectionsAndValidLiterals(t *testing.T) {
	registry, current, old := evolutionRegistry(t)
	for _, migration := range []events.Migration[evolutionV2, evolutionV1]{
		{}, {Upcast: func(*events.MigrationBuilder[evolutionV2, evolutionV1]) {}},
		{Upcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) { b.DefaultValue("Kind", func() {}) }, Downcast: func(*events.MigrationBuilder[evolutionV1, evolutionV2]) {}},
		{Upcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV1]) { b.Split("FirstName", "FullName", " ", -1) }, Downcast: func(*events.MigrationBuilder[evolutionV1, evolutionV2]) {}},
	} {
		if err := chronicle.RegisterEventMigration(registry, current, old, migration); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("invalid authoring: %v", err)
		}
	}
}
func TestHistoricalTypedDecoding(t *testing.T) {
	registry, current, old := evolutionRegistry(t)
	catalog := catalogFor(t, registry, chronicle.WithNamingPolicy(serialization.CamelCase))
	appended := events.Appended{Context: events.Context{EventType: old.Ref()}, Content: json.RawMessage(`{"full_name":"Ada Lovelace","old_state":1}`), GenerationalContent: map[events.Generation]json.RawMessage{2: json.RawMessage(`{"firstName":"Ada","lastName":"Lovelace","state_code":10}`)}}
	historical, err := appended.Decode(catalog)
	if err != nil || historical.(*evolutionV1).FullName != "Ada Lovelace" {
		t.Fatalf("historical decode: %v %v", historical, err)
	}
	latest, err := events.Decode[evolutionV2](catalog, appended)
	if err != nil || latest.FirstName != "Ada" || latest.State != 10 {
		t.Fatalf("upcast decode: %+v %v", latest, err)
	}
	appended.Context.EventType = current.Ref()
	appended.Content = appended.GenerationalContent[2]
	appended.GenerationalContent = map[events.Generation]json.RawMessage{1: json.RawMessage(`{"full_name":"Ada Lovelace"}`)}
	previous, err := events.Decode[evolutionV1](catalog, appended)
	if err != nil || previous.FullName != "Ada Lovelace" {
		t.Fatalf("downcast decode: %+v %v", previous, err)
	}
	appended.Context.EventType = old.Ref()
	appended.Content = json.RawMessage(`{"firstName":"fallback"}`)
	appended.GenerationalContent = nil
	latest, err = events.Decode[evolutionV2](catalog, appended)
	if err != nil || latest.FirstName != "fallback" || appended.Context.EventType != old.Ref() {
		t.Fatalf("raw fallback: %+v %v", latest, err)
	}
	appended.Context.EventType.Generation = 99
	if _, err = appended.Decode(catalog); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("unknown generation: %v", err)
	}
	appended.Context.EventType.ID = "other"
	if _, err = events.Decode[evolutionV2](catalog, appended); !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatalf("wrong identity: %v", err)
	}
}
