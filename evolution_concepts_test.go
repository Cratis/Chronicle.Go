// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"encoding/json"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/serialization"
)

type conceptEvolutionV1 struct {
	Owner *conceptfixtures.AuthorID `json:"owner_id"`
}
type conceptEvolution struct {
	Author *conceptfixtures.AuthorID `json:"author_id"`
}

func TestHistoricalConceptSchemasAndMigrationNames(t *testing.T) {
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[conceptEvolution](registry, events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	old, err := chronicle.RegisterEventGeneration[conceptEvolutionV1](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterEventMigration(registry, current, old, events.Migration[conceptEvolution, conceptEvolutionV1]{Upcast: func(b *events.MigrationBuilder[conceptEvolution, conceptEvolutionV1]) {
		b.RenamedFrom("Author", "Owner")
	}, Downcast: func(b *events.MigrationBuilder[conceptEvolutionV1, conceptEvolution]) {
		b.RenamedFrom("Owner", "Author")
	}}); err != nil {
		t.Fatal(err)
	}
	catalog := catalogFor(t, registry, chronicle.WithNamingPolicy(serialization.CamelCase), chronicle.WithEventTypeGenerationValidation(true))
	for _, ref := range []events.TypeRef{old.Ref(), current.Ref()} {
		descriptor, ok := catalog.LookupRef(ref)
		if !ok || !strings.Contains(descriptor.Schema(), `"format":"uuid?"`) {
			t.Fatalf("nullable concept schema = %s", descriptor.Schema())
		}
	}
	definitions := catalog.Migrations()
	equalEvolutionJSON(t, definitions[0].UpcastJSON, `{"author_id":{"$rename":"owner_id"}}`)
	equalEvolutionJSON(t, definitions[0].DowncastJSON, `{"owner_id":{"$rename":"author_id"}}`)
	value, err := events.Decode[conceptEvolutionV1](catalog, events.Appended{Context: events.Context{EventType: old.Ref()}, Content: json.RawMessage(`{"owner_id":"00112233-4455-6677-8899-aabbccddeeff"}`)})
	if err != nil || value.Owner == nil {
		t.Fatalf("historical concept decode: %+v %v", value, err)
	}
	descriptor, _ := catalog.LookupRef(old.Ref())
	data, err := descriptor.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	equalEvolutionJSON(t, string(data), `{"owner_id":"00112233-4455-6677-8899-aabbccddeeff"}`)
}
