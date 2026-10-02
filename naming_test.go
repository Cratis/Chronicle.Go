// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type namingEvent struct {
	URLValue string
	Person   string
	Nested   struct{ Name string }
}
type namingModel struct {
	ID       string `json:"Id" chronicle:"key"`
	URLValue string `chronicle:"set(namingEvent)"`
	Person   string `chronicle:"set(namingEvent)"`
}

func TestClientNamingFreezesEveryArtifactAndPreservesRegistry(t *testing.T) {
	registry := NewRegistry()
	event, err := RegisterEvent[namingEvent](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[namingModel](registry, readmodels.WithIndexes("Person"))
	if err != nil {
		t.Fatal(err)
	}
	unique, err := constraints.UniqueValues("person").On(event.Descriptor(), "Person", "Nested.Name").Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddConstraint(unique); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		policy              serialization.NamingPolicy
		person, url, nested string
	}{
		{serialization.PreservePropertyNames, "Person", "URLValue", "Nested.Name"},
		{serialization.CamelCase, "person", "URLValue", "nested.name"},
		{serialization.LegacyGoCamelCase, "person", "urlValue", "nested.name"},
	} {
		client, err := NewClient(WithRegistry(registry), WithRegistryForStore("other", registry), WithNamingPolicy(tc.policy))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		})
		for _, catalog := range []*readmodels.Catalog{client.readModelCatalog, client.readModelCatalogs["other"]} {
			d, ok := catalog.LookupIdentifier(model.Identifier())
			if !ok || d.ContainerName() != model.Descriptor().ContainerName() || d.Indexes()[0] != tc.person {
				t.Fatal("model configuration drift")
			}
			data, err := d.Marshal(namingModel{ID: "1", Person: "Ada", URLValue: "url"})
			if err != nil || !strings.Contains(string(data), `"`+tc.person+`":"Ada"`) || !strings.Contains(d.Schema(), `"`+tc.url+`"`) {
				t.Fatalf("model: %s / %s, %v", data, d.Schema(), err)
			}
		}
		d, ok := client.catalog.Lookup(namingEvent{})
		if !ok {
			t.Fatal("event missing")
		}
		data, err := d.Marshal(namingEvent{Person: "Ada"})
		if err != nil || !strings.Contains(string(data), `"`+tc.person+`":"Ada"`) || !strings.Contains(d.Schema(), `"`+tc.url+`"`) {
			t.Fatalf("event: %s, %v", data, err)
		}
		paths := client.constraints[0].Fields()[0].Properties
		if paths[0] != tc.person || paths[1] != tc.nested {
			t.Fatalf("constraint paths %v", paths)
		}
		projection := client.projections[0].KernelDefinition()
		if projection.From[0].Value.Properties[tc.person] != tc.person || projection.From[0].Value.Properties[tc.url] != tc.url {
			t.Fatalf("projection = %v", projection)
		}
		if !strings.Contains(event.Descriptor().Schema(), `"Person"`) || model.Descriptor().Indexes()[0] != "Person" {
			t.Fatal("registry mutated")
		}
	}
}

type fluentNamingModel struct {
	Person string
	Nested struct{ Name string }
}

func TestFluentNamingRebindsOnlyPlanPaths(t *testing.T) {
	r := NewRegistry()
	e, err := RegisterEvent[namingEvent](r)
	if err != nil {
		t.Fatal(err)
	}
	m, err := RegisterReadModel[fluentNamingModel](r)
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("naming", m)
	projections.From(b, e, func(f *projections.FromBuilder[fluentNamingModel, namingEvent]) {
		projections.Map(f, projections.Path[fluentNamingModel, string]("Person"), projections.Path[namingEvent, string]("Person"))
		projections.Map(f, projections.Path[fluentNamingModel, string]("Nested.Name"), projections.Path[namingEvent, string]("Nested.Name"))
	}, projections.UsingKey(projections.Path[namingEvent, string]("Person")))
	d, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(d); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(WithRegistry(r), WithNamingPolicy(serialization.CamelCase))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	from := c.projections[0].KernelDefinition().From[0].Value
	if from.Key != "person" || from.Properties["nested.name"] != "nested.name" || from.Properties["person"] != "person" {
		t.Fatal(from)
	}
}

type idNamingEvent struct {
	ID   string
	Name string
}

type idNamingModel struct {
	ID   string `chronicle:"key"`
	Name string
}

func TestClientReadModelIDTranslationRebindsProjectionPaths(t *testing.T) {
	r := NewRegistry()
	e, err := RegisterEvent[idNamingEvent](r)
	if err != nil {
		t.Fatal(err)
	}
	m, err := RegisterReadModel[idNamingModel](r, readmodels.WithIndexes("Id"), readmodels.WithPII("Name"), readmodels.WithSubjectProperty("Id"))
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("id-naming", m)
	projections.From(b, e, func(f *projections.FromBuilder[idNamingModel, idNamingEvent]) {
		projections.Map(f, projections.Path[idNamingModel, string]("Id"), projections.Path[idNamingEvent, string]("ID"))
		projections.Map(f, projections.Path[idNamingModel, string]("Name"), projections.Path[idNamingEvent, string]("Name"))
	}, projections.UsingKey(projections.Path[idNamingEvent, string]("ID")))
	declaration, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		policy           serialization.NamingPolicy
		modelID, eventID string
	}{
		{serialization.PreservePropertyNames, "Id", "ID"},
		{serialization.CamelCase, "id", "ID"},
		{serialization.LegacyGoCamelCase, "id", "id"},
	} {
		client, err := NewClient(WithRegistry(r), WithNamingPolicy(tc.policy))
		if err != nil {
			t.Fatal(err)
		}
		definition := client.projections[0]
		from := definition.KernelDefinition().From[0].Value
		if definition.KeyField() != tc.modelID || from.Key != tc.eventID || from.Properties[tc.modelID] != tc.eventID || definition.Model().Indexes()[0] != tc.modelID {
			t.Fatalf("policy %v: key=%q projection=%+v indexes=%v", tc.policy, definition.KeyField(), from, definition.Model().Indexes())
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

type namingCollision struct {
	Person string
	Other  string `json:"person"`
}

func TestClientNamingRejectsCollisionsBeforeIO(t *testing.T) {
	r := NewRegistry()
	if _, err := RegisterEvent[namingCollision](r); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.CamelCase, serialization.LegacyGoCamelCase, 255} {
		if _, err := NewClient(WithRegistry(r), WithNamingPolicy(policy)); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("policy %d: %v", policy, err)
		}
	}
}
