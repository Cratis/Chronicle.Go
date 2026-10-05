// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

func TestMigrationChainIsCompleteOrderedAndStoreIsolated(t *testing.T) {
	registry := chronicle.NewRegistry()
	third, err := chronicle.RegisterEvent[evolutionV3](registry, events.WithGeneration(3))
	if err != nil {
		t.Fatal(err)
	}
	first, err := chronicle.RegisterEventGeneration[evolutionV1](registry, third, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := chronicle.RegisterEventGeneration[evolutionV2](registry, third, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterEventMigration(registry, third, second, events.Migration[evolutionV3, evolutionV2]{Upcast: func(b *events.MigrationBuilder[evolutionV3, evolutionV2]) {
		b.Combine("Name", " ", "FirstName", "LastName")
	}, Downcast: func(b *events.MigrationBuilder[evolutionV2, evolutionV3]) {
		b.Split("FirstName", "Name", " ", 0).Split("LastName", "Name", " ", 1)
	}}); err != nil {
		t.Fatal(err)
	}
	if client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithEventTypeGenerationValidation(true)); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		if client != nil {
			_ = client.Close()
		}
		t.Fatalf("missing first link: %v", err)
	}
	if err := chronicle.RegisterEventMigration(registry, second, first, customerMigration()); err != nil {
		t.Fatal(err)
	}
	catalog := catalogFor(t, registry, chronicle.WithEventTypeGenerationValidation(true))
	definitions := catalog.Migrations()
	if len(definitions) != 2 || definitions[0].From != 1 || definitions[1].From != 2 || definitions[1].To != 3 {
		t.Fatalf("chain = %+v", definitions)
	}
	// Empty direction is supported, but an incomplete chain is not when enabled.
	empty := chronicle.NewRegistry()
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithRegistryForStore("other", empty), chronicle.WithEventTypeGenerationValidation(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	other, _, err := client.Catalogs("other")
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Migrations()) != 0 || len(other.Descriptors()) != 0 {
		t.Fatal("default migrations leaked into selected store")
	}
}

type recursiveEvolution struct {
	Name     string
	Child    *recursiveEvolution
	Children []recursiveEvolution
}
type recursiveEvolutionV1 struct {
	OldName  string
	Child    *recursiveEvolutionV1
	Children []recursiveEvolutionV1
}

func TestMigrationRecursivePropertiesAndCollectionRejection(t *testing.T) {
	for _, path := range []string{"Child.Child.Name", "Children.Name"} {
		t.Run(path, func(t *testing.T) {
			registry := chronicle.NewRegistry()
			current, err := chronicle.RegisterEvent[recursiveEvolution](registry, events.WithGeneration(2))
			if err != nil {
				t.Fatal(err)
			}
			old, err := chronicle.RegisterEventGeneration[recursiveEvolutionV1](registry, current, 1)
			if err != nil {
				t.Fatal(err)
			}
			err = chronicle.RegisterEventMigration(registry, current, old, events.Migration[recursiveEvolution, recursiveEvolutionV1]{Upcast: func(b *events.MigrationBuilder[recursiveEvolution, recursiveEvolutionV1]) {
				b.RenamedFrom(events.Property[recursiveEvolution](path), "Child.Child.OldName")
			}, Downcast: func(*events.MigrationBuilder[recursiveEvolutionV1, recursiveEvolution]) {}})
			if err != nil {
				t.Fatal(err)
			}
			client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
			if client != nil {
				defer func() {
					if err := client.Close(); err != nil {
						t.Error(err)
					}
				}()
			}
			if path == "Children.Name" {
				if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
					t.Fatalf("collection path: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			catalog, _, err := client.Catalogs("store")
			if err != nil {
				t.Fatal(err)
			}
			equalEvolutionJSON(t, catalog.Migrations()[0].UpcastJSON, `{"Child.Child.Name":{"$rename":"Child.Child.OldName"}}`)
			equalEvolutionJSON(t, catalog.Migrations()[0].DowncastJSON, `{}`)
		})
	}
}
