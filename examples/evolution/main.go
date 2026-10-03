// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// This offline example declares evolution; the kernel, not this program, migrates
// stored payloads. Run with go run ./examples/evolution.
package main

import (
	"fmt"
	"log"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

type CustomerRegisteredV1 struct {
	Name   string `json:"name"`
	Status int32  `json:"status"`
}

type CustomerRegistered struct {
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Status    int32  `json:"status"`
	Kind      string `json:"kind"`
}

func registry() (*chronicle.Registry, error) {
	registry := chronicle.NewRegistry()
	current, err := chronicle.RegisterEvent[CustomerRegistered](registry,
		events.WithID("customer-registered"), events.WithGeneration(2))
	if err != nil {
		return nil, err
	}
	previous, err := chronicle.RegisterEventGeneration[CustomerRegisteredV1](registry, current, 1)
	if err != nil {
		return nil, err
	}
	err = chronicle.RegisterEventMigration(registry, current, previous,
		events.Migration[CustomerRegistered, CustomerRegisteredV1]{
			Upcast: func(b *events.MigrationBuilder[CustomerRegistered, CustomerRegisteredV1]) {
				b.Split("FirstName", "Name", " ", 0).
					Split("LastName", "Name", " ", 1).
					DefaultValue("Kind", "customer")
			},
			Downcast: func(b *events.MigrationBuilder[CustomerRegisteredV1, CustomerRegistered]) {
				b.Combine("Name", " ", "FirstName", "LastName")
			},
			MapValues: func(b *events.ValueMapBuilder[CustomerRegistered, CustomerRegisteredV1]) {
				b.For("Status", "Status", events.ValueMapping{From: int32(1), To: int32(10)})
			},
		})
	if err != nil {
		return nil, err
	}
	return registry, nil
}

func run() error {
	registry, err := registry()
	if err != nil {
		return err
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithEventTypeGenerationValidation(true))
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Print(err)
		}
	}()
	catalog, _, err := client.Catalogs("customers")
	if err != nil {
		return err
	}
	for _, migration := range catalog.Migrations() {
		fmt.Printf("%s: %d -> %d (upcast and downcast)\n", migration.EventType, migration.From, migration.To)
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
