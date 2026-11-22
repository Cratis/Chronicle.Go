// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/seeding"
)

func ExampleRegisterSeeder_streams() {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ReferenceItemAdded](registry); err != nil {
		panic(err)
	}
	if err := chronicle.RegisterSeederFunc(registry, func(b *seeding.Builder) error {
		seeding.For(b.WithStream("Catalog", "published"), "item-1", ReferenceItemAdded{Name: "Notebook"})
		b.ForNamespace("demo").WithStream("Catalog", "draft").ForEventSource("item-1", ReferenceItemAdded{Name: "Draft notebook"})
		return nil
	}); err != nil {
		panic(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}
	}()
	fmt.Println("Stream seeds prepared without connecting")
	// Registration requires positive kernel routing capability confirmation.
	// Output: Stream seeds prepared without connecting
}
