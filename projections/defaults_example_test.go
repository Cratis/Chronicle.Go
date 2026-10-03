// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type DefaultStockRegistered struct{ Name string }
type DefaultStock struct {
	Available int32    `json:"available"`
	Locations []string `json:"locations"`
	Note      *string  `json:"note"`
}

func ExampleWithInitialValues() {
	model, err := readmodels.Define[DefaultStock]()
	if err != nil {
		fmt.Println(err)
		return
	}
	event, err := events.Define[DefaultStockRegistered]()
	if err != nil {
		fmt.Println(err)
		return
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		fmt.Println(err)
		return
	}
	declaration := projections.ModelBound(model,
		projections.FromEvent(event),
		projections.WithInitialValues(DefaultStock{Available: 10, Locations: []string{}}),
		projections.WithInitialValue(projections.Path[DefaultStock, *string]("note"), nil),
		projections.WithLabels("inventory", "inventory", "warehouse"))
	definition, err := projections.Compile(declaration, catalog)
	if err != nil {
		fmt.Println(err)
		return
	}
	wire := definition.KernelDefinition()
	fmt.Println(wire.InitialModelState)
	fmt.Println(wire.Tags)
	// Output:
	// {"available":10,"locations":[],"note":null}
	// [inventory warehouse]
}
