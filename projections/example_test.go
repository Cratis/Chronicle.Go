// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type ItemRegistered struct {
	Name string `json:"name"`
}
type Item struct {
	ID    string `json:"id" chronicle:"key"`
	Name  string `json:"name"`
	State string `json:"state" chronicle:"value(ItemRegistered,value=\"available\")"`
}

func ExampleModelBound() {
	event, err := events.Define[ItemRegistered]()
	if err != nil {
		fmt.Println(err)
		return
	}
	model, err := readmodels.Define[Item]()
	if err != nil {
		fmt.Println(err)
		return
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		fmt.Println(err)
		return
	}
	// Normal applications use RegisterEvent, RegisterReadModel and NewClient;
	// standalone Compile is useful for inspecting the same frozen definition.
	definition, err := projections.Compile(projections.ModelBound(model), catalog)
	if err != nil {
		fmt.Println(err)
		return
	}
	wire := definition.KernelDefinition()
	fmt.Println(wire.From[0].Key.Id, wire.From[0].Value.Key, wire.From[0].Value.Properties["state"])
	// Output: ItemRegistered $eventSourceId $value(available)
}
