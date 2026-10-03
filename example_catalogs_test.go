// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

type catalogExampleModel struct{ ID string }

func ExampleClient_Catalogs() {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CustomerRegistered](registry); err != nil {
		panic(err)
	}
	if _, err := chronicle.RegisterReadModel[catalogExampleModel](registry,
		readmodels.WithObserver(readmodels.Reducer, "customer-reducer")); err != nil {
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
	// No connection or namespace is needed to classify events and model producers.
	events, models, err := client.Catalogs("customers")
	if err != nil {
		panic(err)
	}
	event, found := events.Lookup(CustomerRegistered{})
	_, producer := models.Descriptors()[0].Observer()
	fmt.Println(found, event.Ref().ID, producer)
	// Output: true CustomerRegistered customer-reducer
}
