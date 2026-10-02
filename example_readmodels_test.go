// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

func ExampleRegisterReadModel() {
	type Person struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	registry := chronicle.NewRegistry()
	model, err := chronicle.RegisterReadModel[Person](registry, readmodels.WithIdentifier("Example.Person"), readmodels.WithIndexes("name"))
	if err != nil {
		panic(err)
	}
	descriptor := model.Descriptor()
	fmt.Println(model.Identifier(), descriptor.ContainerName(), descriptor.Generation(), descriptor.Sink().Type)
	// Pass WithRegistry(registry) to NewClient, then create a typed reader with
	// readmodels.For(store.ReadModels(), model). Get returns Instance[Person]:
	// check Exists before using Value. LastHandled is independent of the model.
	// Output: Example.Person People 1 MongoDB
}
