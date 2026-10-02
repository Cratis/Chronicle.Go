// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

func ExampleRegisterEvent() {
	registry := chronicle.NewRegistry()
	eventType, err := chronicle.RegisterEvent[CustomerRegistered](registry,
		events.WithID("customer-registered"))
	if err != nil {
		fmt.Println(err)
		return
	}
	content, err := eventType.Descriptor().Marshal(CustomerRegistered{Name: "Ada"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(eventType.Ref().ID, eventType.Ref().Generation)
	fmt.Println(string(content))
	// Output:
	// customer-registered 1
	// {"name":"Ada"}
}

func ExampleParseConnectionString() {
	connection, err := chronicle.ParseConnectionString("chronicle://client:secret@localhost")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(connection)
	// Output: chronicle://localhost:35000
}
