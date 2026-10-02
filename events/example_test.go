// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

type CustomerChanged struct{ CustomerID string }

func ExampleWithSubjectResolver() {
	registry := chronicle.NewRegistry()
	registered, err := chronicle.RegisterEvent[CustomerChanged](registry,
		events.WithSubjectResolver(func(event CustomerChanged) (events.Subject, bool) {
			return events.Subject(event.CustomerID), event.CustomerID != ""
		}))
	if err != nil {
		panic(err)
	}
	subject, found := registered.Descriptor().ResolveSubject(CustomerChanged{CustomerID: "customer-42"})
	fmt.Println(subject, found)
	// Output: customer-42 true
}
