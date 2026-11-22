// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
)

func ExampleUniqueValues() {
	registry := chronicle.NewRegistry()
	contact, err := chronicle.RegisterEvent[Contact](registry, events.WithID("contact-registered"))
	if err != nil {
		fmt.Println(err)
		return
	}
	removed, err := chronicle.RegisterEvent[Removed](registry, events.WithID("contact-removed"))
	if err != nil {
		fmt.Println(err)
		return
	}
	unique, err := constraints.UniqueValues("UniqueEmail").
		On(contact.Descriptor(), "emailAddress").
		IgnoreCasing().RemovedWith(removed.Descriptor()).Build()
	if err != nil {
		fmt.Println(err)
		return
	}
	if err = registry.AddConstraint(unique); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(unique.Name(), unique.IgnoresCasing())
	// Supply chronicle.WithRegistry(registry) when constructing the client.
	// Output: UniqueEmail true
}

func ExampleBuilder_WithMode() {
	registry := chronicle.NewRegistry()
	contact, err := chronicle.RegisterEvent[Contact](registry, events.WithID("contact-registered"))
	if err != nil {
		fmt.Println(err)
		return
	}
	latest, err := constraints.UniqueValues("LatestEmail").
		On(contact.Descriptor(), "emailAddress").WithMode(constraints.PerEventSource).Build()
	if err != nil {
		fmt.Println(err)
		return
	}
	retained, err := constraints.UniqueValues("RetainedEmails").
		On(contact.Descriptor(), "emailAddress").WithMode(constraints.PerValue).Build()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(latest.Mode(), retained.Mode())
	// Register only the definition that matches the application's retention rule.
	// Output: 0 1
}
