// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
)

type AccountRegistered struct {
	Email string  `json:"email" chronicle:"unique(name=\"email\",message=\"Email already registered\",sequences=[\"event-log\"])"`
	Owner *string `json:"owner" chronicle:"subject"`
}
type AccountClosed struct{}

func ExampleRegistry_ConfigureDeclaredConstraint() {
	registry := chronicle.NewRegistry()
	_, err := chronicle.RegisterEvent[AccountRegistered](registry,
		events.WithUnique(events.Unique{Name: "account-lifecycle"}), events.WithTags("accounts"))
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = chronicle.RegisterEvent[AccountClosed](registry, events.WithRemoveConstraints("email", "account-lifecycle"))
	if err != nil {
		fmt.Println(err)
		return
	}
	// Use the existing builder to explicitly compose a tagged declaration.
	if err = registry.ConfigureDeclaredConstraint("email", func(builder *constraints.Builder) { builder.IgnoreCasing() }); err != nil {
		fmt.Println(err)
		return
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			fmt.Println(err)
		}
	}()
	catalog, _, err := client.Catalogs("accounts")
	if err != nil {
		fmt.Println(err)
		return
	}
	owner := "customer-42"
	event := AccountRegistered{Email: "ada@example.test", Owner: &owner}
	descriptor, _ := catalog.Lookup(event)
	subject, present := descriptor.ResolveSubject(event)
	fmt.Println(subject, present, descriptor.Tags())
	// Output: customer-42 true [accounts]
}
