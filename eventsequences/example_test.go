// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

func ExampleHistory_Scope() {
	// ReadHistory supplies these fields from one successful read. This empty
	// fixture illustrates converting protected absence into a batch check.
	source := events.SourceID("customer-42")
	history := eventsequences.History{
		Filter:      eventsequences.ScopeFilter{SourceID: &source},
		Expectation: eventsequences.NoMatchingEvent(),
	}
	check := eventsequences.LabeledScope{Label: string(source), Scope: history.Scope()}
	fmt.Println(check.Label, check.Scope.Expectation == eventsequences.NoMatchingEvent())
	// Output: customer-42 true
}
