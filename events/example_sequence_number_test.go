// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"errors"
	"fmt"

	"github.com/cratis/chronicle.go/events"
)

func ExampleSequenceNumber_Next() {
	position, err := events.First.Add(2)
	if err != nil {
		fmt.Println("add:", err)
		return
	}
	next, err := position.Next()
	if err != nil {
		fmt.Println("next:", err)
		return
	}
	previous, err := next.Subtract(1)
	if err != nil {
		fmt.Println("subtract:", err)
		return
	}
	fmt.Println(position, next, previous, next.IsActualValue())

	beforeFirst, err := events.BeforeFirst.Next()
	if err != nil {
		fmt.Println("sentinel:", err)
		return
	}
	fmt.Println("still before first:", beforeFirst.IsBeforeFirst())

	unavailable, err := (events.BeforeFirst - 1).Next()
	if errors.Is(err, events.ErrSequenceNumberRange) {
		fmt.Println("range exhausted:", unavailable.IsUnavailable())
	} else if err != nil {
		fmt.Println("unexpected:", err)
		return
	}
	// Output:
	// 2 3 2 true
	// still before first: true
	// range exhausted: true
}
