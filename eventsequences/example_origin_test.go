// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/eventsequences"
)

func ExampleWithOrigin() {
	origin := eventsequences.NewOrigin()
	ctx := eventsequences.WithOrigin(context.Background(), origin)
	// Pass ctx to immediate appends; filter notifications with n.Origin == origin.
	fmt.Println("same execution:", eventsequences.OriginFrom(ctx) == origin)
	fmt.Println("different execution:", eventsequences.NewOrigin() != origin)
	fmt.Println("unattributed:", eventsequences.OriginFrom(context.Background()) == (eventsequences.Origin{}))
	// Output:
	// same execution: true
	// different execution: true
	// unattributed: true
}
