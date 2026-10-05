// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
)

func ExampleClient_EvictEventStores() {
	client, err := chronicle.NewClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = client.Close() }()
	// Eviction is local, even before connecting. Old handles (if any) remain
	// usable; subsequent EventStore calls acquire new cached facades.
	fmt.Println(client.EvictEventStores() == nil)
	fmt.Println(client.EvictEventStores() == nil)
	// Output:
	// true
	// true
}
