// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
)

func ExampleWithMaxSendMessageSize() {
	client, err := chronicle.NewClient(
		chronicle.WithMaxSendMessageSize(8*1024*1024),
		chronicle.WithMaxReceiveMessageSize(16*1024*1024),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Construction is offline; Connect or Dial performs authenticated startup.
	fmt.Println(client)
	if err := client.Close(); err != nil {
		fmt.Println(err)
	}
	// Output: Chronicle client
}

func ExampleWithMaxReceiveMessageSize() {
	// A later scalar option overrides an earlier one. Neither zero nor negative
	// values mean unlimited: NewClient validates the final bound.
	client, err := chronicle.NewClient(
		chronicle.WithMaxReceiveMessageSize(4*1024*1024),
		chronicle.WithMaxReceiveMessageSize(8*1024*1024),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(client)
	if err := client.Close(); err != nil {
		fmt.Println(err)
	}
	// Output: Chronicle client
}

func ExampleWithSkipKeepAlive() {
	// A short-lived, one-shot client: do not configure reactors/reducers or an
	// explicit keepalive timeout. Compatibility and the protected startup probe
	// remain enabled. HTTP/2 keepalive is unchanged.
	client, err := chronicle.NewClient(chronicle.WithSkipKeepAlive())
	if err != nil {
		fmt.Println(err)
		return
	}
	// Use client.Connect(ctx), then ordinary store/append/read APIs. Without a
	// logical session, transport reconnect is not a registration-replay guarantee.
	fmt.Println(client)
	if err := client.Close(); err != nil {
		fmt.Println(err)
	}
	// Output: Chronicle client
}
