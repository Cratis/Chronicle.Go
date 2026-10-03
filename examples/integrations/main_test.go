// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

func Example() {
	if err := run(); err != nil {
		panic(err)
	}
	// Output:
	// observer=inbox-orders subscription=orders webhook=notify service=OrdersApi
	// capture ImportOrders
	//   source api
	//     api OrdersApi
	//     route /orders
	//     poll 5m
	//   key id
	//   append OrderPlaced
	//     when added
	//     OrderNumber = $.number
}
