// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
)

func ExampleWithOnConnected() {
	client, err := chronicle.NewClient(chronicle.WithOnConnected(func(_ context.Context, event chronicle.ConnectionEvent) {
		// For registration readiness, call Ready or WaitForRegistration rather
		// than treating this notification as an observer attachment receipt.
		fmt.Printf("connected generation %d\n", event.Generation)
	}))
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()
	// Construction does not connect or invoke hooks. Call Connect to start.
	fmt.Println("connected hook configured")
	// Output: connected hook configured
}

func ExampleWithOnDisconnected() {
	client, err := chronicle.NewClient(chronicle.WithOnDisconnected(func(_ context.Context, event chronicle.ConnectionEvent) {
		// ErrClosed pairs a delivered Connected on client shutdown too.
		fmt.Printf("disconnected generation %d\n", event.Generation)
	}))
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()
	fmt.Println("disconnected hook configured")
	// Output: disconnected hook configured
}

type preferLastServer struct{}

func (preferLastServer) Next(ctx context.Context, candidates []chronicle.ServerAddress) (chronicle.ServerAddress, error) {
	if err := ctx.Err(); err != nil {
		return chronicle.ServerAddress{}, err
	}
	return candidates[len(candidates)-1], nil
}

func ExampleWithLoadBalancer() {
	client, err := chronicle.NewClient(chronicle.WithConnectionString("chronicle://one:35000,two:35000"), chronicle.WithLoadBalancer(preferLastServer{}))
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()
	// Next runs only during Connect, not during construction.
	uri, _ := chronicle.ParseConnectionString("chronicle://one:35000,two:35000")
	selected, _ := (preferLastServer{}).Next(context.Background(), uri.Addresses())
	fmt.Println(selected)
	// Output: two:35000
}
