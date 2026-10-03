// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"context"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
)

func TestWatchExampleRegistersWithoutAContainer(t *testing.T) {
	names := make(chan string, 1)
	declarations, _, err := registry(names, make(chan error, 1))
	if err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(declarations))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	reactor := &AnnounceProduct{names}
	if err := reactor.Added(context.Background(), &Product{Name: "Coffee"}); err != nil {
		t.Fatal(err)
	}
	if name := <-names; name != "Coffee" {
		t.Fatal(name)
	}
}
