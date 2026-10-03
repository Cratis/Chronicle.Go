// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
)

func TestStockDefaultsRegistryCompiles(t *testing.T) {
	registry, _, err := stockDeclarations()
	if err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	artifacts, err := client.Artifacts("stock")
	if err != nil {
		t.Fatal(err)
	}
	definition := artifacts.Projections[0].KernelDefinition()
	if definition.InitialModelState != `{"available":10,"id":"","locations":[],"name":"","note":null}` || len(definition.Tags) != 2 {
		t.Fatalf("stock defaults: %+v", definition)
	}
}
