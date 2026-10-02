// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	chronicle "github.com/cratis/chronicle.go"
	"testing"
)

func TestBothExampleRegistriesCompile(t *testing.T) {
	bound, _, err := declarations()
	if err != nil {
		t.Fatal(err)
	}
	fluent, _, err := fluentDeclarations()
	if err != nil {
		t.Fatal(err)
	}
	for _, registry := range []*chronicle.Registry{bound, fluent} {
		client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
		if err != nil {
			t.Fatal(err)
		}
		if err = client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
