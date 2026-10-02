// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"errors"
	"sync"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

type CustomerRegistered struct {
	Name string `json:"name"`
}
type CustomerRenamed struct {
	Name string `json:"name"`
}

func TestRegistration(t *testing.T) {
	registry := chronicle.NewRegistry()
	declaration, err := chronicle.RegisterEvent[CustomerRegistered](registry, events.WithID("customer-registered"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	if declaration.Ref() != (events.TypeRef{ID: "customer-registered", Generation: 2}) {
		t.Fatal(declaration.Ref())
	}
	catalog, err := events.NewCatalog(declaration.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{CustomerRegistered{}, &CustomerRegistered{}} {
		if _, found := catalog.Lookup(value); !found {
			t.Fatalf("missing %T", value)
		}
	}
	var nilEvent *CustomerRegistered
	if _, found := catalog.Lookup(nilEvent); found {
		t.Fatal("nil event matched")
	}
	if _, err = chronicle.RegisterEvent[CustomerRegistered](registry); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if _, err = chronicle.RegisterEvent[CustomerRenamed](registry, events.WithID("customer-registered")); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if _, err = chronicle.RegisterEvent[*CustomerRegistered](chronicle.NewRegistry()); err == nil {
		t.Fatal("pointer declaration accepted")
	}
	if _, err = chronicle.RegisterEvent[CustomerRegistered](nil); err == nil {
		t.Fatal("nil registry accepted")
	}
	if _, err = chronicle.RegisterEvent[CustomerRegistered](chronicle.NewRegistry(), events.WithGeneration(0)); err == nil {
		t.Fatal("zero generation accepted")
	}
}

func TestConcurrentDuplicateRegistration(t *testing.T) {
	registry := chronicle.NewRegistry()
	results := make(chan error, 16)
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() { _, err := chronicle.RegisterEvent[CustomerRegistered](registry); results <- err })
	}
	group.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successes = %d", successes)
	}
}
