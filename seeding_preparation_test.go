// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/seeding"
	"github.com/cratis/chronicle.go/serialization"
)

type declarationSeeder struct {
	calls  *int
	closed *int
}

func (s *declarationSeeder) Seed(b *seeding.Builder) error {
	(*s.calls)++
	seeding.For(b, "one", lifecycleEvent{Name: "prepared"})
	return nil
}
func (s *declarationSeeder) Close() error { (*s.closed)++; return nil }

func TestSeederInstancesAndConstructorsHaveExplicitOwnership(t *testing.T) {
	for _, constructed := range []bool{false, true} {
		t.Run(map[bool]string{false: "borrowed", true: "constructed"}[constructed], func(t *testing.T) {
			calls, closed := 0, 0
			registry := NewRegistry()
			if _, err := RegisterEvent[lifecycleEvent](registry); err != nil {
				t.Fatal(err)
			}
			seeder := &declarationSeeder{&calls, &closed}
			var err error
			if constructed {
				err = RegisterSeederFactory[*declarationSeeder](registry, func() *declarationSeeder { return seeder })
			} else {
				err = RegisterSeeder(registry, seeder)
			}
			if err != nil {
				t.Fatal(err)
			}
			if calls != 0 {
				t.Fatal("seeder ran at admission")
			}
			if err := RegisterSeeder(registry, seeder); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatal("duplicate seeder admitted", err)
			}
			client, err := NewClient(WithRegistry(registry))
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || closed != map[bool]int{false: 0, true: 1}[constructed] {
				t.Fatal(calls, closed)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if closed != map[bool]int{false: 0, true: 1}[constructed] {
				t.Fatal("client reclosed seeder")
			}
		})
	}
}

func TestSeedersPrepareInOrderAgainstSelectedFrozenNamingCatalog(t *testing.T) {
	var order []int
	registry := NewRegistry()
	for i := range 2 {
		if err := RegisterSeederFunc(registry, func(b *seeding.Builder) error {
			order = append(order, i)
			seeding.For(b, "source", lifecycleEvent{Name: "seed"})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Forward references are valid at admission, but resolved before I/O.
	if _, err := RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry), WithRegistryForStore("empty", NewRegistry()), WithNamingPolicy(serialization.CamelCase))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if !slices.Equal(order, []int{0, 1}) {
		t.Fatal(order)
	}
	global := (&EventStore{client: client, name: "other"}).seedDefinition()
	if got := global.Contract("other").GlobalByEventSource[0].Entries[0].Content; got != `{"name":"seed"}` {
		t.Fatal(got)
	}
	if !(&EventStore{client: client, name: "empty"}).seedDefinition().IsEmpty() {
		t.Fatal("default seeds leaked into replacement store")
	}
	bad := NewRegistry()
	if err := RegisterSeederFunc(bad, func(b *seeding.Builder) error { seeding.For(b, "one", lifecycleEvent{}); return nil }); err != nil {
		t.Fatal(err)
	}
	if c, err := NewClient(WithRegistry(registry), WithRegistryForStore("bad", bad)); c != nil || !errors.Is(err, ErrNotRegistered) {
		t.Fatal("catalog leaked into isolated store", c, err)
	}
}

func TestSeederPreparationRejectsInvalidRegistrationsAndPreservesConstructionFailure(t *testing.T) {
	if err := RegisterSeeder(nil, (*declarationSeeder)(nil)); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if err := RegisterSeederFunc(NewRegistry(), nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	failure := errors.New("construction failure")
	calls, closed := 0, 0
	registry := NewRegistry()
	if err := RegisterSeederFactory[*declarationSeeder](registry, func() (*declarationSeeder, error) { return &declarationSeeder{&calls, &closed}, failure }); err != nil {
		t.Fatal(err)
	}
	if c, err := NewClient(WithRegistry(registry)); c != nil || !errors.Is(err, failure) || closed != 1 || calls != 0 {
		t.Fatal(c, err, calls, closed)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := NewClientContext(canceled, WithRegistry(registry)); !errors.Is(err, context.Canceled) || closed != 1 {
		t.Fatal(err, closed)
	}
}
