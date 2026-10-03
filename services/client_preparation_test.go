// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/seeding"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type preparationCollaborator struct {
	client *chronicle.Client
	closed *int
}

func (c *preparationCollaborator) Seed(*seeding.Builder) error {
	// Identity is available for composition, not operational use during preparation.
	if err := c.client.Ready(context.Background()); !errors.Is(err, chronicle.ErrNotPrepared) {
		return errors.New("expected an unprepared identity")
	}
	return nil
}
func (c *preparationCollaborator) Close() error { *c.closed++; return nil }

// This is a zero-I/O catalog consumer, not an Arc adoption test.
func newCatalogConsumer(client *chronicle.Client) error {
	_, _, err := client.Catalogs("consumer")
	return err
}

func TestSharedProviderBorrowsCapturedIdentityBeforeCatalogConsumer(t *testing.T) {
	registry := chronicle.NewRegistry()
	if err := chronicle.RegisterSeederFactory[*preparationCollaborator](registry, nil); err != nil {
		t.Fatal(err)
	}
	p, err := chronicle.CaptureClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	if err := newCatalogConsumer(p.Client()); !errors.Is(err, chronicle.ErrNotPrepared) {
		t.Fatal("consumer accepted an unprepared client", err)
	}
	var bindings container.Registry
	if err := dependencyinjection.BindValue(&bindings, p.Client()); err != nil {
		t.Fatal(err)
	}
	closed, constructed := 0, 0
	if err := dependencyinjection.BindFunc1(&bindings, dependencyinjection.Singleton, func(_ context.Context, client *chronicle.Client) (*preparationCollaborator, error) {
		if client != p.Client() {
			t.Fatal("identity changed")
		}
		constructed++
		return &preparationCollaborator{client, &closed}, nil
	}); err != nil {
		t.Fatal(err)
	}
	provider, err := bindings.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close(context.Background()) }()
	defer func() { _ = p.Client().Close() }()
	client, err := services.PrepareClient(t.Context(), p, provider)
	if err != nil || client != p.Client() {
		t.Fatal(client, err)
	}
	if err := newCatalogConsumer(client); err != nil {
		t.Fatal(err)
	}
	if constructed != 1 || closed != 0 {
		t.Fatal("provider resource ownership changed", constructed, closed)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if closed != 0 {
		t.Fatal("client closed provider singleton")
	}
	if err := provider.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatal("provider did not close singleton exactly once")
	}
	if err := newCatalogConsumer(client); err != nil {
		t.Fatal("historical catalogs lost", err)
	}
}
