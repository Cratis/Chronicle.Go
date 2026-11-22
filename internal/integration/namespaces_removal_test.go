//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"errors"
	"slices"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

// TestKernelNamespaceRemoval requires the CI-owned kernel. It never skips an
// absent endpoint or an older kernel that lacks the removal RPC. It witnesses
// namespace listing and event-data isolation, not observer/reminder teardown.
func TestKernelNamespaceRemoval(t *testing.T) {
	fixture := newKernelFixture(t)
	registry := integrationRegistry[CustomerRegistered](t)
	writer := fixture.client(registry)
	for _, namespace := range []chronicle.Namespace{chronicle.DefaultNamespace, "remove-me", "keep-me"} {
		store := removalStore(t, fixture, writer, namespace)
		appendSuccessfully(t, fixture.ctx, store, "customer", CustomerRegistered{Name: string(namespace)})
	}
	// No target-scoped client remains active during removal. The kernel does
	// not enforce this operator prerequisite; a default handle is not permission.
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	admin := fixture.client(registry)
	store := removalStore(t, fixture, admin, chronicle.DefaultNamespace)
	assertRemovalNamespaceListed(t, fixture, store, "remove-me", true)
	if err := store.Namespaces().Remove(fixture.ctx, "remove-me"); err != nil {
		t.Fatal(err)
	}
	assertRemovalNamespaceListed(t, fixture, store, "remove-me", false)
	if err := store.Namespaces().Remove(fixture.ctx, "remove-me"); err != nil {
		t.Fatalf("removing a missing namespace: %v", err)
	}
	assertRemovalNamespaceListed(t, fixture, store, "remove-me", false)
	for _, namespace := range []chronicle.Namespace{chronicle.DefaultNamespace, "keep-me"} {
		assertRemovalNamespaceListed(t, fixture, store, namespace, true)
		assertRemovalEventCount(t, fixture, removalStore(t, fixture, admin, namespace), 1)
	}
	assertDefaultNamespaceRemovalRefused(t, fixture, store)
	// Reopen only after proving absence: EventStore deliberately ensures a
	// namespace and can recreate it. A fresh client must see no old event data.
	reopened := fixture.client(registry)
	target := removalStore(t, fixture, reopened, "remove-me")
	assertRemovalEventCount(t, fixture, target, 0)
}

func removalStore(t *testing.T, fixture *kernelFixture, client *chronicle.Client, namespace chronicle.Namespace) *chronicle.EventStore {
	t.Helper()
	store, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace(namespace))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func assertRemovalNamespaceListed(t *testing.T, fixture *kernelFixture, store *chronicle.EventStore, namespace chronicle.Namespace, want bool) {
	t.Helper()
	namespaces, err := store.Namespaces().List(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(namespaces, namespace) != want {
		t.Fatalf("namespace %q listed = %v; want %v (list %v)", namespace, !want, want, namespaces)
	}
}

func assertRemovalEventCount(t *testing.T, fixture *kernelFixture, store *chronicle.EventStore, want int) {
	t.Helper()
	got, err := store.EventLog().ReadSource(fixture.ctx, events.SourceID("customer"), eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != want {
		t.Fatalf("namespace %q event count = %d; want %d", store.Namespace(), len(got), want)
	}
}

func assertDefaultNamespaceRemovalRefused(t *testing.T, fixture *kernelFixture, store *chronicle.EventStore) {
	t.Helper()
	for _, namespace := range []chronicle.Namespace{chronicle.DefaultNamespace, "dEfAuLt"} {
		var envelope *chronicle.EnvelopeError
		if err := store.Namespaces().Remove(fixture.ctx, namespace); !errors.As(err, &envelope) {
			t.Fatalf("default namespace %q removal error = %v; want a server envelope refusal", namespace, err)
		}
	}
	assertRemovalNamespaceListed(t, fixture, store, chronicle.DefaultNamespace, true)
	assertRemovalEventCount(t, fixture, store, 1)
}
