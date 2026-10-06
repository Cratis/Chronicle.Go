//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/seeding"
)

type CatalogSeeded struct{ Name string }

func TestKernelSeedingRestartReconnectNamespaceIsolationAndObserverVisibility(t *testing.T) {
	fixture := newKernelFixture(t)
	t.Log("store", fixture.storeName)
	// Establish existing namespaces before global seeding, without registering
	// application observers or substituting ordinary appends for the seed RPC.
	setup := fixture.client(chronicle.NewRegistry())
	for _, ns := range []chronicle.Namespace{chronicle.DefaultNamespace, "red", "blue"} {
		if _, err := setup.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace(ns)); err != nil {
			t.Fatal(err)
		}
	}
	if err := setup.Close(); err != nil {
		t.Fatal(err)
	}
	registry := integrationRegistry[CatalogSeeded](t, events.WithID("go-catalog-seeded"), events.WithTags("reference-data"))
	var preparations atomic.Int32
	if err := chronicle.RegisterSeederFunc(registry, func(b *seeding.Builder) error {
		preparations.Add(1)
		// Repeated equal entries are two facts, not client-side deduplication.
		seeding.For(b, "global", CatalogSeeded{Name: "Global"}, CatalogSeeded{Name: "Global"})
		seeding.For(b.ForNamespace("red"), "red-only", CatalogSeeded{Name: "Red"})
		seeding.For(b.ForNamespace(chronicle.DefaultNamespace), "default-only", CatalogSeeded{Name: "Default"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	observed := make(chan string, 32)
	if err := chronicle.RegisterReactorHandler(registry, "seed-observer", func(ctx context.Context, value CatalogSeeded) error {
		select {
		case observed <- value.Name:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry)
	if _, err := client.EventStore(fixture.ctx, fixture.storeName); err != nil {
		t.Fatal(err)
	}
	verify := func(client *chronicle.Client, ns chronicle.Namespace) {
		t.Helper()
		store, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace(ns))
		if err != nil {
			t.Fatal(err)
		}
		for _, source := range []events.SourceID{"global", "red-only", "default-only"} {
			got, err := store.EventLog().ReadSource(fixture.ctx, source, eventsequences.SourceFilter{})
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if source == "global" {
				want = 2
			}
			if source == "red-only" && ns == "red" || source == "default-only" && ns == chronicle.DefaultNamespace {
				want = 1
			}
			if len(got) != want {
				t.Fatalf("namespace %q source %q: got %d seeds, want %d", ns, source, len(got), want)
			}
			for _, event := range got {
				var content CatalogSeeded
				if err := json.Unmarshal(event.Content, &content); err != nil {
					t.Fatal(err)
				}
				wantName := map[events.SourceID]string{"global": "Global", "red-only": "Red", "default-only": "Default"}[source]
				if content.Name != wantName {
					t.Fatalf("namespace %q source %q: got content %+v, want Name %q", ns, source, content, wantName)
				}
				if event.Context.EventType.ID != "go-catalog-seeded" || event.Context.EventType.Generation != 1 || !slices.Contains(event.Context.Tags, events.Tag("reference-data")) {
					t.Fatal("seed metadata", event.Context)
				}
			}
		}
	}
	// Prove persistence before considering the known kernel delivery failure.
	verify(client, chronicle.DefaultNamespace)
	// A skipped delivery assertion must not skip restart/reconnect/idempotence.
	if !t.Run("observer_visibility", func(t *testing.T) {
		awaitSeedObserver(t, fixture, observed)
	}) {
		return
	}
	for _, ns := range []chronicle.Namespace{chronicle.DefaultNamespace, "red", "blue"} {
		verify(client, ns)
	}
	if preparations.Load() != 1 {
		t.Fatal("seeder rerun for namespace", preparations.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	uri, err := chronicle.ParseConnectionString(fixture.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	relay := newRelay(t, uri.Addresses()[0].String())
	restarted := fixture.client(registry, chronicle.WithConnectionString("chronicle://"+relay.listener.Addr().String()))
	for _, ns := range []chronicle.Namespace{chronicle.DefaultNamespace, "red", "blue"} {
		verify(restarted, ns)
	}
	store, err := restarted.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	relay.cut()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-fixture.ctx.Done():
			t.Fatal(fixture.ctx.Err())
		case <-ticker.C:
		}
		after, err := store.WaitForRegistration(fixture.ctx)
		if err == nil && after.IsSuccess() && after.Generation > before.Generation {
			break
		}
	}
	for _, ns := range []chronicle.Namespace{chronicle.DefaultNamespace, "red", "blue"} {
		verify(restarted, ns)
	}
	if preparations.Load() != 2 {
		t.Fatal("reconnect reran seeder", preparations.Load())
	}
}

func awaitSeedObserver(t *testing.T, fixture *kernelFixture, observed <-chan string) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	var names []string
	for len(names) < 3 {
		select {
		case name := <-observed:
			if name != "Global" && name != "Default" {
				t.Fatalf("unexpected seed delivered in default namespace: %q", name)
			}
			names = append(names, name)
		case <-fixture.ctx.Done():
			t.Fatal("seed not visible to observer", fixture.ctx.Err())
		case <-deadline.C:
			info, err := observation.NewObserversClient(fixture.conn).GetObserverInformation(fixture.ctx, &observation.GetObserverInformationRequest{
				EventStore: string(fixture.storeName), Namespace: string(chronicle.DefaultNamespace), EventSequenceId: "event-log", ObserverId: "seed-observer",
			})
			if err != nil {
				t.Fatal(err)
			}
			failures, err := observation.NewFailedPartitionsClient(fixture.conn).GetFailedPartitions(fixture.ctx, &observation.GetFailedPartitionsRequest{
				EventStore: string(fixture.storeName), Namespace: string(chronicle.DefaultNamespace), ObserverId: "seed-observer",
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Fatalf("seed delivery timed out: observed %v, observer %v, failures %v", names, info, failures)
		}
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"Default", "Global", "Global"}) {
		t.Fatalf("observed seeds %v, want Default and two Global events", names)
	}
}
