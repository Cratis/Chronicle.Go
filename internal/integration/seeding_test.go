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
	ensureSeedNamespaces(t, fixture)
	var preparations atomic.Int32
	observed := make(chan string, 32)
	registry := seedRegistry(t, &preparations, observed)
	client := fixture.client(registry)
	if _, err := client.EventStore(fixture.ctx, fixture.storeName); err != nil {
		t.Fatal(err)
	}
	// Prove persistence before considering the known kernel delivery failure.
	positions := map[chronicle.Namespace]map[events.SourceID][]events.SequenceNumber{
		chronicle.DefaultNamespace: readSeedNamespace(t, fixture, client, chronicle.DefaultNamespace),
	}
	// A skipped delivery assertion must not skip restart/reconnect/idempotence.
	if !t.Run("observer_visibility", func(t *testing.T) {
		awaitSeedObserver(t, fixture, observed)
	}) {
		return
	}
	verifySeedReplay(t, fixture, client, positions)
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
	verifySeedReplay(t, fixture, restarted, positions)
	store, err := restarted.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	awaitSeedReconnect(t, fixture, store, relay)
	verifySeedReplay(t, fixture, restarted, positions)
	if preparations.Load() != 2 {
		t.Fatal("reconnect reran seeder", preparations.Load())
	}
}

func ensureSeedNamespaces(t *testing.T, fixture *kernelFixture) {
	t.Helper()
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
}

func seedRegistry(t *testing.T, preparations *atomic.Int32, observed chan<- string) *chronicle.Registry {
	t.Helper()
	registry := integrationRegistry[CatalogSeeded](t, events.WithID("go-catalog-seeded"), events.WithTags("reference-data"))
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
	return registry
}

func readSeedNamespace(t *testing.T, fixture *kernelFixture, client *chronicle.Client, ns chronicle.Namespace) map[events.SourceID][]events.SequenceNumber {
	t.Helper()
	store, err := client.EventStore(fixture.ctx, fixture.storeName, chronicle.WithNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	want := map[events.SourceID][]string{"global": {"Global", "Global"}, "red-only": nil, "default-only": nil}
	switch ns {
	case "red":
		want["red-only"] = []string{"Red"}
	case chronicle.DefaultNamespace:
		want["default-only"] = []string{"Default"}
	}
	positions := make(map[events.SourceID][]events.SequenceNumber)
	for _, source := range []events.SourceID{"global", "red-only", "default-only"} {
		positions[source] = assertSeedSource(t, fixture, store, source, want[source])
	}
	return positions
}

func assertSeedSource(t *testing.T, fixture *kernelFixture, store *chronicle.EventStore, source events.SourceID, want []string) []events.SequenceNumber {
	t.Helper()
	got, err := store.EventLog().ReadSource(fixture.ctx, source, eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("namespace %q source %q: got %d seeds, want %d", store.Namespace(), source, len(got), len(want))
	}
	positions := make([]events.SequenceNumber, len(got))
	for i, event := range got {
		var content CatalogSeeded
		if err := json.Unmarshal(event.Content, &content); err != nil {
			t.Fatal(err)
		}
		if content.Name != want[i] {
			t.Fatalf("namespace %q source %q: got content %+v, want Name %q", store.Namespace(), source, content, want[i])
		}
		assertSeedMetadata(t, store, event)
		positions[i] = event.Context.SequenceNumber
	}
	return positions
}

func assertSeedMetadata(t *testing.T, store *chronicle.EventStore, event events.Appended) {
	t.Helper()
	if event.Context.Namespace != store.Namespace() {
		t.Fatalf("seed namespace %q, want %q", event.Context.Namespace, store.Namespace())
	}
	if event.Context.EventType != (events.TypeRef{ID: "go-catalog-seeded", Generation: 1}) {
		t.Fatal("seed type/generation", event.Context.EventType)
	}
	if !slices.Contains(event.Context.Tags, events.Tag("reference-data")) {
		t.Fatal("seed static tags", event.Context.Tags)
	}
}

func verifySeedReplay(t *testing.T, fixture *kernelFixture, client *chronicle.Client, positions map[chronicle.Namespace]map[events.SourceID][]events.SequenceNumber) {
	t.Helper()
	for _, ns := range []chronicle.Namespace{chronicle.DefaultNamespace, "red", "blue"} {
		got := readSeedNamespace(t, fixture, client, ns)
		want, recorded := positions[ns]
		if !recorded {
			positions[ns] = got
			continue
		}
		for _, source := range []events.SourceID{"global", "red-only", "default-only"} {
			if !slices.Equal(got[source], want[source]) {
				t.Fatalf("namespace %q source %q: seed positions changed: got %v, want %v", ns, source, got[source], want[source])
			}
		}
	}
}

func awaitSeedReconnect(t *testing.T, fixture *kernelFixture, store *chronicle.EventStore, relay *cuttableRelay) {
	t.Helper()
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
