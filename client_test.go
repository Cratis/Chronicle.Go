// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc/metadata"
)

func TestStoreRegistrationCacheAndNamespaceIsolation(t *testing.T) {
	kernel := &fakeKernel{}
	registered := make(chan *eventtypes.RegisterEventTypesRequest, 4)
	kernel.register = func(request *eventtypes.RegisterEventTypesRequest) { registered <- request }
	client, conn := testClient(t, kernel)
	ctx := testContext(t)
	results := make(chan *chronicle.EventStore, 12)
	var group sync.WaitGroup
	for range 12 {
		group.Go(func() {
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Error(err)
				return
			}
			results <- store
		})
	}
	group.Wait()
	close(results)
	var first *chronicle.EventStore
	count := 0
	for store := range results {
		count++
		if first == nil {
			first = store
		}
		if first != store {
			t.Fatal("cache did not share handles")
		}
	}
	if count != 12 || kernel.registrations.Load() != 1 || kernel.connectCalls.Load() != 1 {
		t.Fatalf("count=%d registrations=%d connections=%d", count, kernel.registrations.Load(), kernel.connectCalls.Load())
	}
	log, logError := first.EventSequence(events.EventLog)
	if first.Name() != "customers" || first.Namespace() != chronicle.DefaultNamespace || log != first.EventLog() || logError != nil {
		t.Fatal("wrong store coordinates")
	}
	if _, err := first.EventSequence(""); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	request := <-registered
	if len(request.Types) != 1 || request.Types[0].Type.Id != "CustomerRegistered" || request.Types[0].Type.Generation != 1 || request.Types[0].Schema == "" {
		t.Fatal("incomplete registration")
	}
	other, err := client.EventStore(ctx, "customers", chronicle.WithNamespace("tenant-b"))
	if err != nil || other == first || other.Namespace() != "tenant-b" {
		t.Fatalf("namespace isolation: %v", err)
	}
	if _, err = other.EventLog().Append(ctx, "source", CustomerRegistered{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	stores, err := client.EventStores(ctx)
	if err != nil || len(stores) != 1 {
		t.Fatalf("stores: %v %v", stores, err)
	}
	namespaces, err := first.Namespaces().List(ctx)
	if err != nil || len(namespaces) != 1 {
		t.Fatalf("namespaces: %v %v", namespaces, err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = client.EventStore(ctx, "customers"); !errors.Is(err, chronicle.ErrClosed) {
		t.Fatal(err)
	}
	if _, err = first.EventLog().Append(ctx, "source", CustomerRegistered{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); !errors.Is(err, chronicle.ErrClosed) {
		t.Fatal(err)
	}
	if _, err = clients.NewConnectionServiceClient(conn).CheckCompatibility(ctx, &clients.CompatibilityRequest{}); err != nil {
		t.Fatalf("borrowed connection was closed: %v", err)
	}
}

func TestRegistrationFailureDoesNotPoisonCache(t *testing.T) {
	kernel := &fakeKernel{}
	kernel.failRegistration.Store(true)
	client, _ := testClient(t, kernel)
	ctx := testContext(t)
	_, err := client.EventStore(ctx, "customers")
	var envelope *chronicle.EnvelopeError
	if !errors.As(err, &envelope) || len(envelope.ExceptionMessages) != 1 {
		t.Fatalf("failure = %v", err)
	}
	if _, err = client.EventStore(ctx, "customers"); err != nil {
		t.Fatal(err)
	}
	if kernel.registrations.Load() != 2 {
		t.Fatal("registration was not retried")
	}
}

func TestRegistrySnapshotAndPerStoreSelection(t *testing.T) {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CustomerRegistered](registry); err != nil {
		t.Fatal(err)
	}
	override := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CustomerRenamed](override); err != nil {
		t.Fatal(err)
	}
	client, _ := testClient(t, &fakeKernel{}, chronicle.WithRegistry(registry), chronicle.WithRegistryForStore("other", override))
	if _, err := chronicle.RegisterEvent[CustomerRenamed](registry); err != nil {
		t.Fatal(err)
	}
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	if _, found := store.EventTypes().Lookup(CustomerRenamed{}); found {
		t.Fatal("registry mutation leaked")
	}
	other, err := client.EventStore(ctx, "other")
	if err != nil {
		t.Fatal(err)
	}
	if _, found := other.EventTypes().Lookup(CustomerRegistered{}); found {
		t.Fatal("store registry leaked")
	}
	if _, found := other.EventTypes().Lookup(CustomerRenamed{}); !found {
		t.Fatal("override missing")
	}
}

func TestCompatibilityFailsClosed(t *testing.T) {
	kernel := &fakeKernel{incompatible: true}
	client, _ := testClient(t, kernel)
	_, err := client.EventStore(testContext(t), "customers")
	var mismatch *chronicle.CompatibilityError
	if !errors.As(err, &mismatch) || kernel.registrations.Load() != 0 {
		t.Fatalf("incompatibility ignored: %v", err)
	}
}

type tokenSource struct{}

func (tokenSource) Token(context.Context) (chronicle.Token, error) {
	return chronicle.Token{AccessToken: "test-credential"}, nil
}

func TestBearerAndCallerMetadataSurvivePreflight(t *testing.T) {
	seen := make(chan bool, 1)
	kernel := &fakeKernel{compatibility: func(ctx context.Context, request *clients.CompatibilityRequest) {
		md, _ := metadata.FromIncomingContext(ctx)
		seen <- len(md.Get("authorization")) == 1 && md.Get("authorization")[0] == "Bearer test-credential" && md.Get("custom")[0] == "value" && request.ClientType == "Go" && request.ProtocolVersion == contracts.ProtocolVersion && len(request.DescriptorSet) > 0
	}}
	conn := kernelConnection(t, kernel)
	client, err := chronicle.NewClient(chronicle.WithGRPCConnection(conn), chronicle.WithTokenSource(tokenSource{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := metadata.AppendToOutgoingContext(testContext(t), "custom", "value", "authorization", "old")
	if err = client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if !<-seen {
		t.Fatal("preflight descriptor or metadata incorrect")
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	if md.Get("authorization")[0] != "old" {
		t.Fatal("caller metadata mutated")
	}
}
