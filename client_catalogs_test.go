// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type catalogEvent struct{ Title string }
type catalogModel struct{ ID, Title string }
type catalogReplacement struct{ Value string }

func catalogRegistry(t *testing.T) *Registry {
	t.Helper()
	r := NewRegistry()
	event, err := RegisterEvent[catalogEvent](r, events.WithSourceStore("origin"), events.WithTags("catalog"))
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[catalogModel](r, readmodels.WithIndexes("Title"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(model, projections.FromEvent(event))); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCatalogsMatchConnectedStoreAndBindProducer(t *testing.T) {
	r := catalogRegistry(t)
	kernel := &supervisedKernel{
		readModels:  &readModelKernel{register: func(context.Context, *modelcontracts.RegisterManyRequest) error { return nil }},
		projections: &projectionKernel{register: func(context.Context, *projectioncontracts.RegisterRequest) error { return nil }},
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(r), WithNamingPolicy(serialization.CamelCase))
	for _, name := range []StoreName{"origin", "consumer"} {
		eventCatalog, modelCatalog, err := client.Catalogs(name)
		if err != nil {
			t.Fatal(err)
		}
		store, err := client.EventStore(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(eventCatalog.Descriptors(), store.EventTypes().Descriptors()) || !reflect.DeepEqual(modelCatalog.Descriptors(), store.ReadModels().Catalog().Descriptors()) {
			t.Fatal("offline and connected catalogs differ")
		}
		model := modelCatalog.Descriptors()[0]
		kind, producer := model.Observer()
		want := events.SequenceID("inbox-origin")
		if name == "origin" {
			want = events.EventLog
		}
		if model.EventSequence() != want || kind != readmodels.Projection || producer != store.Projections()[0].Identifier() || model.Indexes()[0] != "title" {
			t.Fatal("store binding, naming or producer metadata lost")
		}
		content, err := eventCatalog.Descriptors()[0].Marshal(catalogEvent{Title: "named"})
		if err != nil || string(content) != `{"title":"named"}` {
			t.Fatalf("naming: %s %v", content, err)
		}
	}
}

func TestCatalogsAreFrozenStoreReplacementsWithoutIO(t *testing.T) {
	r := catalogRegistry(t)
	override := NewRegistry()
	if _, err := RegisterEvent[catalogReplacement](override); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterReadModel[catalogReplacement](override, readmodels.WithObserver(readmodels.Reducer, "replacement-producer")); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	transport, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
			calls.Add(1)
			return errors.New("catalog accessor must not use transport")
		}),
		grpc.WithStreamInterceptor(func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, grpc.Streamer, ...grpc.CallOption) (grpc.ClientStream, error) {
			calls.Add(1)
			return nil, errors.New("catalog accessor must not use transport")
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := transport.Close(); err != nil {
			t.Error(err)
		}
	})
	client, err := NewClient(WithRegistry(r), WithRegistryForStore("replacement", override), WithGRPCConnection(transport), WithNoAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := RegisterEvent[catalogReplacement](r); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterReadModel[catalogReplacement](r); err != nil {
		t.Fatal(err)
	}
	defaultEvents, defaultModels, err := client.Catalogs("consumer")
	if err != nil {
		t.Fatal(err)
	}
	selectedEvents, selectedModels, err := client.Catalogs("replacement")
	if err != nil {
		t.Fatal(err)
	}
	if len(defaultEvents.Descriptors()) != 1 || len(defaultModels.Descriptors()) != 1 || len(selectedEvents.Descriptors()) != 1 || len(selectedModels.Descriptors()) != 1 {
		t.Fatal("catalogs were merged or registry mutation leaked")
	}
	if _, ok := selectedEvents.Lookup(catalogEvent{}); ok {
		t.Fatal("default event leaked into replacement")
	}
	if _, ok := selectedModels.Lookup(catalogModel{}); ok {
		t.Fatal("default model leaked into replacement")
	}
	if _, ok := selectedEvents.Lookup(catalogReplacement{}); !ok {
		t.Fatal("replacement event missing")
	}
	kind, producer := selectedModels.Descriptors()[0].Observer()
	if kind != readmodels.Reducer || producer != "replacement-producer" {
		t.Fatal("replacement producer missing")
	}
	// Mutating returned collections and deriving new descriptors cannot change the snapshot.
	eventList, modelList := defaultEvents.Descriptors(), defaultModels.Descriptors()
	eventList[0], modelList[0] = events.Descriptor{}, readmodels.Descriptor{}
	tags := defaultEvents.Descriptors()[0].Tags()
	tags[0] = "mutated"
	indexes := defaultModels.Descriptors()[0].Indexes()
	indexes[0] = "mutated"
	if _, err := defaultModels.Descriptors()[0].WithNamingPolicy(serialization.CamelCase); err != nil {
		t.Fatal(err)
	}
	againEvents, againModels, err := client.Catalogs("consumer")
	if err != nil || !reflect.DeepEqual(defaultEvents.Descriptors(), againEvents.Descriptors()) || !reflect.DeepEqual(defaultModels.Descriptors(), againModels.Descriptors()) {
		t.Fatal("catalog mutation leaked")
	}
	if calls.Load() != 0 || len(client.stores) != 0 {
		t.Fatal("catalog access used transport or constructed a store")
	}
	for _, blank := range []StoreName{"", " \t"} {
		if _, _, err := client.Catalogs(blank); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("blank store: %v", err)
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Catalogs("consumer"); err != nil {
		t.Fatalf("frozen catalogs unavailable after close: %v", err)
	}
}
