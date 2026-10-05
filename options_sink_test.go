// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/serialization"
)

type sinkDefaultEvent struct{ Name string }
type sinkDefaultModel struct{ ID, Name string }
type sinkOverrideModel struct{ ID, Name string }
type sinkFactory struct{}

func sinkCatalogModel(t *testing.T, c *Client, store StoreName, id readmodels.Identifier) readmodels.Descriptor {
	t.Helper()
	_, catalog, err := c.Catalogs(store)
	if err != nil {
		t.Fatal(err)
	}
	model, ok := catalog.LookupIdentifier(id)
	if !ok {
		t.Fatal("missing model", id)
	}
	return model
}

func TestDefaultSinkSelectionIsDetachedAndStoreReplacementIsNotMerged(t *testing.T) {
	registry, replacement := NewRegistry(), NewRegistry()
	model, err := RegisterReadModel[sinkDefaultModel](registry, readmodels.WithIndexes("Name"))
	if err != nil {
		t.Fatal(err)
	}
	const configID = "00112233-4455-6677-8899-aabbccddeeff"
	override, err := RegisterReadModel[sinkOverrideModel](registry, readmodels.WithSink(readmodels.Sink{Type: readmodels.MongoDB, ConfigurationID: configID}))
	if err != nil {
		t.Fatal(err)
	}
	replaced, err := RegisterReadModel[sinkDefaultModel](replacement)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []readmodels.SinkType{readmodels.SQL, readmodels.InMemory} {
		client, err := NewClient(WithRegistry(registry), WithRegistryForStore("custom", replacement), WithRegistryForStore("empty", nil), WithDefaultSinkType(kind))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		})
		for _, store := range []StoreName{"default", "custom"} {
			got := sinkCatalogModel(t, client, store, model.Identifier())
			if got.Sink().Type != kind || got.Generation() != model.Descriptor().Generation() || got.Schema() != model.Descriptor().Schema() {
				t.Fatal("default or schema changed", got.Sink())
			}
		}
		if got := sinkCatalogModel(t, client, "default", override.Identifier()).Sink(); got.Type != readmodels.MongoDB || got.ConfigurationID != configID {
			t.Fatal("explicit override lost", got)
		}
		_, custom, _ := client.Catalogs("custom")
		_, empty, _ := client.Catalogs("empty")
		if len(custom.Descriptors()) != 1 || custom.Descriptors()[0].Identifier() != replaced.Identifier() || len(empty.Descriptors()) != 0 || len(client.stores) != 0 {
			t.Fatal("replacement merged or catalog performed I/O")
		}
	}
	if model.Descriptor().Sink().Type != readmodels.MongoDB || model.Descriptor().Indexes()[0] != "Name" {
		t.Fatal("registry mutated")
	}
	client, err := NewClient(WithRegistry(registry), WithDefaultSinkType(readmodels.SQL), WithDefaultSinkType(readmodels.MongoDB))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if sinkCatalogModel(t, client, "default", model.Identifier()).Sink().Type != readmodels.MongoDB {
		t.Fatal("last value did not win")
	}
}

func TestDefaultSinkRejectsUnknownAndNoneBeforePreparation(t *testing.T) {
	for _, kind := range []readmodels.SinkType{"", "future", readmodels.NoSink} {
		r := NewRegistry()
		model, err := RegisterReadModel[sinkDefaultModel](r)
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		if err := RegisterProjectionFactory(r, "p", model.Descriptor(), func() *sinkFactory { calls++; return &sinkFactory{} }, func(context.Context, *sinkFactory) (projections.Declaration, error) {
			calls++
			return projections.Declaration{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		client, err := NewClient(WithRegistry(r), WithDefaultSinkType(kind))
		if client != nil || !errors.Is(err, ErrInvalidConfiguration) || calls != 0 {
			t.Fatalf("kind=%q client=%v err=%v calls=%d", kind, client, err, calls)
		}
	}
}

func TestDefaultSinkPassiveProducerPrecedenceAndExplicitConflicts(t *testing.T) {
	for _, producer := range []string{"projection", "reducer"} {
		for _, explicit := range []readmodels.SinkType{"", readmodels.NoSink, readmodels.MongoDB, readmodels.SQL, readmodels.InMemory} {
			t.Run(producer+"/"+string(explicit), func(t *testing.T) {
				r := NewRegistry()
				ev, err := RegisterEvent[sinkDefaultEvent](r)
				if err != nil {
					t.Fatal(err)
				}
				var options []readmodels.ModelOption
				if explicit != "" {
					observer := readmodels.Projection
					if producer == "reducer" {
						observer = readmodels.Reducer
					}
					options = append(options, readmodels.WithSink(readmodels.Sink{Type: explicit}), readmodels.WithObserver(observer, "p"))
				}
				model, err := RegisterReadModel[sinkDefaultModel](r, options...)
				if err != nil {
					t.Fatal(err)
				}
				if producer == "projection" {
					err = r.AddProjection(projections.ModelBound(model, projections.WithIdentifier("p"), projections.FromEvent(ev), projections.Passive()))
				} else {
					err = RegisterReducerHandlers(r, model, "p", []reducers.Handler{reducers.On(func(_ context.Context, _ sinkDefaultEvent, current *sinkDefaultModel, _ events.Context) (*sinkDefaultModel, error) {
						return current, nil
					})}, reducers.Passive())
				}
				if err != nil {
					t.Fatal(err)
				}
				client, err := NewClient(WithRegistry(r), WithDefaultSinkType(readmodels.SQL))
				if explicit != "" && explicit != readmodels.NoSink {
					if client != nil || !errors.Is(err, ErrInvalidConfiguration) {
						t.Fatalf("explicit materialized passive sink accepted: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = client.Close() }()
				got := sinkCatalogModel(t, client, "store", model.Identifier())
				if got.Sink().Type != readmodels.NoSink {
					t.Fatal("passive inherited default", got.Sink())
				}
				if producer == "projection" && client.projections[0].Model() != client.readModelCatalog.Descriptors()[0] {
					t.Fatal("projection/catalog mismatch")
				}
				if producer == "reducer" && client.reducers.defaults[0].Model() != client.readModelCatalog.Descriptors()[0] {
					t.Fatal("reducer/catalog mismatch")
				}
			})
		}
	}
}

func TestDefaultSinkFactoryProducerPlansUseFinalCatalogWithoutRepreparation(t *testing.T) {
	r := NewRegistry()
	ev, err := RegisterEvent[sinkDefaultEvent](r)
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[sinkDefaultModel](r)
	if err != nil {
		t.Fatal(err)
	}
	foldModel, err := RegisterReadModel[sinkOverrideModel](r)
	if err != nil {
		t.Fatal(err)
	}
	constructed, defined := 0, 0
	if err := RegisterProjectionFactory(r, "p", model.Descriptor(), func() *sinkFactory { constructed++; return &sinkFactory{} }, func(context.Context, *sinkFactory) (projections.Declaration, error) {
		defined++
		return projections.ModelBound(model, projections.WithIdentifier("p"), projections.FromEvent(ev), projections.WithInitialValues(sinkDefaultModel{Name: "snapshot"})), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReducerHandlers(r, foldModel, "r", []reducers.Handler{reducers.On(func(_ context.Context, _ sinkDefaultEvent, current *sinkOverrideModel, _ events.Context) (*sinkOverrideModel, error) {
		return current, nil
	})}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReadModelReactorHandlers(r, "changes", model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(sinkDefaultModel) {})}); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(r), WithRegistryForStore("shared", r), WithDefaultSinkType(readmodels.SQL), WithNamingPolicy(serialization.CamelCase))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if constructed != 1 || defined != 1 {
		t.Fatal("preparation repeated", constructed, defined)
	}
	for _, store := range []StoreName{"default", "shared"} {
		artifacts, err := client.Artifacts(store)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := artifacts.ReadModels.LookupIdentifier(model.Identifier())
		if got.Sink().Type != readmodels.SQL || artifacts.Projections[0].Model() != got {
			t.Fatal("factory model not rebound")
		}
		fold, _ := artifacts.ReadModels.LookupIdentifier(foldModel.Identifier())
		if artifacts.Reducers[0].Model() != fold || fold.Sink().Type != readmodels.SQL || client.readModelReactors.defaults[0].Model().Sink() != got.Sink() {
			t.Fatal("producer final model mismatch")
		}
		if artifacts.Projections[0].KernelDefinition().InitialModelState != `{"id":"","name":"snapshot"}` {
			t.Fatal("initial state not frozen/rebound", artifacts.Projections[0].KernelDefinition().InitialModelState)
		}
	}
	if constructed != 1 || defined != 1 || !reflect.DeepEqual(model.Descriptor().Sink(), readmodels.Sink{Type: readmodels.MongoDB, ConfigurationID: "00000000-0000-0000-0000-000000000000"}) {
		t.Fatal("authoring changed or callback reran")
	}
}
