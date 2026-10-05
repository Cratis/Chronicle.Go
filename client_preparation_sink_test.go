// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

func TestCaptureSinkDefaultsFreezeOptionsRegistriesCodecsAndFactoryModels(t *testing.T) {
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	classified := 0
	classifier := compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		classified++
		return compliance.Classification{}, nil
	})
	r, replacement := NewRegistry(), NewRegistry()
	ev, err := RegisterEvent[sinkDefaultEvent](r)
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[factoryCodecModel](r, readmodels.WithCodecs(codecs), readmodels.WithProtection(classifier))
	if err != nil {
		t.Fatal(err)
	}
	const configuration = "00112233-4455-6677-8899-aabbccddeeff"
	override, err := RegisterReadModel[sinkOverrideModel](r, readmodels.WithSink(readmodels.Sink{Type: readmodels.MongoDB, ConfigurationID: configuration}))
	if err != nil {
		t.Fatal(err)
	}
	passive, err := RegisterReadModel[sinkDefaultModel](r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(passive, projections.WithIdentifier("passive"), projections.FromEvent(ev), projections.Passive())); err != nil {
		t.Fatal(err)
	}
	replaced, err := RegisterReadModel[sinkDefaultModel](replacement)
	if err != nil {
		t.Fatal(err)
	}
	member := &derivedfixtures.HumanValue{Name: "frozen", Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}
	defined, opened, resolved, closed, optionCalls := 0, 0, 0, 0, 0
	services := preparationFactory{
		contains: func(typ reflect.Type) bool { return typ == reflect.TypeFor[*sinkFactory]() },
		open: func(context.Context) (reactors.Scope, error) {
			opened++
			return &preparationScope{
				resolve: func(context.Context, reflect.Type) (any, error) { resolved++; return &sinkFactory{}, nil },
				close:   func(context.Context) error { closed++; return nil },
			}, nil
		},
	}
	if err := RegisterProjectionFactory(r, "factory", model.Descriptor(), nil, func(context.Context, *sinkFactory) (projections.Declaration, error) {
		defined++
		return projections.ModelBound(model, projections.WithIdentifier("factory"), projections.FromEvent(ev), projections.WithInitialValues(factoryCodecModel{DisplayName: "initial", Member: member})), nil
	}); err != nil {
		t.Fatal(err)
	}
	declarationClassifications := classified
	var optionConfig *clientConfig
	options := []ClientOption{WithRegistry(r), WithRegistryForStore("shared", r), WithRegistryForStore("replacement", replacement), WithRegistryForStore("empty", nil), WithServices(services), WithNamingPolicy(serialization.CamelCase), func(c *clientConfig) { optionCalls++; optionConfig = c }}
	sql := captureForTest(t, append(options, WithDefaultSinkType(readmodels.SQL))...)
	memory := captureForTest(t, append(options, WithDefaultSinkType(readmodels.InMemory))...)
	if defined != 0 || opened != 0 || resolved != 0 || closed != 0 || classified != declarationClassifications || optionCalls != 2 {
		t.Fatal("capture invoked preparation callbacks")
	}
	// Mutating the option's original configuration, codec handle and selected
	// registries cannot rewrite either already captured client epoch.
	WithDefaultSinkType(readmodels.MongoDB)(optionConfig)
	WithNamingPolicy(serialization.PreservePropertyNames)(optionConfig)
	WithServices(preparationFactory{open: func(context.Context) (reactors.Scope, error) {
		t.Error("late services used")
		return nil, errors.New("late services")
	}})(optionConfig)
	*codecs = serialization.Codecs{}
	if _, err := RegisterReadModel[catalogModel](r); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterReadModel[sinkOverrideModel](replacement); err != nil {
		t.Fatal(err)
	}
	for i, p := range []*ClientPreparation{sql, memory} {
		kind := []readmodels.SinkType{readmodels.SQL, readmodels.InMemory}[i]
		assertPreparationGuards(t, p.Client(), ErrNotPrepared)
		client, err := p.Prepare(t.Context(), nil)
		if err != nil || client != p.Client() {
			t.Fatal("preparation identity", client, err)
		}
		for _, store := range []StoreName{"default", "shared"} {
			view := client.registryOutput.compose()
			preparedModel, _ := view.models.LookupIdentifier(model.Identifier())
			if preparedModel.Sink().Type != kind || client.registryOutput != client.storeRegistryOutputs["shared"] {
				t.Fatal("output composition lost the captured default")
			}
			artifacts, err := client.Artifacts(store)
			if err != nil {
				t.Fatal(err)
			}
			if len(artifacts.ReadModels.Descriptors()) != 3 {
				t.Fatal("late declaration admitted")
			}
			final, _ := artifacts.ReadModels.LookupIdentifier(model.Identifier())
			if final.Sink().Type != kind || final.Generation() != model.Descriptor().Generation() {
				t.Fatal("captured default or generation lost", final.Sink())
			}
			if got := sinkCatalogModel(t, client, store, override.Identifier()).Sink(); got.Type != readmodels.MongoDB || got.ConfigurationID != configuration {
				t.Fatal("explicit MongoDB override lost", got)
			}
			if sinkCatalogModel(t, client, store, passive.Identifier()).Sink().Type != readmodels.NoSink {
				t.Fatal("passive producer inherited materialized default")
			}
			for _, projection := range artifacts.Projections {
				bound, _ := artifacts.ReadModels.LookupIdentifier(projection.Model().Identifier())
				if projection.Model() != bound {
					t.Fatal("producer and final catalog disagree")
				}
				if projection.Identifier() != "factory" {
					continue
				}
				state := []byte(projection.KernelDefinition().InitialModelState)
				decoded, err := bound.Unmarshal(state)
				if err != nil || !reflect.DeepEqual(decoded.(*factoryCodecModel).Member, &derivedfixtures.HumanValue{Name: "frozen", Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}) {
					t.Fatal("frozen codec initial state lost", err)
				}
				if decoded.(*factoryCodecModel).DisplayName != "initial" || bound.Schema() == model.Descriptor().Schema() {
					t.Fatal("naming or initial state lost")
				}
			}
		}
		_, custom, err := client.Catalogs("replacement")
		if err != nil || len(custom.Descriptors()) != 1 || custom.Descriptors()[0].Identifier() != replaced.Identifier() || custom.Descriptors()[0].Sink().Type != kind {
			t.Fatal("replacement snapshot/default lost", err)
		}
		_, empty, err := client.Catalogs("empty")
		if err != nil || len(empty.Descriptors()) != 0 {
			t.Fatal("nil replacement merged", err)
		}
		repeated, err := p.Prepare(t.Context(), nil)
		if err != nil || repeated != client || client.config.defaultSinkType != kind {
			t.Fatal("retained preparation changed config", err)
		}
	}
	member.Name = "mutated"
	for _, p := range []*ClientPreparation{sql, memory} {
		artifacts, err := p.Client().Artifacts("shared")
		if err != nil {
			t.Fatal(err)
		}
		for _, projection := range artifacts.Projections {
			if projection.Identifier() == "factory" {
				decoded, err := projection.Model().Unmarshal([]byte(projection.KernelDefinition().InitialModelState))
				if err != nil || decoded.(*factoryCodecModel).Member.(*derivedfixtures.HumanValue).Name != "frozen" {
					t.Fatal("initial state mutated after preparation", err)
				}
			}
		}
	}
	if defined != 2 || opened != 2 || resolved != 2 || closed != 2 || optionCalls != 2 || classified != declarationClassifications {
		t.Fatal("shared registries, services, options or classifications reprepared", defined, opened, resolved, closed, optionCalls, classified)
	}
	if model.Descriptor().Sink().Type != readmodels.MongoDB || passive.Descriptor().Sink().Type != readmodels.MongoDB {
		t.Fatal("client mutated original declarations")
	}
}

func TestCaptureInvalidSinkRunsNoSchemaFactoryOrServiceCallbacks(t *testing.T) {
	for _, kind := range []readmodels.SinkType{"", "unknown", readmodels.NoSink} {
		r := NewRegistry()
		calls := 0
		model, err := RegisterReadModel[sinkDefaultModel](r, readmodels.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
			calls++
			return compliance.Classification{}, nil
		})))
		if err != nil {
			t.Fatal(err)
		}
		if err := RegisterProjectionFactory(r, "factory", model.Descriptor(), func() *sinkFactory { calls++; return &sinkFactory{} }, func(context.Context, *sinkFactory) (projections.Declaration, error) {
			calls++
			return projections.Declaration{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		services := preparationFactory{
			contains: func(reflect.Type) bool { calls++; return true },
			open:     func(context.Context) (reactors.Scope, error) { calls++; return nil, errors.New("unexpected scope") },
		}
		calls = 0
		p, err := CaptureClient(WithRegistry(r), WithServices(services), WithDefaultSinkType(kind))
		if p != nil || !errors.Is(err, ErrInvalidConfiguration) || calls != 0 {
			t.Fatalf("kind=%q preparation=%v error=%v callbacks=%d", kind, p, err, calls)
		}
	}
}
