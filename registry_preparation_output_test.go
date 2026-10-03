// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

type outputCallbacks struct {
	classification, catalog, open, construct, cleanup, scopeClose int
	projection, constraint, migration, composition, seed          int
}

type preparationOutputFixture struct {
	registry *Registry
	current  events.Type[factoryCodecCurrent]
	previous events.Type[factoryCodecPrevious]
	model    readmodels.Model[factoryCodecModel]
	calls    *outputCallbacks
	services preparationFactory
}

func newPreparationOutputFixture(t *testing.T) preparationOutputFixture {
	t.Helper()
	f := preparationOutputFixture{registry: NewRegistry(), calls: &outputCallbacks{}}
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	classifier := compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		f.calls.classification++
		return compliance.Classification{}, nil
	})
	f.current, err = RegisterEvent[factoryCodecCurrent](f.registry, events.WithCodecs(codecs), events.WithGeneration(2), events.WithProtection(classifier))
	if err != nil {
		t.Fatal(err)
	}
	f.previous, err = RegisterEventGeneration[factoryCodecPrevious](f.registry, f.current, 1, events.WithCodecs(codecs), events.WithProtection(classifier))
	if err != nil {
		t.Fatal(err)
	}
	f.model, err = RegisterReadModel[factoryCodecModel](f.registry, readmodels.WithCodecs(codecs), readmodels.WithProtection(classifier))
	if err != nil {
		t.Fatal(err)
	}
	// The family graphs belong to the descriptors, not this mutable caller handle.
	*codecs = serialization.Codecs{}
	factory := func() *preparationArtifact {
		f.calls.construct++
		return &preparationArtifact{close: func(context.Context) error { f.calls.cleanup++; return nil }}
	}
	if err := RegisterProjectionFactory(f.registry, "prepared", f.model.Descriptor(), factory, func(context.Context, *preparationArtifact) (projections.Declaration, error) {
		f.calls.projection++
		return projections.ModelBound(f.model, projections.WithIdentifier("prepared"), projections.FromEvent(f.current), projections.WithLabels("retained"), projections.WithInitialValues(factoryCodecModel{DisplayName: "initial", Member: &derivedfixtures.HumanValue{Name: "frozen", Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}})), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterConstraintFactory(f.registry, []string{"name"}, factory, func(context.Context, *preparationArtifact) ([]constraints.Definition, error) {
		f.calls.constraint++
		definition, err := constraints.UniqueValues("name").On(f.current.Descriptor(), "DisplayName").WithMessage("retained").Build()
		return []constraints.Definition{definition}, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterEventMigrationFactory(f.registry, f.current.Descriptor(), f.previous.Descriptor(), factory, func(context.Context, *preparationArtifact) (events.MigrationDeclaration, error) {
		f.calls.migration++
		return events.DefineMigration(f.current, f.previous, events.Migration[factoryCodecCurrent, factoryCodecPrevious]{
			Upcast: func(b *events.MigrationBuilder[factoryCodecCurrent, factoryCodecPrevious]) {
				b.RenamedFrom("DisplayName", "Name")
			},
			Downcast: func(b *events.MigrationBuilder[factoryCodecPrevious, factoryCodecCurrent]) {
				b.RenamedFrom("Name", "DisplayName")
			},
		})
	}); err != nil {
		t.Fatal(err)
	}
	declareEvent[DeclaredEmail](t, f.registry)
	if err := f.registry.ConfigureDeclaredConstraint("email", func(b *constraints.Builder) { f.calls.composition++; b.IgnoreCasing() }); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSeederFunc(f.registry, func(b *seeding.Builder) error {
		f.calls.seed++
		seeding.For(b.ForNamespace("tenant"), "source", factoryCodecCurrent{DisplayName: "seed", Member: derivedfixtures.RobotValue{Count: 42}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactorHandler(f.registry, "reactor", func(context.Context, factoryCodecCurrent) error {
		t.Error("runtime handler ran during preparation")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fold, err := RegisterReadModel[factoryVariantOne](f.registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterReducerHandlers(f.registry, fold, "reducer", []reducers.Handler{reducers.On(func(_ context.Context, _ factoryCodecCurrent, current *factoryVariantOne, _ events.Context) (*factoryVariantOne, error) {
		t.Error("runtime fold ran during preparation")
		return current, nil
	})}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReadModelReactorHandlers(f.registry, "changes", f.model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(factoryCodecModel) { t.Error("runtime model handler ran during preparation") })}); err != nil {
		t.Fatal(err)
	}
	f.services = preparationFactory{
		contains: func(reflect.Type) bool { f.calls.catalog++; return false },
		open: func(context.Context) (reactors.Scope, error) {
			f.calls.open++
			return &preparationScope{close: func(context.Context) error { f.calls.scopeClose++; return nil }}, nil
		},
	}
	return f
}

func TestPreparedOutputCompositionNeverRepeatsApplicationPreparation(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		t.Run(map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "preserved", serialization.CamelCase: "camel"}[policy], func(t *testing.T) {
			f := newPreparationOutputFixture(t)
			classified := f.calls.classification
			options := []ClientOption{WithRegistry(f.registry), WithRegistryForStore("same", f.registry), WithRegistryForStore("empty", nil), WithServices(f.services), WithNamingPolicy(policy), WithEventTypeGenerationValidation(true)}
			p := captureForTest(t, options...)
			logger := p.Client().config.logger
			if logger != slog.Default() {
				t.Fatal("capture lost default logger")
			}
			client, err := p.Prepare(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			output := client.registryOutput
			if output == nil || output != client.storeRegistryOutputs["same"] || output == client.storeRegistryOutputs["empty"] {
				t.Fatal("publication did not retain distinct prepared outputs")
			}
			before := *f.calls
			if before.classification != classified || before.catalog == 0 || before.open != 3 || before.construct != 3 || before.cleanup != 3 || before.scopeClose != 3 || before.projection != 1 || before.constraint != 1 || before.migration != 1 || before.composition != 1 || before.seed != 1 {
				t.Fatalf("preparation callback counts = %+v, declaration classifications = %d", before, classified)
			}
			original, ok := output.authoring.events.LookupRef(f.previous.Ref())
			model, modelOK := output.authoring.models.LookupIdentifier(f.model.Identifier())
			if !ok || !original.SameDeclaration(f.previous.Descriptor()) || !modelOK || model != f.model.Descriptor() || output.authoring.projections[0].Model().Schema() != f.model.Descriptor().Schema() {
				t.Fatal("original authoring identities were discarded")
			}
			for range 3 {
				view := output.compose()
				if view.events != client.catalog || view.models != client.readModelCatalog || view.reactors[0] != client.reactors.defaults[0] || view.reducers[0] != client.reducers.defaults[0] || view.readModelReactors[0] != client.readModelReactors.defaults[0] {
					t.Fatal("composition rebuilt a frozen catalog or observer plan")
				}
				if view.projections[0] != client.projections[0] || view.projections[0].Model().Sink().Type != readmodels.MongoDB || client.config.logger != logger {
					t.Fatal("composition changed compiled definition, default sink or logger")
				}
				// The naming rebind uses the retained finalized graph, not declarations.
				bound, err := output.authoring.projections[0].Rebind(view.projections[0].Model(), output.authoring.events, view.events)
				if err != nil || !proto.Equal(bound.KernelDefinition(), view.projections[0].KernelDefinition()) {
					t.Fatal("finalized authoring could not reproduce the accepted naming binding", err)
				}
				service, err := readmodels.New("store", "tenant", view.models, &clientTransport{})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if _, err := readmodels.For(service, f.model).Get(ctx, "key"); !errors.Is(err, context.Canceled) {
					t.Fatal("original model handle not admitted", err)
				}
				previous, ok := view.events.LookupRef(f.previous.Ref())
				if !ok || !previous.IsHistorical() || previous.Ref() != f.previous.Ref() {
					t.Fatal("historical generation changed")
				}
				value := factoryCodecPrevious{Name: "old", Member: &derivedfixtures.HumanValue{Name: "member"}}
				data, err := previous.Marshal(value)
				var decoded factoryCodecPrevious
				if err != nil || previous.Unmarshal(data, &decoded) != nil || !reflect.DeepEqual(value, decoded) {
					t.Fatal("historical family codec lost", err)
				}
				if len(view.events.Migrations()) != 1 || view.seeds.IsEmpty() {
					t.Fatal("migration or serialized seeds lost")
				}
			}
			if *f.calls != before {
				t.Fatalf("compose/rebind invoked application code: before=%+v after=%+v", before, *f.calls)
			}
			// Callback reuse is per preparation, not a global cache keyed by Registry.
			next := captureForTest(t, options...)
			if _, err := next.Prepare(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			if next.Client().registryOutput == output || f.calls.projection != 2 || f.calls.constraint != 2 || f.calls.migration != 2 || f.calls.composition != 2 || f.calls.seed != 2 || f.calls.construct != 6 || f.calls.cleanup != 6 || f.calls.classification != classified {
				t.Fatal("independent client reused another preparation's outputs", *f.calls)
			}
		})
	}
}

func TestPreparedOutputViewsDetachCollectionsWireMapsAndBytes(t *testing.T) {
	f := newPreparationOutputFixture(t)
	p := captureForTest(t, WithRegistry(f.registry), WithServices(f.services), WithNamingPolicy(serialization.CamelCase))
	client, err := p.Prepare(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	output := client.registryOutput
	view := output.compose()
	projection := view.projections[0]
	wire := projection.KernelDefinition()
	wantWire := proto.Clone(wire)
	wire.Tags[0] = "changed"
	wire.InitialModelState = "{}"
	wire.From[0].Value.Properties["displayName"] = "changed"
	view.constraints[0].Fields()[0].Properties[0] = "changed"
	seed := view.seeds.Contract("store")
	wantSeed := proto.Clone(seed)
	seed.NamespacedEntries[0].ByEventSource[0].Entries[0].Content = "{}"
	seed.NamespacedEntries[0].ByEventType[0].Entries[0].EventTypeId = "changed"
	view.events.Migrations()[0].UpcastJSON = "{}"
	view.events.Descriptors()[0] = events.Descriptor{}
	view.models.Descriptors()[0] = readmodels.Descriptor{}
	encoded, err := projection.Model().Marshal(factoryCodecModel{DisplayName: "initial", Member: &derivedfixtures.HumanValue{Name: "frozen", Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range encoded {
		encoded[i] = 0
	}
	view.projections[0] = projections.Definition{}
	view.constraints[0] = constraints.Definition{}
	view.reactors[0], view.reducers[0], view.readModelReactors[0] = nil, nil, nil
	next := output.compose()
	if !proto.Equal(next.projections[0].KernelDefinition(), wantWire) || !proto.Equal(next.seeds.Contract("store"), wantSeed) || next.constraints[0].Fields()[0].Properties[0] != "displayName" || next.events.Migrations()[0].UpcastJSON == "{}" {
		t.Fatal("returned maps, bytes or collections corrupted prepared definitions")
	}
	if next.projections[0] != client.projections[0] || next.reactors[0] != client.reactors.defaults[0] || next.reducers[0] != client.reducers.defaults[0] || next.readModelReactors[0] != client.readModelReactors.defaults[0] {
		t.Fatal("mutating a view corrupted retained publication plans")
	}
	value, err := next.projections[0].Model().Unmarshal([]byte(next.projections[0].KernelDefinition().InitialModelState))
	if err != nil || value.(*factoryCodecModel).Member.(*derivedfixtures.HumanValue).Name != "frozen" {
		t.Fatal("initial family JSON changed", err)
	}
}
