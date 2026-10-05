//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/chronicle.go/services"
	di "github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type FactoryMemberSelected struct{ Member derivedfixtures.Member }
type FactoryMemberView struct {
	ID     string `chronicle:"key"`
	Title  string
	Member derivedfixtures.Member
}

func TestKernelDefinitionFactoriesWithUnprotectedDerivedCodecs(t *testing.T) {
	for _, mode := range []string{"plain", "provider-scoped", "provider-singleton"} {
		t.Run(mode, func(t *testing.T) {
			f := newKernelFixture(t)
			codecs, err := derivedfixtures.Codecs()
			if err != nil {
				t.Fatal(err)
			}
			registry := chronicle.NewRegistry()
			event, err := chronicle.RegisterEvent[FactoryMemberSelected](registry, events.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[FactoryMemberView](registry, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			constructed, defined, closed := 0, 0, 0
			factory := func() *kernelDefinitionArtifact {
				constructed++
				return &kernelDefinitionArtifact{label: "configured", closed: &closed}
			}
			if err := chronicle.RegisterProjectionFactory(registry, "factory-members", model.Descriptor(), factory, func(_ context.Context, artifact *kernelDefinitionArtifact) (projections.Declaration, error) {
				defined++
				*codecs = serialization.Codecs{}
				return projections.ModelBound(model, projections.WithIdentifier("factory-members"), projections.FromEvent(event), projections.WithLabels(artifact.label), projections.WithInitialValues(FactoryMemberView{Title: artifact.label, Member: derivedfixtures.RobotValue{Count: 7}})), nil
			}); err != nil {
				t.Fatal(err)
			}
			options := []chronicle.ClientOption{chronicle.WithNamingPolicy(serialization.CamelCase)}
			var provider *kernelDefinitionProvider
			wantClosed := 1
			if mode != "plain" {
				lifetime := di.Scoped
				if mode == "provider-singleton" {
					lifetime, wantClosed = di.Singleton, 0
				}
				var bindings container.Registry
				if err := di.Bind(&bindings, lifetime, func(context.Context, di.Resolver) (*kernelDefinitionArtifact, error) { return factory(), nil }); err != nil {
					t.Fatal(err)
				}
				built, err := bindings.Build()
				if err != nil {
					t.Fatal(err)
				}
				provider = &kernelDefinitionProvider{Provider: built}
				t.Cleanup(func() {
					if provider.disposed == 0 {
						if err := provider.Close(context.Background()); err != nil {
							t.Error(err)
						}
					}
				})
				options = append(options, services.WithServices(provider))
			}
			client := f.client(registry, options...)
			store, err := client.EventStore(f.ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			artifacts, err := client.Artifacts(f.storeName)
			if err != nil || len(artifacts.Projections) != 1 {
				t.Fatal("missing frozen factory projection", err)
			}
			projection := artifacts.Projections[0]
			initial, err := projection.Model().Unmarshal([]byte(projection.KernelDefinition().InitialModelState))
			if err != nil || initial.(*FactoryMemberView).Member != (derivedfixtures.RobotValue{Count: 7}) {
				t.Fatal("initial state lost its codec on naming/store rebind", err)
			}
			selected := derivedfixtures.Sample().Primary
			appendSuccessfully(t, f.ctx, store, "owner", FactoryMemberSelected{Member: selected})
			instance := awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), model), "owner", func(value FactoryMemberView) bool {
				return reflect.DeepEqual(value.Member, selected) && value.Title == "configured"
			})
			if instance.Value.ID != "owner" {
				t.Fatal("factory projection lost its key")
			}
			history, err := store.EventLog().ReadSource(f.ctx, "owner", eventsequences.SourceFilter{})
			if err != nil || len(history) != 1 {
				t.Fatal("factory event history missing", err)
			}
			decoded, err := events.Decode[FactoryMemberSelected](artifacts.Events, history[0])
			if err != nil || !reflect.DeepEqual(decoded.Member, selected) {
				t.Fatal("frozen event codec changed", err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if constructed != 1 || defined != 1 || closed != wantClosed {
				t.Fatalf("constructed=%d defined=%d closed=%d", constructed, defined, closed)
			}
			if provider != nil {
				if provider.opened != 1 || provider.closed != 1 || provider.disposed != 0 {
					t.Fatal("borrowed provider/scoped ownership changed")
				}
				if err := provider.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				if closed != 1 || provider.disposed != 1 {
					t.Fatal("singleton not disposed exactly once by its owner")
				}
			}
		})
	}
}
