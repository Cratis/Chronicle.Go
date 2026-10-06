// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/clientoptions"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

func defaultsSubscriptionRegistry(t *testing.T) *chronicle.Registry {
	t.Helper()
	r := chronicle.NewRegistry()
	e, err := chronicle.RegisterEvent[subscriptionAdded](r)
	if err != nil {
		t.Fatal(err)
	}
	m, err := chronicle.RegisterReadModel[subscriptionModel](r)
	if err != nil {
		t.Fatal(err)
	}
	d := projections.ModelBound(m, projections.FromEvent(e), projections.WithInitialValues(subscriptionModel{Name: "default"}))
	if err := r.AddProjection(d); err != nil {
		t.Fatal(err)
	}
	return r
}
func TestReadModelScenarioMaterializedAdmitsProjectionDefaultsBeforeConnection(t *testing.T) {
	s, err := OpenReadModelScenario[subscriptionModel](t.Context(), Config{Registry: defaultsSubscriptionRegistry(t), Engine: Kernel}, ReadModelOptions[subscriptionModel]{Materialized: true})
	if s != nil || !errors.Is(err, ErrKernelUnavailable) {
		t.Fatalf("defaults: %v, %v", s, err)
	}
}
func TestReadModelScenarioMaterializedRefusesOverlayInitial(t *testing.T) {
	s, err := OpenReadModelScenario[subscriptionModel](t.Context(), Config{Registry: defaultsSubscriptionRegistry(t), Engine: Kernel}, ReadModelOptions[subscriptionModel]{Materialized: true, Initial: &subscriptionModel{Name: "overlay"}})
	if s != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("overlay: %v %v", s, err)
	}
}
func assertMaterializedUnsupported[M any](t *testing.T, r *chronicle.Registry, want error) {
	t.Helper()
	s, err := OpenReadModelScenario[M](t.Context(), Config{Registry: r, Engine: Kernel, ConnectionString: "chronicle://127.0.0.1:1"}, ReadModelOptions[M]{Materialized: true})
	if s != nil || !errors.Is(err, want) {
		t.Fatalf("profile: %v %v", s, err)
	}
	connection := &subscriptionConnection{substituteTransport: substituteConnection()}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	})
	client, err := chronicle.NewClient(chronicle.WithRegistry(r), clientoptions.Connection[chronicle.ClientOption](connection), chronicle.WithNoAuthentication())
	if err == nil {
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		})
		var artifacts chronicle.Artifacts
		artifacts, err = client.Artifacts("materialized-profile")
		if err == nil && len(artifacts.Projections) != 0 {
			_, err = materializedProjectionMembership(artifacts.Projections[0], artifacts.Events)
		}
		if err == nil && len(artifacts.Reducers) == 0 {
			t.Fatal("profile admitted")
		}
	}
	if connection.unaryCalls.Load() != 0 || connection.streamCalls.Load() != 0 {
		t.Fatal("profile checks performed RPC")
	}
}
func TestReadModelScenarioMaterializedRefusesUnsupportedProfiles(t *testing.T) {
	for _, kind := range []string{"variant", "custom key", "mixed all", "join removal"} {
		t.Run(kind, func(t *testing.T) {
			assertMaterializedUnsupported[subscriptionModel](t, subscriptionRegistry(t, kind), chronicle.ErrUnsupported)
		})
	}
	for _, kind := range []string{"passive", "no sink", "reducer"} {
		t.Run(kind, func(t *testing.T) {
			r := chronicle.NewRegistry()
			e, err := chronicle.RegisterEvent[subscriptionAdded](r)
			if err != nil {
				t.Fatal(err)
			}
			var opts []readmodels.ModelOption
			if kind == "no sink" {
				opts = append(opts, readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}), readmodels.WithObserver(readmodels.Projection, "materialized"))
			}
			m, err := chronicle.RegisterReadModel[subscriptionModel](r, opts...)
			if err != nil {
				t.Fatal(err)
			}
			want := chronicle.ErrUnsupported
			if kind == "no sink" {
				want = chronicle.ErrInvalidConfiguration
			} // Active NoSink is already rejected by production compilation.
			if kind == "reducer" {
				err = chronicle.RegisterReducerHandlers(r, m, "local", []reducers.Handler{reducers.On(func(_ context.Context, _ subscriptionAdded, _ *subscriptionModel, _ events.Context) (*subscriptionModel, error) {
					return nil, nil
				})})
				want = ErrFidelityUnavailable
			} else {
				options := []projections.Option{projections.FromEvent(e), projections.WithIdentifier("materialized")}
				if kind == "passive" {
					options = append(options, projections.Passive())
				}
				err = r.AddProjection(projections.ModelBound(m, options...))
			}
			if err != nil {
				t.Fatal(err)
			}
			assertMaterializedUnsupported[subscriptionModel](t, r, want)
		})
	}
	for _, kind := range []string{"children", "nested", "join"} {
		t.Run(kind, func(t *testing.T) {
			r := chronicle.NewRegistry()
			e, err := chronicle.RegisterEvent[subscriptionAdded](r)
			if err != nil {
				t.Fatal(err)
			}
			m, err := chronicle.RegisterReadModel[subscriptionComplex](r)
			if err != nil {
				t.Fatal(err)
			}
			b := projections.NewBuilder("complex", m)
			projections.From(b, e, nil)
			switch kind {
			case "children":
				projections.Children(b, projections.Path[subscriptionComplex, []subscriptionChild]("children"), func(child *projections.Builder[subscriptionChild]) { projections.From(child, e, nil) })
			case "nested":
				projections.Nested(b, projections.Path[subscriptionComplex, *subscriptionChild]("nested"), func(child *projections.Builder[subscriptionChild]) { projections.From(child, e, nil) })
			case "join":
				projections.Join(b, e, projections.Path[subscriptionComplex, string]("id"), nil)
			}
			d, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
			if err = r.AddProjection(d); err != nil {
				t.Fatal(err)
			}
			assertMaterializedUnsupported[subscriptionComplex](t, r, chronicle.ErrUnsupported)
		})
	}
	t.Run("protected", func(t *testing.T) {
		r := chronicle.NewRegistry()
		e, err := chronicle.RegisterEvent[subscriptionAdded](r)
		if err != nil {
			t.Fatal(err)
		}
		m, err := chronicle.RegisterReadModel[subscriptionProtected](r)
		if err != nil {
			t.Fatal(err)
		}
		if err = r.AddProjection(projections.ModelBound(m, projections.FromEvent(e))); err != nil {
			t.Fatal(err)
		}
		assertMaterializedUnsupported[subscriptionProtected](t, r, chronicle.ErrUnsupported)
	})
	t.Run("Substitute", func(t *testing.T) {
		s, err := OpenReadModelScenario[subscriptionModel](t.Context(), Config{Registry: defaultsSubscriptionRegistry(t)}, ReadModelOptions[subscriptionModel]{Materialized: true})
		if s != nil || !errors.Is(err, ErrFidelityUnavailable) {
			t.Fatalf("substitute: %v %v", s, err)
		}
	})
}

func TestMaterializedCompletionPreservesSeedGenerations(t *testing.T) {
	f, k := materializedFixture(t)
	d, _ := f.scenario.artifacts.Events.Lookup(subscriptionAdded{})
	f.scenario.materializedSeeds = []materializedSeed{{events.TypeRef{ID: d.Ref().ID, Generation: 2}, 0}, {events.TypeRef{ID: d.Ref().ID, Generation: 1}, 1}}
	k.position = 1
	if err := f.scenario.awaitMaterialization(t.Context()); err != nil {
		t.Fatal(err)
	}
	if k.request == nil || len(k.request.EventTypeTails) != 1 || k.request.EventTypeTails[0].EventType.Generation != 1 || k.request.EventTypeTails[0].SequenceNumber != 1 {
		t.Fatalf("generation: %+v", k.request)
	}
}
