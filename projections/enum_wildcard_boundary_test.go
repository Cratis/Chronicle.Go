// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type enumWildcardEveryFirst struct {
	Status   *projectionEnum `chronicle:"every(from=Status)"`
	Observed *time.Time      `chronicle:"all(context=occurred)"`
}

type enumWildcardAllFirst struct {
	Observed *time.Time      `chronicle:"all(context=occurred)"`
	Status   *projectionEnum `chronicle:"every(from=Status)"`
}

type enumWildcardFluent struct {
	Status   *projectionEnum
	Observed *time.Time
}

type enumWildcardPlain struct{ Status int32 }
type enumWildcardEvent struct{ Status *projectionEnum }

func TestEnumEveryMixedWithAllRefusesBeforeRPC(t *testing.T) {
	for _, frontEnd := range []string{"model-bound", "fluent"} {
		for _, order := range []string{"Every then All", "All then Every"} {
			for _, sparse := range []bool{false, true} {
				handlers := "handlerless"
				if sparse {
					handlers = "sparse From"
				}
				t.Run(frontEnd+"/"+order+"/"+handlers, func(t *testing.T) {
					r := chronicle.NewRegistry()
					if _, err := chronicle.RegisterEvent[enumWildcardPlain](r); err != nil {
						t.Fatal(err)
					}
					declaration, buildErr := enumWildcardDeclaration(t, r, frontEnd, order == "All then Every", sparse)
					plain := mustEvent[enumWildcardPlain](t)
					tick := mustEvent[enumSparseTick](t)
					catalog, err := events.NewCatalog(plain.Descriptor(), tick.Descriptor())
					if err != nil {
						t.Fatal(err)
					}
					if frontEnd == "fluent" {
						enumWildcardFailure(t, buildErr)
						// Build refuses before a fluent declaration can be registered.
						// Exercise the equivalent deferred graph at the RPC boundary.
						r = chronicle.NewRegistry()
						if _, err := chronicle.RegisterEvent[enumWildcardPlain](r); err != nil {
							t.Fatal(err)
						}
						declaration, buildErr = enumWildcardDeclaration(t, r, "model-bound", order == "All then Every", sparse)
					}
					if buildErr != nil {
						t.Fatal(buildErr)
					}
					_, err = projections.Compile(declaration, catalog)
					enumWildcardFailure(t, err)
					if err := r.AddProjection(declaration); err != nil {
						t.Fatal(err)
					}
					enumRegistrationNoRPC(t, r, "Status", serialization.PreservePropertyNames)
				})
			}
		}
	}
}

func enumWildcardDeclaration(t *testing.T, r *chronicle.Registry, frontEnd string, allFirst, sparse bool) (projections.Declaration, error) {
	t.Helper()
	codecs := projectionEnumCodecs(t, false)
	tick, err := chronicle.RegisterEvent[enumSparseTick](r)
	if err != nil {
		t.Fatal(err)
	}
	options := []projections.Option{projections.NoAutoMap()}
	if frontEnd == "model-bound" {
		if sparse {
			options = append(options, projections.FromEvent(tick))
		}
		if allFirst {
			model, err := chronicle.RegisterReadModel[enumWildcardAllFirst](r, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			return projections.ModelBound(model, options...), nil
		}
		model, err := chronicle.RegisterReadModel[enumWildcardEveryFirst](r, readmodels.WithCodecs(codecs))
		if err != nil {
			t.Fatal(err)
		}
		return projections.ModelBound(model, options...), nil
	}
	model, err := chronicle.RegisterReadModel[enumWildcardFluent](r, readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("enum-wildcard", model, options...)
	if sparse {
		projections.From(b, tick, nil)
	}
	every := func() {
		projections.Every(b, func(e *projections.EveryBuilder[enumWildcardFluent]) {
			projections.EveryMap(e, projections.Path[enumWildcardFluent, *projectionEnum]("Status"), "Status")
		})
	}
	all := func() {
		projections.All(b, func(e *projections.EveryBuilder[enumWildcardFluent]) {
			projections.EveryContext(e, projections.Path[enumWildcardFluent, *time.Time]("Observed"), "occurred")
		})
	}
	if allFirst {
		all()
		every()
	} else {
		every()
		all()
	}
	return b.Build()
}

func enumWildcardFailure(t *testing.T, err error) {
	t.Helper()
	enumBoundaryFailure(t, err, "Status")
	var located *projections.DeclarationError
	if !errors.As(err, &located) || located.GoField != "Status" || located.Directive != "every" {
		t.Fatalf("want root enum member and Every location, got %v", err)
	}
}

func TestEnumWildcardGuardPreservesOrdinaryAllAndExplicitEvery(t *testing.T) {
	codecs := projectionEnumCodecs(t, false)
	event := mustEvent[enumWildcardEvent](t, events.WithCodecs(codecs))
	tick := mustEvent[enumSparseTick](t)
	catalog, err := events.NewCatalog(event.Descriptor(), tick.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	model := mustModel[enumWildcardFluent](t, readmodels.WithCodecs(codecs))
	for _, mode := range []string{"ordinary All", "matching Every", "sparse Every"} {
		t.Run(mode, func(t *testing.T) {
			b := projections.NewBuilder("enum-wildcard-control", model, projections.NoAutoMap())
			if mode == "ordinary All" {
				projections.All(b, func(e *projections.EveryBuilder[enumWildcardFluent]) {
					projections.EveryContext(e, projections.Path[enumWildcardFluent, *time.Time]("Observed"), "occurred")
				})
			} else {
				if mode == "matching Every" {
					projections.From(b, event, nil)
				} else {
					projections.From(b, tick, nil)
				}
				projections.Every(b, func(e *projections.EveryBuilder[enumWildcardFluent]) {
					projections.EveryMap(e, projections.Path[enumWildcardFluent, *projectionEnum]("Status"), "Status")
				})
			}
			declaration, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
			definition, err := projections.Compile(declaration, catalog)
			if err != nil {
				t.Fatal(err)
			}
			wire := definition.KernelDefinition()
			if wire.SubscribesToAllEvents != (mode == "ordinary All") {
				t.Fatal("subscription population changed")
			}
			path, expression, froms := "Status", "Status", 1
			if mode == "ordinary All" {
				path, expression, froms = "Observed", "$eventContext(occurred)", 0
			}
			if len(wire.All.Properties) != 1 || wire.All.Properties[path] != expression || len(wire.From) != froms || len(wire.Join) != 0 {
				t.Fatal("admitted mapping or explicit subscription changed")
			}
		})
	}
}
