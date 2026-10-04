// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type enumDottedJoinModel struct {
	ID     string         `json:"ID" chronicle:"key"`
	Status projectionEnum `json:"State.Value"`
	Ready  bool           `chronicle:"join(enumDottedEvent,on=ID)"`
}

func TestEnumDottedRootJoinRegistrationRefusesBeforeRPC(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		t.Run(policyName(policy), func(t *testing.T) {
			codecs := projectionEnumCodecs(t, false)
			r := chronicle.NewRegistry()
			if _, err := chronicle.RegisterEvent[enumDottedEvent](r, events.WithCodecs(codecs)); err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[enumDottedJoinModel](r, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			if err := r.AddProjection(projections.ModelBound(model)); err != nil {
				t.Fatal(err)
			}
			enumRegistrationNoRPC(t, r, "State.Value", policy)
		})
	}
}

func policyName(policy serialization.NamingPolicy) string {
	switch policy {
	case serialization.PreservePropertyNames:
		return "preserve"
	case serialization.CamelCase:
		return "camel"
	default:
		return "legacy"
	}
}

func TestEnumDirectDottedExplicitMappingsRefuse(t *testing.T) {
	codecs := projectionEnumCodecs(t, false)
	event := mustEvent[enumDottedEvent](t, events.WithCodecs(codecs))
	model := mustModel[enumDottedModel](t, readmodels.WithCodecs(codecs))
	for _, handler := range []string{"From", "Join", "Every", "Literal"} {
		t.Run(handler, func(t *testing.T) {
			b := projections.NewBuilder("enum-dotted-explicit", model, projections.NoAutoMap())
			define := func(f *projections.FromBuilder[enumDottedModel, enumDottedEvent]) {
				if handler == "Literal" {
					projections.Value(f, projections.Path[enumDottedModel, projectionEnum]("State.Value"), projectionEnum(1))
					return
				}
				projections.Map(f, projections.Path[enumDottedModel, projectionEnum]("State.Value"), projections.Path[enumDottedEvent, projectionEnum]("State.Value"))
			}
			switch handler {
			case "Join":
				projections.Join(b, event, projections.Path[enumDottedModel, string]("ID"), define)
			case "Every":
				projections.From(b, event, nil)
				projections.Every(b, func(e *projections.EveryBuilder[enumDottedModel]) {
					projections.EveryMap(e, projections.Path[enumDottedModel, projectionEnum]("State.Value"), "State.Value")
				})
			default:
				projections.From(b, event, define)
			}
			_, err := b.Build()
			enumBoundaryFailure(t, err, "State.Value")
		})
	}
	// An explicit mapping to a different, safe target cannot read a dotted raw key.
	b := projections.NewBuilder("enum-dotted-source", mustModel[enumCaseModel](t, readmodels.WithCodecs(codecs)), projections.NoAutoMap())
	projections.From(b, event, func(f *projections.FromBuilder[enumCaseModel, enumDottedEvent]) {
		projections.Map(f, projections.Path[enumCaseModel, projectionEnum]("Status"), projections.Path[enumDottedEvent, projectionEnum]("State.Value"))
	})
	_, err := b.Build()
	enumBoundaryFailure(t, err, "Status")
	clear := projections.NewBuilder("enum-dotted-clear", mustModel[enumDottedNullable](t, readmodels.WithCodecs(codecs)), projections.NoAutoMap())
	projections.From(clear, event, func(f *projections.FromBuilder[enumDottedNullable, enumDottedEvent]) {
		projections.Clear(f, projections.Path[enumDottedNullable, *projectionEnum]("State.Value"))
	})
	_, err = clear.Build()
	enumBoundaryFailure(t, err, "State.Value")
}

type enumOrdinaryNested struct{ Value int32 }
type enumNestedControlEvent struct {
	State  *enumOrdinaryNested
	Status projectionEnum
}
type enumNestedControlModel struct {
	ID     string
	State  *enumOrdinaryNested
	Status projectionEnum
}

func TestEnumRootGuardPreservesGenuineNestedOrdinaryMappings(t *testing.T) {
	codecs := projectionEnumCodecs(t, false)
	event := mustEvent[enumNestedControlEvent](t, events.WithCodecs(codecs))
	b := projections.NewBuilder("enum-nested-control", mustModel[enumNestedControlModel](t, readmodels.WithCodecs(codecs)))
	projections.From(b, event, func(f *projections.FromBuilder[enumNestedControlModel, enumNestedControlEvent]) {
		projections.Map(f, projections.Path[enumNestedControlModel, int32]("State.Value"), projections.Path[enumNestedControlEvent, int32]("State.Value"))
	})
	declaration, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	wire := mustCompile(t, declaration, event.Descriptor()).KernelDefinition()
	if wire.From[0].Value.Properties["State.Value"] != "State.Value" {
		t.Fatal("ordinary nested mapping changed")
	}
}

func TestEnumSparseEverySafeNamingDoesNotSubscribeUnusedCatalog(t *testing.T) {
	codecs := projectionEnumCodecs(t, false)
	tick := mustEvent[enumSparseTick](t)
	unused := mustEvent[enumSparseUnused](t, events.WithCodecs(codecs))
	model := mustModel[enumSparseModel](t, readmodels.WithCodecs(codecs))
	catalog, err := events.NewCatalog(tick.Descriptor(), unused.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("enum-sparse-safe", model, projections.NoAutoMap())
	projections.From(b, tick, nil)
	projections.Every(b, func(e *projections.EveryBuilder[enumSparseModel]) {
		projections.EveryMap(e, projections.Path[enumSparseModel, *projectionEnum]("Status"), "TRUE")
	})
	declaration, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	definition, err := projections.Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		t.Run(policyName(policy), func(t *testing.T) {
			descriptors := catalog.Descriptors()
			for i, descriptor := range descriptors {
				descriptors[i], err = descriptor.WithNamingPolicy(policy)
				if err != nil {
					t.Fatal(err)
				}
			}
			next, err := events.NewCatalog(descriptors...)
			if err != nil {
				t.Fatal(err)
			}
			bound, err := model.Descriptor().WithNamingPolicy(policy)
			if err != nil {
				t.Fatal(err)
			}
			rebound, err := definition.Rebind(bound, catalog, next)
			if err != nil {
				t.Fatal(err)
			}
			wire := rebound.KernelDefinition()
			if len(wire.From) != 1 || len(wire.Join) != 0 || wire.SubscribesToAllEvents {
				t.Fatal("Every broadened subscription")
			}
		})
	}
}
