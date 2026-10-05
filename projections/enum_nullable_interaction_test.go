// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type enumClearCollections struct {
	State      *projectionEnum    `chronicle:"value(AuditCleared,value=null)"`
	Labels     *[]string          `chronicle:"clear(AuditCleared)"`
	Attributes *map[string]string `chronicle:"clear(AuditCleared)"`
}
type enumNullCollections struct {
	State      *projectionEnum    `chronicle:"value(AuditCleared,value=1)"`
	Labels     *[]string          `chronicle:"value(AuditCleared,value=null)"`
	Attributes *map[string]string `chronicle:"value(AuditCleared,value=null)"`
}
type enumFluentCollections struct {
	State      *projectionEnum
	Labels     *[]string
	Attributes *map[string]string
}

func TestEnumSiblingPreservesAllFourNullableCollectionForms(t *testing.T) {
	codecs := projectionEnumCodecs(t, false)
	event := mustEvent[AuditCleared](t)
	bound := []projections.Declaration{
		projections.ModelBound(mustModel[enumClearCollections](t, readmodels.WithCodecs(codecs))),
		projections.ModelBound(mustModel[enumNullCollections](t, readmodels.WithCodecs(codecs))),
	}
	for i, declaration := range bound {
		definition := mustCompile(t, declaration, event.Descriptor())
		assertCollectionNullMappings(t, definition, "Labels", "Attributes")
		want := "$null"
		if i == 1 {
			want = "$value(1)"
		}
		if definition.KernelDefinition().From[0].Value.Properties["State"] != want {
			t.Fatal("enum literal changed")
		}
	}
	for _, literal := range []bool{false, true} {
		builder := projections.NewBuilder("enum-null", mustModel[enumFluentCollections](t, readmodels.WithCodecs(codecs)), projections.NoAutoMap())
		projections.From(builder, event, func(f *projections.FromBuilder[enumFluentCollections, AuditCleared]) {
			projections.Value(f, projections.Path[enumFluentCollections, *projectionEnum]("State"), nil)
			if literal {
				projections.Value(f, projections.Path[enumFluentCollections, *[]string]("Labels"), nil)
				projections.Value(f, projections.Path[enumFluentCollections, *map[string]string]("Attributes"), nil)
			} else {
				projections.Clear(f, projections.Path[enumFluentCollections, *[]string]("Labels"))
				projections.Clear(f, projections.Path[enumFluentCollections, *map[string]string]("Attributes"))
			}
		})
		declaration, err := builder.Build()
		if err != nil {
			t.Fatal(err)
		}
		assertCollectionNullMappings(t, mustCompile(t, declaration, event.Descriptor()), "State", "Labels", "Attributes")
	}
}

func TestNullableCollectionsDoNotWidenEnumAssignments(t *testing.T) {
	codecs := projectionEnumCodecs(t, false)
	event := mustEvent[EnumChanged](t, events.WithCodecs(codecs))
	model := mustModel[enumFluentCollections](t, readmodels.WithCodecs(codecs))
	for _, operation := range []string{"context", "increment", "cross-table"} {
		t.Run(operation, func(t *testing.T) {
			builder := projections.NewBuilder("enum-refusal", model, projections.NoAutoMap())
			source := event
			if operation == "cross-table" {
				source = mustEvent[EnumChanged](t, events.WithCodecs(projectionEnumCodecs(t, true)))
			}
			projections.From(builder, source, func(f *projections.FromBuilder[enumFluentCollections, EnumChanged]) {
				projections.Clear(f, projections.Path[enumFluentCollections, *[]string]("Labels"))
				target := projections.Path[enumFluentCollections, *projectionEnum]("State")
				switch operation {
				case "context":
					projections.Context(f, target, "observationState")
				case "increment":
					projections.Increment(f, target)
				case "cross-table":
					projections.Map(f, target, projections.Path[EnumChanged, *projectionEnum]("Optional"))
				}
			})
			_, err := builder.Build()
			enumBoundaryFailure(t, err, "State")
		})
	}
}
