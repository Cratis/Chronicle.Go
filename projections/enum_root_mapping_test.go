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

type enumSparseTick struct{ Ready bool }
type enumSparseUnused struct{ TRUE *projectionEnum }
type enumSparseModel struct {
	ID     string `json:"ID" chronicle:"key"`
	Status *projectionEnum
	Ready  bool
}
type enumSparseBoundModel struct {
	ID     string          `json:"ID" chronicle:"key"`
	Status *projectionEnum `chronicle:"every(from=TRUE)"`
	Ready  bool
}
type enumSparseGlobal struct {
	Ready bool `chronicle:"set(enumSparseTick)"`
}

func TestEnumSparseEveryUnusedCatalogRebindRefusesBeforeRPC(t *testing.T) {
	for _, name := range []string{"root", "generated EntersOn", "merged global"} {
		variant, global := name != "root", name == "merged global"
		t.Run(name+"/fluent", func(t *testing.T) { enumSparseUnusedCatalog[enumSparseModel](t, variant, false, global) })
		t.Run(name+"/model-bound", func(t *testing.T) { enumSparseUnusedCatalog[enumSparseBoundModel](t, variant, true, global) })
	}
}

func enumSparseUnusedCatalog[M any](t *testing.T, variant, bound, global bool) {
	t.Helper()
	codecs := projectionEnumCodecs(t, false)
	r := chronicle.NewRegistry()
	tick, err := chronicle.RegisterEvent[enumSparseTick](r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[enumSparseUnused](r, events.WithCodecs(codecs)); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[M](r, readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	options := []projections.Option{projections.NoAutoMap()}
	if variant {
		options = append(options, projections.VariantOf[WorkItem](), projections.EntersOn(tick))
	}
	var declaration projections.Declaration
	if bound {
		if !variant {
			options = append(options, projections.FromEvent(tick))
		}
		declaration = projections.ModelBound(model, options...)
	} else {
		b := projections.NewBuilder("enum-sparse-rebound", model, options...)
		if !variant {
			projections.From(b, tick, nil)
		}
		projections.Every(b, func(e *projections.EveryBuilder[M]) {
			projections.EveryMap(e, projections.Path[M, *projectionEnum]("Status"), "TRUE")
		})
		declaration, err = b.Build()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := r.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	if global {
		shared, err := projections.Global[enumSparseGlobal](projections.GlobalFor[WorkItem]())
		if err != nil {
			t.Fatal(err)
		}
		if err := r.AddProjection(shared); err != nil {
			t.Fatal(err)
		}
	}
	enumRegistrationNoRPC(t, r, "status", serialization.LegacyGoCamelCase)
}

type enumDottedEvent struct {
	Status projectionEnum `json:"State.Value"`
	Ready  bool
}
type enumDottedModel struct {
	ID     string         `json:"ID" chronicle:"key"`
	Status projectionEnum `json:"State.Value"`
}
type enumDottedPlain struct {
	ID     string `json:"ID" chronicle:"key"`
	Status int32  `json:"State.Value"`
}
type enumDottedNullable struct {
	ID     string          `json:"ID" chronicle:"key"`
	Status *projectionEnum `json:"State.Value"`
}
type enumDottedArray struct {
	ID     string           `json:"ID" chronicle:"key"`
	Status []projectionEnum `json:"State.Value"`
}
type enumDottedPlainEvent struct {
	Status int32 `json:"State.Value"`
}
type enumDottedNullableEvent struct {
	Status *projectionEnum `json:"State.Value"`
}
type enumDottedArrayEvent struct {
	Status []projectionEnum `json:"State.Value"`
}

func TestEnumDirectDottedRootAutoMapRefusesBeforeRPC(t *testing.T) {
	t.Run("both enum", func(t *testing.T) {
		enumLiteralProperty[enumDottedModel, enumDottedEvent](t, "State.Value", serialization.PreservePropertyNames)
	})
	t.Run("source enum only", func(t *testing.T) {
		enumLiteralProperty[enumDottedPlain, enumDottedEvent](t, "State.Value", serialization.PreservePropertyNames)
	})
	t.Run("target enum only", func(t *testing.T) {
		enumLiteralProperty[enumDottedModel, enumDottedPlainEvent](t, "State.Value", serialization.PreservePropertyNames)
	})
	t.Run("nullable", func(t *testing.T) {
		enumLiteralProperty[enumDottedNullable, enumDottedNullableEvent](t, "State.Value", serialization.PreservePropertyNames)
	})
	t.Run("array", func(t *testing.T) {
		enumLiteralProperty[enumDottedArray, enumDottedArrayEvent](t, "State.Value", serialization.PreservePropertyNames)
	})
}
