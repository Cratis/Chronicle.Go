// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type modelKeyAliasFirst struct {
	Alias string `json:"data.payload" chronicle:"key"`
	Data  struct{ Payload []byte }
}
type modelKeyAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"data.payload" chronicle:"key"`
}
type modelVariantAliasFirst struct {
	Alias string `json:"data.payload"`
	Data  struct{ Payload []byte }
}
type modelVariantAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"data.payload"`
}
type modelAliasEvent struct{ Name string }
type modelAliasVariant struct{}

func TestBinaryModelKeysRefuseNewNamingAmbiguity(t *testing.T) {
	t.Run("key tag alias first", func(t *testing.T) { testBinaryModelKeyNaming[modelKeyAliasFirst](t, false) })
	t.Run("key tag alias last", func(t *testing.T) { testBinaryModelKeyNaming[modelKeyAliasLast](t, false) })
	t.Run("variant alias first", func(t *testing.T) { testBinaryModelKeyNaming[modelVariantAliasFirst](t, true) })
	t.Run("variant alias last", func(t *testing.T) { testBinaryModelKeyNaming[modelVariantAliasLast](t, true) })
}
func testBinaryModelKeyNaming[M any](t *testing.T, variant bool) {
	t.Helper()
	model, err := readmodels.Define[M]()
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[modelAliasEvent]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	options := []projections.Option{projections.NoAutoMap()}
	if variant {
		options = append(options, projections.VariantOf[modelAliasVariant](), projections.VariantKey(projections.Path[M, string]("data.payload")), projections.EntersOn(event))
	}
	b := projections.NewBuilder("binary-model-key-naming", model, options...)
	projections.From(b, event, nil)
	declaration, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	definition, err := projections.Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	next, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err == nil {
		_, err = definition.Rebind(next, catalog, catalog)
	}
	if !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("binary model key ambiguity admitted: %v", err)
	}
}
