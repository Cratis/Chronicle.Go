// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/projections"
)

type projectionGlobalIdentity struct{}
type projectionBareGlobal struct {
	Name string `json:"name"`
}

func TestGlobalFromWithoutPropertiesFailsAtNewClient(t *testing.T) {
	for _, name := range []string{"AutoMap", "NoAutoMap"} {
		t.Run(name, func(t *testing.T) {
			registry := NewRegistry()
			model, err := RegisterReadModel[ProjectionModel](registry)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := RegisterEvent[ProjectionOpened](registry)
			if err != nil {
				t.Fatal(err)
			}
			variant := projections.ModelBound(model, projections.VariantOf[projectionGlobalIdentity](), projections.EntersOn(opened))
			if err := registry.AddProjection(variant); err != nil {
				t.Fatal(err)
			}
			options := []projections.Option{projections.GlobalFor[projectionGlobalIdentity](), projections.FromEvent(opened)}
			if name == "NoAutoMap" {
				options = append(options, projections.NoAutoMap())
			}
			global, err := projections.Global[projectionBareGlobal](options...)
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.AddProjection(global); err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(WithRegistry(registry))
			if client != nil {
				if closeErr := client.Close(); closeErr != nil {
					t.Error(closeErr)
				}
				t.Fatal("invalid global published a client")
			}
			var empty *projections.GlobalFromHasNoProperties
			var declaration *projections.DeclarationError
			if !errors.As(err, &empty) || !errors.As(err, &declaration) || !errors.Is(err, ErrInvalidConfiguration) || empty.Global != reflect.TypeFor[projectionBareGlobal]() || empty.Event != opened.Ref() || declaration.Directive != "FromEvent" {
				t.Fatalf("missing typed global From failure: %v", err)
			}
		})
	}
}
