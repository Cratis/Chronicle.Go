// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestZeroDefinitionAccessors(t *testing.T) {
	var definition projections.Definition
	if definition.Identifier() != "" || definition.Model().GoType() != nil || definition.EventSequence() != "" || definition.IsPassive() || definition.KeyField() != "" || definition.Provenance() != nil || definition.Diagnostics() != nil || definition.KernelDefinition() != nil {
		t.Fatal("zero definition did not return zero metadata")
	}
}

type OptionalName struct {
	Name *string `json:"name"`
}

func TestMapAsPreservesModelBoundCompatibility(t *testing.T) {
	event := mustEvent[Opened](t)
	model := mustModel[OptionalName](t)
	builder := projections.NewBuilder("optional", model)
	projections.From(builder, event, func(from *projections.FromBuilder[OptionalName, Opened]) {
		projections.MapAs(from, projections.Path[OptionalName, *string]("name"), projections.Path[Opened, string]("fullName"))
	})
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if got := mustCompile(t, declaration, event.Descriptor()).KernelDefinition().From[0].Value.Properties["name"]; got != "fullName" {
		t.Fatalf("mapping = %q, want fullName", got)
	}
}

func TestMapAsRejectsIncompatibleAndIncorrectFieldTypes(t *testing.T) {
	event := mustEvent[Opened](t)
	for _, test := range []struct {
		name   string
		define func(*projections.FromBuilder[FluentAccount, Opened])
	}{
		{"incompatible", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.MapAs(from, projections.Path[FluentAccount, bool]("enabled"), projections.Path[Opened, string]("fullName"))
		}},
		{"wrong target type", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.MapAs(from, projections.Path[FluentAccount, *string]("name"), projections.Path[Opened, string]("fullName"))
		}},
		{"wrong source type", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.MapAs(from, projections.Path[FluentAccount, *string]("note"), projections.Path[Opened, *string]("fullName"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := projections.NewBuilder("invalid", mustModel[FluentAccount](t))
			projections.From(builder, event, test.define)
			_, err := builder.Build()
			var declaration *projections.DeclarationError
			if !errors.As(err, &declaration) || declaration.Directive != "set" {
				t.Fatalf("invalid MapAs accepted: %v", err)
			}
		})
	}
}

func TestFluentBuilderInheritsModelDefaults(t *testing.T) {
	event := mustEvent[Opened](t)
	model := mustModel[OptionalName](t, readmodels.WithObserver(readmodels.Projection, "optional"), readmodels.WithEventSequence("orders"))
	builder := projections.NewBuilder("", model)
	projections.From(builder, event, nil)
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	definition := mustCompile(t, declaration, event.Descriptor())
	if definition.Identifier() != "optional" || definition.EventSequence() != "orders" {
		t.Fatal("fluent defaults did not inherit model settings")
	}
}

type BooleanTruePath struct {
	Value string `json:"true"`
}
type BooleanTitleTruePath struct {
	Value string `json:"True"`
}
type BooleanFalsePath struct {
	Value string `json:"false"`
}
type BooleanTitleFalsePath struct {
	Value string `json:"False"`
}

func TestBooleanPropertyPathsRejectLiteralResolverAmbiguity(t *testing.T) {
	t.Run("true", func(t *testing.T) { rejectBooleanPropertyPath[BooleanTruePath](t, "true") })
	t.Run("True", func(t *testing.T) { rejectBooleanPropertyPath[BooleanTitleTruePath](t, "True") })
	t.Run("false", func(t *testing.T) { rejectBooleanPropertyPath[BooleanFalsePath](t, "false") })
	t.Run("False", func(t *testing.T) { rejectBooleanPropertyPath[BooleanTitleFalsePath](t, "False") })
}

func rejectBooleanPropertyPath[E any](t *testing.T, path string) {
	t.Helper()
	event := mustEvent[E](t)
	builder := projections.NewBuilder("boolean-path", mustModel[OptionalName](t))
	projections.From(builder, event, func(from *projections.FromBuilder[OptionalName, E]) {
		projections.MapAs(from, projections.Path[OptionalName, *string]("name"), projections.Path[E, string](path))
	})
	_, err := builder.Build()
	var declaration *projections.DeclarationError
	if !errors.As(err, &declaration) || declaration.Path != "name" {
		t.Fatalf("boolean property path accepted: %v", err)
	}
}

func TestFluentLiteralUTF16Boundary(t *testing.T) {
	event := mustEvent[Opened](t)
	for _, test := range []struct {
		name  string
		value string
		valid bool
	}{
		{"BMP letter", "\uff21", true},
		{"supplementary letter", "\U00010400", false},
		{"supplementary digit", "\U0001d7ce", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := projections.NewBuilder("literal", mustModel[FluentAccount](t))
			projections.From(builder, event, func(from *projections.FromBuilder[FluentAccount, Opened]) {
				projections.Value(from, projections.Path[FluentAccount, string]("name"), test.value)
			})
			_, err := builder.Build()
			var declaration *projections.DeclarationError
			if test.valid && err != nil || !test.valid && !errors.As(err, &declaration) {
				t.Fatalf("valid = %v, error = %v", test.valid, err)
			}
		})
	}
}
