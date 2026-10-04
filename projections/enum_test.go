// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type projectionEnum int32
type otherProjectionEnum int32

func (projectionEnum) MarshalJSON() ([]byte, error) { panic("enum literal invoked JSON hook") }
func (*projectionEnum) UnmarshalJSON([]byte) error  { panic("enum literal invoked JSON decoder") }

type EnumChanged struct {
	Value    projectionEnum
	Values   []projectionEnum
	Number   int32
	Optional *projectionEnum
	Other    otherProjectionEnum
}
type EnumView struct {
	ID       string
	Value    projectionEnum
	Values   []projectionEnum
	Optional *projectionEnum
	Number   int32
}
type EnumLiteralView struct {
	ID    string
	Value projectionEnum `chronicle:"value(EnumChanged,value=1)"`
}
type EnumBadLiteralView struct {
	ID    string
	Value projectionEnum `chronicle:"value(EnumChanged,value=9)"`
}
type EnumCopyView struct {
	ID    string
	Value projectionEnum `chronicle:"set(EnumChanged)"`
}
type EnumBadCopyView struct {
	ID    string
	Value projectionEnum `chronicle:"set(EnumChanged,from=Number)"`
}

func projectionEnumCodecs(t *testing.T, changed bool) *serialization.Codecs {
	t.Helper()
	one := projectionEnum(1)
	if changed {
		one = 9
	}
	c, err := serialization.NewCodecs(
		serialization.Enum(serialization.EnumMember[projectionEnum]{Name: "Zero", Value: 0}, serialization.EnumMember[projectionEnum]{Name: "One", Value: one}),
		serialization.Enum(serialization.EnumMember[otherProjectionEnum]{Name: "Zero", Value: 0}, serialization.EnumMember[otherProjectionEnum]{Name: "One", Value: 1}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestEnumProjectionCopyLiteralsAndRefusals(t *testing.T) {
	c := projectionEnumCodecs(t, false)
	event := mustEvent[EnumChanged](t, events.WithCodecs(c))
	model := mustModel[EnumView](t, readmodels.WithCodecs(c))
	value := projections.Path[EnumView, projectionEnum]("Value")
	array := projections.Path[EnumView, []projectionEnum]("Values")
	optional := projections.Path[EnumView, *projectionEnum]("Optional")
	for _, test := range []struct {
		name   string
		valid  bool
		define func(*projections.FromBuilder[EnumView, EnumChanged])
	}{
		{"copy", true, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.Map(b, value, projections.Path[EnumChanged, projectionEnum]("Value"))
		}},
		{"array copy", true, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.Map(b, array, projections.Path[EnumChanged, []projectionEnum]("Values"))
		}},
		{"literal", true, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.Value(b, value, projectionEnum(1))
		}},
		{"unknown literal", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.Value(b, value, projectionEnum(9))
		}},
		{"clear nullable", true, func(b *projections.FromBuilder[EnumView, EnumChanged]) { projections.Clear(b, optional) }},
		{"clear required", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) { projections.Clear(b, value) }},
		{"plain into enum", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.MapAs(b, value, projections.Path[EnumChanged, int32]("Number"))
		}},
		{"enum into plain", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.MapAs(b, projections.Path[EnumView, int32]("Number"), projections.Path[EnumChanged, projectionEnum]("Value"))
		}},
		{"other enum", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.MapAs(b, value, projections.Path[EnumChanged, otherProjectionEnum]("Other"))
		}},
		{"nullable conversion", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.MapAs(b, optional, projections.Path[EnumChanged, projectionEnum]("Value"))
		}},
		{"add", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.Add(b, value, projections.Path[EnumChanged, projectionEnum]("Value"))
		}},
		{"subtract", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.Subtract(b, value, projections.Path[EnumChanged, projectionEnum]("Value"))
		}},
		{"increment", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) { projections.Increment(b, value) }},
		{"decrement", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) { projections.Decrement(b, value) }},
		{"count", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) { projections.Count(b, value) }},
		{"context", false, func(b *projections.FromBuilder[EnumView, EnumChanged]) {
			projections.Context(b, value, "observationState")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := projections.NewBuilder("enum", model, projections.NoAutoMap())
			projections.From(b, event, test.define)
			_, err := b.Build()
			if (err == nil) != test.valid {
				t.Fatalf("valid %v: %v", test.valid, err)
			}
		})
	}
	all := projections.NewBuilder("all-enums", model)
	projections.From(all, event, nil)
	projections.All(all, func(b *projections.EveryBuilder[EnumView]) { projections.EveryMap(b, value, "Value") })
	if _, err := all.Build(); err == nil {
		t.Fatal("all-event enum mapping admitted unknown profiles")
	}
	changed := mustEvent[EnumChanged](t, events.WithCodecs(projectionEnumCodecs(t, true)))
	b := projections.NewBuilder("enum", model)
	projections.From(b, changed, nil)
	if _, err := b.Build(); err == nil {
		t.Fatal("auto-map accepted changed enum table")
	}
	for _, declaration := range []projections.Declaration{
		projections.ModelBound(mustModel[EnumLiteralView](t, readmodels.WithCodecs(c))),
		projections.ModelBound(mustModel[EnumCopyView](t, readmodels.WithCodecs(c))),
	} {
		_ = mustCompile(t, declaration, event.Descriptor())
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range []projections.Declaration{
		projections.ModelBound(mustModel[EnumBadLiteralView](t, readmodels.WithCodecs(c))),
		projections.ModelBound(mustModel[EnumBadCopyView](t, readmodels.WithCodecs(c))),
	} {
		if _, err := projections.Compile(declaration, catalog); err == nil {
			t.Fatal("model-bound enum bypass")
		}
	}
}
