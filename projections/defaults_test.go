// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"math"
	"os"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type DefaultEvent struct{ Name string }
type defaultNested struct {
	City string
	Note *string
}
type defaultModel struct {
	ID       string
	Name     string
	Number   int32
	Enabled  bool
	Note     *string
	Items    []string
	Lookup   map[string]defaultNested
	Address  defaultNested
	Amount   conceptfixtures.Number
	External string `json:"external_name"`
	Optional int32  `json:",omitempty"`
}

func defaultFixture(t *testing.T) (readmodels.Model[defaultModel], events.Type[DefaultEvent], *events.Catalog) {
	t.Helper()
	model, err := readmodels.Define[defaultModel](readmodels.WithIdentifier("Example.DefaultModel"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[DefaultEvent](events.WithID("DefaultEvent"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return model, event, catalog
}

func TestInitialValuesAndLabelsShareCanonicalDefinition(t *testing.T) {
	model, event, catalog := defaultFixture(t)
	values := defaultModel{Items: []string{}, Lookup: map[string]defaultNested{"one": {City: "Oslo"}}, Address: defaultNested{City: "Bergen"}}
	labels := []string{"accounts", "read", "accounts", "Read"}
	options := []projections.Option{projections.WithIdentifier("Example.DefaultProjection"), projections.WithInitialValues(values), projections.WithLabels(labels...)}
	bound := projections.ModelBound(model, append(options, projections.FromEvent(event))...)
	builder := projections.NewBuilder("", model, options...)
	projections.From(builder, event, nil)
	fluent, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	values.Lookup["one"] = defaultNested{City: "mutated"}
	labels[0] = "mutated"
	first, err := projections.Compile(bound, catalog)
	if err != nil {
		t.Fatal(err)
	}
	second, err := projections.Compile(fluent, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(first.KernelDefinition(), second.KernelDefinition()) {
		t.Fatal("front ends diverged")
	}
	// Hand-derived, not captured from C#: ProjectionBuilder.WithInitialValues
	// and ProjectionBuilderFor.Build at 2e31b0df; C# ignores null properties.
	// Go canonicalizes object order, unlike System.Text.Json declaration order.
	want := `{"Address":{"City":"Bergen"},"Amount":0,"Enabled":false,"Id":"","Items":[],"Lookup":{"one":{"City":"Oslo"}},"Name":"","Number":0,"external_name":""}`
	fixture, err := os.ReadFile("testdata/defaults.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := &contracts.ProjectionDefinition{}
	if err = protojson.Unmarshal(fixture, expected); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(first.KernelDefinition(), expected) {
		t.Fatal("hand-derived C# definition mismatch")
	}
	if got := first.KernelDefinition().InitialModelState; got != want {
		t.Fatalf("state=%s want=%s", got, want)
	}
	if got := first.KernelDefinition().Tags; len(got) != 3 || got[0] != "accounts" || got[1] != "read" || got[2] != "Read" {
		t.Fatalf("labels=%v", got)
	}
	hash, err := first.Hash()
	if err != nil {
		t.Fatal(err)
	}
	wire := first.KernelDefinition()
	wire.InitialModelState = "{}"
	wire.Tags[0] = "mutated"
	if next, err := first.Hash(); err != nil || next != hash {
		t.Fatal("mutable output changed hash")
	}
	if other, err := second.Hash(); err != nil || other != hash {
		t.Fatal("front-end hash mismatch")
	}
}

func TestInitialValuesPresenceAndFrozenNaming(t *testing.T) {
	model, event, catalog := defaultFixture(t)
	options := []projections.Option{
		projections.FromEvent(event),
		projections.WithInitialValue(projections.Path[defaultModel, int32]("Number"), 0),
		projections.WithInitialValue(projections.Path[defaultModel, int32]("Optional"), 0),
		projections.WithInitialValue(projections.Path[defaultModel, *string]("Note"), nil),
		projections.WithInitialValue(projections.Path[defaultModel, string]("Address.City"), "Oslo"),
		projections.WithInitialValue(projections.Path[defaultModel, conceptfixtures.Number]("Amount"), conceptfixtures.Number(7)),
	}
	definition, err := projections.Compile(projections.ModelBound(model, options...), catalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		next, err := model.Descriptor().WithNamingPolicy(policy)
		if err != nil {
			t.Fatal(err)
		}
		rebound, err := definition.Rebind(next, catalog, catalog)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"Address":{"City":"Oslo"},"Amount":7,"Note":null,"Number":0,"Optional":0}`
		if policy != serialization.PreservePropertyNames {
			want = `{"address":{"city":"Oslo"},"amount":7,"note":null,"number":0,"optional":0}`
		}
		if got := rebound.KernelDefinition().InitialModelState; got != want {
			t.Fatalf("policy=%v state=%s", policy, got)
		}
	}
	omitted, err := projections.Compile(projections.ModelBound(model, projections.FromEvent(event)), catalog)
	if err != nil || omitted.KernelDefinition().InitialModelState != "{}" {
		t.Fatal("omitted state", err)
	}
	a, err := omitted.Hash()
	if err != nil {
		t.Fatal(err)
	}
	b, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("initial state absent from identity")
	}
}

func TestInitialValuesRejectInvalidShapesAndLabels(t *testing.T) {
	model, event, catalog := defaultFixture(t)
	cases := map[string][]projections.Option{
		"foreign model":      {projections.WithInitialValues(42)},
		"null root":          {projections.WithInitialValues((*defaultModel)(nil))},
		"unknown path":       {projections.WithInitialValue(projections.Path[defaultModel, int32]("missing"), 0)},
		"wrong type":         {projections.WithInitialValue(projections.Path[defaultModel, string]("Number"), "zero")},
		"object path":        {projections.WithInitialValue(projections.Path[defaultModel, defaultNested]("Address"), defaultNested{})},
		"collection path":    {projections.WithInitialValue(projections.Path[defaultModel, string]("Items"), "empty")},
		"duplicate":          {projections.WithInitialValue(projections.Path[defaultModel, int32]("Number"), 0), projections.WithInitialValue(projections.Path[defaultModel, int32]("Number"), 1)},
		"collision":          {projections.WithInitialValues(defaultModel{}), projections.WithInitialValue(projections.Path[defaultModel, string]("Address.City"), "override")},
		"blank label":        {projections.WithLabels(" ")},
		"control label":      {projections.WithLabels("read\n")},
		"invalid UTF-8":      {projections.WithLabels(string([]byte{0xff}))},
		"error stays sticky": {projections.WithLabels(""), projections.WithInitialValues(defaultModel{})},
	}
	for name, options := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := projections.Compile(projections.ModelBound(model, append(options, projections.FromEvent(event))...), catalog)
			if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

type protectedDefault struct {
	Name  *string `chronicle:"pii"`
	Count int32
}
type invalidDefault struct{ Number float64 }

func TestInitialValuesRejectPlaintextProtectionAndNonfiniteValues(t *testing.T) {
	_, event, catalog := defaultFixture(t)
	model, err := readmodels.Define[protectedDefault]()
	if err != nil {
		t.Fatal(err)
	}
	name := "secret"
	for _, option := range []projections.Option{projections.WithInitialValues(protectedDefault{Name: &name}), projections.WithInitialValue(projections.Path[protectedDefault, *string]("Name"), nil)} {
		_, err := projections.Compile(projections.ModelBound(model, projections.FromEvent(event), option), catalog)
		if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatal("protected initializer accepted", err)
		}
	}
	if _, err := projections.Compile(projections.ModelBound(model, projections.FromEvent(event), projections.WithInitialValues(protectedDefault{Count: 0})), catalog); err != nil {
		t.Fatal("omitted protected value should be allowed", err)
	}
	invalid, err := readmodels.Define[invalidDefault]()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projections.Compile(projections.ModelBound(invalid, projections.FromEvent(event), projections.WithInitialValues(invalidDefault{Number: math.NaN()})), catalog); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal("nonfinite initializer accepted", err)
	}
}
