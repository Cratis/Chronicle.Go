// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
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

type ownedDefaultNested struct {
	Name   conceptfixtures.Name
	Amount *conceptfixtures.Unsigned
}
type ownedDefaultModel struct {
	Note   *string
	Nested *ownedDefaultNested
}

func TestInitialValuesSnapshotPopulatedPointersAndNestedConcepts(t *testing.T) {
	_, event, catalog := defaultFixture(t)
	model, err := readmodels.Define[ownedDefaultModel]()
	if err != nil {
		t.Fatal(err)
	}
	note := "owned"
	amount := conceptfixtures.Unsigned{Value: 7}
	value := ownedDefaultModel{Note: &note, Nested: &ownedDefaultNested{Name: "nested", Amount: &amount}}
	bound := projections.ModelBound(model, projections.FromEvent(event), projections.WithInitialValues(value))
	builder := projections.NewBuilder("", model, projections.WithInitialValues(value))
	projections.From(builder, event, nil)
	fluent, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	scalars := projections.ModelBound(model, projections.FromEvent(event),
		projections.WithInitialValue(projections.Path[ownedDefaultModel, *string]("Note"), &note),
		projections.WithInitialValue(projections.Path[ownedDefaultModel, conceptfixtures.Name]("Nested.Name"), value.Nested.Name),
		projections.WithInitialValue(projections.Path[ownedDefaultModel, *conceptfixtures.Unsigned]("Nested.Amount"), &amount))
	note, amount.Value, value.Nested.Name = "mutated", 99, "mutated"
	value.Nested.Amount = nil
	for _, declaration := range []projections.Declaration{bound, fluent, scalars} {
		definition, err := projections.Compile(declaration, catalog)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"Nested":{"Amount":7,"Name":"nested"},"Note":"owned"}`
		if got := definition.KernelDefinition().InitialModelState; got != want {
			t.Fatalf("caller mutation changed snapshot: %s", got)
		}
		camel, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
		if err != nil {
			t.Fatal(err)
		}
		rebound, err := definition.Rebind(camel, catalog, catalog)
		if err != nil {
			t.Fatal(err)
		}
		if got := rebound.KernelDefinition().InitialModelState; got != `{"nested":{"amount":7,"name":"nested"},"note":"owned"}` {
			t.Fatalf("pointer/concept identity changed: %s", got)
		}
	}
}

type protectedDefaultBranch struct {
	Secret conceptfixtures.Name
	Public *string
	Next   *protectedDefaultBranch
}
type protectedDefaultRoot struct {
	Nested *protectedDefaultBranch
	Safe   string
}

func TestInitialValuesRejectEntireProtectedRootThroughOptionsAndReferences(t *testing.T) {
	_, event, catalog := defaultFixture(t)
	classifications := map[string]readmodels.ModelOption{
		"PII path":          readmodels.WithPII("Nested.Next.Secret"),
		"confidential path": readmodels.WithProtection(compliance.Property("Nested.Secret", compliance.Classification{Encrypted: true, Scope: compliance.Namespace})),
		"PII concept type and recursive references": readmodels.WithProtection(compliance.For[conceptfixtures.Name](compliance.Classification{PII: true})),
	}
	for name, classification := range classifications {
		t.Run(name, func(t *testing.T) {
			model, err := readmodels.Define[protectedDefaultRoot](classification)
			if err != nil {
				t.Fatal(err)
			}
			text := "public sibling"
			for _, option := range []projections.Option{
				projections.WithInitialValue(projections.Path[protectedDefaultRoot, *string]("Nested.Public"), nil),
				projections.WithInitialValue(projections.Path[protectedDefaultRoot, *string]("Nested.Public"), &text),
				projections.WithInitialValues(protectedDefaultRoot{Nested: &protectedDefaultBranch{Public: &text}}),
			} {
				_, err := projections.Compile(projections.ModelBound(model, projections.FromEvent(event), option), catalog)
				if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
					t.Fatalf("protected-root sibling/null accepted: %v", err)
				}
			}
			if _, err := projections.Compile(projections.ModelBound(model, projections.FromEvent(event), projections.WithInitialValues(protectedDefaultRoot{Safe: "allowed"})), catalog); err != nil {
				t.Fatalf("omitted protected root rejected: %v", err)
			}
		})
	}
}

type integerDefaultModel struct {
	Unsigned uint64
	Signed   int64
	Lookup   map[string]int64
}

func TestInitialValuesHonorKernelIntegerGuards(t *testing.T) {
	_, event, catalog := defaultFixture(t)
	model, err := readmodels.Define[integerDefaultModel]()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		option projections.Option
		valid  bool
	}{
		{"unsigned limit", projections.WithInitialValue(projections.Path[integerDefaultModel, uint64]("Unsigned"), math.MaxInt64), true},
		{"unsigned overflow", projections.WithInitialValue(projections.Path[integerDefaultModel, uint64]("Unsigned"), uint64(math.MaxInt64)+1), false},
		{"signed minimum", projections.WithInitialValues(integerDefaultModel{Signed: math.MinInt64}), true},
		{"signed maximum", projections.WithInitialValues(integerDefaultModel{Signed: math.MaxInt64}), true},
		{"dictionary positive limit", projections.WithInitialValues(integerDefaultModel{Lookup: map[string]int64{"n": 1 << 53}}), true},
		{"dictionary negative limit", projections.WithInitialValues(integerDefaultModel{Lookup: map[string]int64{"n": -1 << 53}}), true},
		{"dictionary positive overflow", projections.WithInitialValues(integerDefaultModel{Lookup: map[string]int64{"n": 1<<53 + 1}}), false},
		{"dictionary negative overflow", projections.WithInitialValues(integerDefaultModel{Lookup: map[string]int64{"n": -1<<53 - 1}}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := projections.Compile(projections.ModelBound(model, projections.FromEvent(event), tc.option), catalog)
			if tc.valid && err != nil || !tc.valid && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}

func TestInitialValuesStringsAreJSONNotProjectionExpressions(t *testing.T) {
	model, event, catalog := defaultFixture(t)
	for _, value := range []string{"\"quoted\"\\path\n\t{};=$event.Name", "$null", "true", "😀漢字<>&'"} {
		t.Run(value, func(t *testing.T) {
			definition, err := projections.Compile(projections.ModelBound(model, projections.FromEvent(event), projections.WithInitialValue(projections.Path[defaultModel, string]("Name"), value)), catalog)
			if err != nil {
				t.Fatal(err)
			}
			var state map[string]string
			if err := json.Unmarshal([]byte(definition.KernelDefinition().InitialModelState), &state); err != nil || state["Name"] != value {
				t.Fatalf("literal changed: state=%v error=%v", state, err)
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
