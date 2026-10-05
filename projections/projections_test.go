// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Opened struct {
	FullName    string    `json:"fullName"`
	ProductName string    `json:"productName"`
	Excluded    string    `json:"excluded"`
	Local       string    `json:"local"`
	AccountID   uuid.UUID `json:"accountId"`
	ParentID    uuid.UUID `json:"parentId"`
}
type Account struct {
	ID          uuid.UUID `json:"id" chronicle:"key"`
	Name        string    `json:"name" chronicle:"set(@opened,from=fullName)"`
	ProductName string    `json:"productName"`
	Updated     time.Time `json:"updated" chronicle:"context(@opened,from=occurred)"`
	State       string    `json:"state" chronicle:"value(@opened,value=\"active\")"`
	Number      int32     `json:"number" chronicle:"value(@opened,value=42)"`
	Enabled     bool      `json:"enabled" chronicle:"value(@opened,value=true)"`
	Note        *string   `json:"note" chronicle:"value(@opened,value=null)"`
	Excluded    string    `json:"excluded" chronicle:"no-auto"`
	Local       string    `json:"local" chronicle:"not-projected"`
}
type FluentAccount struct {
	ID          uuid.UUID `json:"id" chronicle:"key"`
	Name        string    `json:"name"`
	ProductName string    `json:"productName"`
	Updated     time.Time `json:"updated"`
	State       string    `json:"state"`
	Number      int32     `json:"number"`
	Enabled     bool      `json:"enabled"`
	Note        *string   `json:"note"`
	Excluded    string    `json:"excluded" chronicle:"no-auto"`
	Local       string    `json:"local" chronicle:"not-projected"`
}

func mustEvent[T any](t *testing.T, options ...events.TypeOption) events.Type[T] {
	t.Helper()
	event, err := events.Define[T](options...)
	if err != nil {
		t.Fatal(err)
	}
	return event
}
func mustModel[T any](t *testing.T, options ...readmodels.ModelOption) readmodels.Model[T] {
	t.Helper()
	model, err := readmodels.Define[T](options...)
	if err != nil {
		t.Fatal(err)
	}
	return model
}
func mustCompile(t *testing.T, declaration projections.Declaration, event events.Descriptor) projections.Definition {
	t.Helper()
	catalog, err := events.NewCatalog(event)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := projections.Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}
func TestBothFrontEndsMatchCSharpBasicGolden(t *testing.T) {
	opened := mustEvent[Opened](t, events.WithID("account-opened"))
	model := mustModel[Account](t, readmodels.WithIdentifier("Example.Account"))
	modelBound := projections.ModelBound(model, projections.WithIdentifier("Example.AccountProjection"), projections.BindEvent("opened", opened), projections.FromEvent(opened))
	bound := mustCompile(t, modelBound, opened.Descriptor())
	fluentModel := mustModel[FluentAccount](t, readmodels.WithIdentifier("Example.Account"))
	builder := projections.NewBuilder("Example.AccountProjection", fluentModel)
	projections.From(builder, opened, func(from *projections.FromBuilder[FluentAccount, Opened]) {
		projections.Map(from, projections.Path[FluentAccount, string]("name"), projections.Path[Opened, string]("fullName"))
		projections.Context(from, projections.Path[FluentAccount, time.Time]("updated"), "occurred")
		projections.Value(from, projections.Path[FluentAccount, string]("state"), "active")
		projections.Value(from, projections.Path[FluentAccount, int32]("number"), int32(42))
		projections.Value(from, projections.Path[FluentAccount, bool]("enabled"), true)
		projections.Value(from, projections.Path[FluentAccount, *string]("note"), nil)
	})
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	fluent := mustCompile(t, declaration, opened.Descriptor())
	data, err := os.ReadFile("testdata/basic.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := &contracts.ProjectionDefinition{}
	if err = protojson.Unmarshal(data, expected); err != nil {
		t.Fatal(err)
	}
	for _, definition := range []projections.Definition{bound, fluent} {
		actual := definition.KernelDefinition()
		if !proto.Equal(actual, expected) {
			t.Fatalf("got %s\nwant %s", protojson.Format(actual), protojson.Format(expected))
		}
		if definition.KeyField() != "id" || len(definition.Provenance()) != 10 {
			t.Fatalf("missing provenance: %+v", definition.Provenance())
		}
		actual.From[0].Value.Properties["state"] = "mutated"
		actual.NoAutoMapProperties[0] = "mutated"
		provenance := definition.Provenance()
		provenance[0].Path = "mutated"
		if !proto.Equal(definition.KernelDefinition(), expected) {
			t.Fatal("definition is mutable")
		}
	}
}
func TestFromEventKeysFlagsAndDefaultIdentity(t *testing.T) {
	event := mustEvent[Opened](t)
	model := mustModel[FluentAccount](t)
	for _, test := range []struct {
		name        string
		options     []projections.FromOption
		key, parent string
	}{
		{"source", nil, "$eventSourceId", ""},
		{"property", []projections.FromOption{projections.UsingKey(projections.Path[Opened, uuid.UUID]("accountId"))}, "accountId", ""},
		{"parent", []projections.FromOption{projections.UsingParentKey(projections.Path[Opened, uuid.UUID]("parentId"))}, "$eventSourceId", "parentId"},
		{"constant wins", []projections.FromOption{projections.UsingKey(projections.Path[Opened, uuid.UUID]("accountId")), projections.UsingConstantKey("total")}, "$value(total)", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			definition := mustCompile(t, projections.ModelBound(model, projections.FromEvent(event, test.options...), projections.NotRewindable(), projections.NoAutoMap(), projections.WithEventSequence("orders")), event.Descriptor())
			wire := definition.KernelDefinition()
			if wire.Identifier != "github.com/cratis/chronicle.go/projections_test.FluentAccount" || wire.AutoMap != contracts.AutoMap_Disabled || wire.IsRewindable || wire.EventSequenceId != "orders" || wire.From[0].Value.Key != test.key || wire.From[0].Value.ParentKey != test.parent {
				t.Fatalf("%s", protojson.Format(wire))
			}
			builder := projections.NewBuilder("", model, projections.NotRewindable(), projections.NoAutoMap(), projections.WithEventSequence("orders"))
			projections.From(builder, event, nil, test.options...)
			declaration, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(wire, mustCompile(t, declaration, event.Descriptor()).KernelDefinition()) {
				t.Fatal("front-end key/flag mismatch")
			}
		})
	}
	passive := mustCompile(t, projections.ModelBound(model, projections.FromEvent(event), projections.Passive()), event.Descriptor())
	kind, id := passive.Model().Observer()
	if !passive.IsPassive() || passive.KernelDefinition().IsActive || passive.Model().Sink().Type != readmodels.NoSink || kind != readmodels.Projection || id != passive.Identifier() {
		t.Fatal("passive model binding failed")
	}
	if model.Descriptor().Sink().Type != readmodels.MongoDB {
		t.Fatal("original descriptor mutated")
	}
}

type ContextOnly struct {
	Name      string    `json:"fullName"`
	Timestamp time.Time `json:"timestamp" chronicle:"context(Opened,from=occurred)"`
}
type ReferenceModel struct {
	Bare      string `json:"bare" chronicle:"set(Opened,from=fullName)"`
	Qualified string `json:"qualified" chronicle:"set(go(\"github.com/cratis/chronicle.go/projections_test.Opened\"),from=fullName)"`
	Persisted string `json:"persisted" chronicle:"set(id(\"account-opened\",2),from=fullName)"`
	Alias     string `json:"alias" chronicle:"set(@event,from=fullName)"`
}

func TestContextOnlySubscriptionAndCatalogReferences(t *testing.T) {
	event := mustEvent[Opened](t, events.WithID("account-opened"), events.WithGeneration(2))
	definition := mustCompile(t, projections.ModelBound(mustModel[ContextOnly](t)), event.Descriptor())
	wire := definition.KernelDefinition()
	if len(wire.From) != 1 || wire.AutoMap != contracts.AutoMap_Enabled || len(wire.From[0].Value.Properties) != 1 || wire.From[0].Value.Properties["timestamp"] != "$eventContext(occurred)" {
		t.Fatal(protojson.Format(wire))
	}
	references := mustCompile(t, projections.ModelBound(mustModel[ReferenceModel](t), projections.BindEvent("event", event)), event.Descriptor())
	if len(references.KernelDefinition().From) != 1 || len(references.KernelDefinition().From[0].Value.Properties) != 4 {
		t.Fatal("references did not resolve to one descriptor")
	}
	other := mustEvent[Opened](t, events.WithID("different"))
	catalog, err := events.NewCatalog(other.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = projections.Compile(projections.ModelBound(mustModel[ReferenceModel](t), projections.BindEvent("event", event)), catalog); err == nil {
		t.Fatal("foreign typed alias accepted")
	}
}

type Shadowed struct {
	Name string `json:"name" chronicle:"value(Opened,value=\"last\");set(Opened,from=fullName);value(Opened,value=\"winner\")"`
}

func TestModelBoundPrecedenceAndFluentDuplicateRejection(t *testing.T) {
	event := mustEvent[Opened](t)
	definition := mustCompile(t, projections.ModelBound(mustModel[Shadowed](t)), event.Descriptor())
	if definition.KernelDefinition().From[0].Value.Properties["name"] != "$value(winner)" || len(definition.Diagnostics()) != 2 {
		t.Fatalf("%+v", definition.Diagnostics())
	}
	builder := projections.NewBuilder("duplicate", mustModel[FluentAccount](t))
	projections.From(builder, event, func(from *projections.FromBuilder[FluentAccount, Opened]) {
		projections.Value(from, projections.Path[FluentAccount, string]("state"), "first")
		projections.Value(from, projections.Path[FluentAccount, string]("state"), "second")
	})
	if _, err := builder.Build(); err == nil {
		t.Fatal("duplicate fluent writer accepted")
	}
	mixed := projections.NewBuilder("mixed", mustModel[Shadowed](t))
	projections.From(mixed, event, nil)
	if _, err := mixed.Build(); err == nil {
		t.Fatal("mixed declarations accepted")
	}
}

func TestInvalidFluentMappingsHaveRedactedProvenance(t *testing.T) {
	event := mustEvent[Opened](t)
	for _, test := range []struct {
		name   string
		define func(*projections.FromBuilder[FluentAccount, Opened])
	}{
		{"unknown target", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.Value(from, projections.Path[FluentAccount, string]("missing"), "secret")
		}},
		{"wrong target type", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.Value(from, projections.Path[FluentAccount, int]("name"), 42)
		}},
		{"unknown source", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.Map(from, projections.Path[FluentAccount, string]("name"), projections.Path[Opened, string]("missing"))
		}},
		{"wrong source type", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.Map(from, projections.Path[FluentAccount, string]("name"), projections.Path[Opened, string]("accountId"))
		}},
		{"unknown context", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.Context(from, projections.Path[FluentAccount, string]("name"), "missing")
		}},
		{"wrong context type", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.Context(from, projections.Path[FluentAccount, string]("name"), "occurred")
		}},
		{"unrepresentable literal", func(from *projections.FromBuilder[FluentAccount, Opened]) {
			projections.Value(from, projections.Path[FluentAccount, string]("name"), "secret,;$value(injected)")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := projections.NewBuilder("Example", mustModel[FluentAccount](t))
			projections.From(builder, event, test.define)
			_, err := builder.Build()
			var declaration *projections.DeclarationError
			if !errors.As(err, &declaration) || declaration.Artifact != "Example" || declaration.Path == "" || declaration.Offset != -1 || strings.Contains(err.Error(), "secret") {
				t.Fatalf("%+v", err)
			}
		})
	}
}

type BadNull struct {
	Name string `json:"name" chronicle:"value(Opened,value=null)"`
}
type BadNumber struct {
	Number int8 `json:"number" chronicle:"value(Opened,value=128)"`
}
type BadGeneration struct {
	Name string `json:"name" chronicle:"set(id(\"Opened\",9),from=fullName)"`
}
type BadName struct {
	Name string `json:"name" chronicle:"set(Missing)"`
}

func TestInvalidModelBoundDeclarationsFail(t *testing.T) {
	event := mustEvent[Opened](t)
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []projections.Declaration{
		projections.ModelBound(mustModel[BadNull](t)), projections.ModelBound(mustModel[BadNumber](t)), projections.ModelBound(mustModel[BadGeneration](t)), projections.ModelBound(mustModel[BadName](t)),
		projections.ModelBound(mustModel[FluentAccount](t), projections.FromEvent(event, projections.UsingConstantKey("invalid,key"))),
		projections.ModelBound(mustModel[FluentAccount](t), projections.FromEvent(event, projections.UsingKey(projections.Path[Opened, string]("missing")))),
		projections.ModelBound(mustModel[FluentAccount](t), projections.FromEvent(event, projections.UsingConstantKey("total")), projections.Passive()),
	} {
		_, err := projections.Compile(d, catalog)
		var declaration *projections.DeclarationError
		if !errors.As(err, &declaration) {
			t.Fatalf("expected declaration error: %v", err)
		}
	}
}

func TestFluentBuildSnapshotsValuesAndCallbacks(t *testing.T) {
	event := mustEvent[Opened](t)
	builder := projections.NewBuilder("snapshot", mustModel[FluentAccount](t))
	value := "before"
	var retained *projections.FromBuilder[FluentAccount, Opened]
	projections.From(builder, event, func(from *projections.FromBuilder[FluentAccount, Opened]) {
		retained = from
		projections.Value(from, projections.Path[FluentAccount, *string]("note"), &value)
	})
	value = "after"
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	projections.Value(retained, projections.Path[FluentAccount, string]("name"), "later")
	before := mustCompile(t, declaration, event.Descriptor()).KernelDefinition()
	if before.From[0].Value.Properties["note"] != "$value(before)" || len(before.From[0].Value.Properties) != 1 {
		t.Fatal("literal or callback escaped snapshot")
	}
}
