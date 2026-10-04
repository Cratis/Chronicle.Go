// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"os"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type AuditStamped struct {
	Labels     []events.Tag       `json:"labels"`
	Attributes *map[string]string `json:"attributes"`
	Marker     string             `json:"marker"`
}
type AuditCleared struct {
	Marker string `json:"marker"`
}
type AuditID struct {
	ID         uuid.UUID          `json:"id" chronicle:"key"`
	Labels     *[]events.Tag      `json:"labels" chronicle:"no-auto;context(AuditStamped,from=Tags);clear(AuditCleared)"`
	Attributes *map[string]string `json:"attributes" chronicle:"not-projected;set(AuditStamped,from=attributes);clear(AuditCleared)"`
	CorrID     uuid.UUID          `json:"corrId" chronicle:"context(AuditStamped,from=correlationId)"`
	Marker     string             `json:"marker"`
}
type ContextAuditOnly struct {
	ID         uuid.UUID          `json:"id" chronicle:"key"`
	Labels     *[]events.Tag      `json:"labels" chronicle:"no-auto;context(AuditStamped,from=Tags)"`
	Attributes *map[string]string `json:"attributes" chronicle:"not-projected;set(AuditStamped,from=attributes)"`
	CorrID     uuid.UUID          `json:"corrId" chronicle:"context(AuditStamped,from=correlationId)"`
	Marker     string             `json:"marker"`
}

type FluentAudit struct {
	ID         uuid.UUID          `json:"id" chronicle:"key"`
	Labels     *[]events.Tag      `json:"labels" chronicle:"no-auto"`
	Attributes *map[string]string `json:"attributes" chronicle:"not-projected"`
	CorrID     uuid.UUID          `json:"corrId"`
	Marker     string             `json:"marker"`
}

func auditDeclarations(t *testing.T) (projections.Definition, projections.Definition) {
	t.Helper()
	stamped := mustEvent[AuditStamped](t, events.WithID("audit-stamped"))
	catalog, err := events.NewCatalog(stamped.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	bound, err := projections.Compile(projections.ModelBound(mustModel[ContextAuditOnly](t, readmodels.WithIdentifier("Example.Audit")), projections.WithIdentifier("Example.AuditProjection")), catalog)
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("Example.AuditProjection", mustModel[FluentAudit](t, readmodels.WithIdentifier("Example.Audit")))
	projections.From(b, stamped, func(f *projections.FromBuilder[FluentAudit, AuditStamped]) {
		projections.Context(f, projections.Path[FluentAudit, *[]events.Tag]("labels"), "Tags")
		projections.Map(f, projections.Path[FluentAudit, *map[string]string]("attributes"), projections.Path[AuditStamped, *map[string]string]("attributes"))
		projections.Context(f, projections.Path[FluentAudit, uuid.UUID]("corrId"), "correlationId")
	})
	decl, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	fluent, err := projections.Compile(decl, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return bound, fluent
}

// The fixture is hand-derived from C# 2e31b0d, not captured by executing .NET.
func TestContextCollectionFrontEndsGolden(t *testing.T) {
	bound, fluent := auditDeclarations(t)
	data, err := os.ReadFile("testdata/context-clear.json")
	if err != nil {
		t.Fatal(err)
	}
	want := &contracts.ProjectionDefinition{}
	if err := protojson.Unmarshal(data, want); err != nil {
		t.Fatal(err)
	}
	for _, definition := range []projections.Definition{bound, fluent} {
		if !proto.Equal(definition.KernelDefinition(), want) {
			t.Fatalf("got %s\nwant %s", protojson.Format(definition.KernelDefinition()), protojson.Format(want))
		}
	}
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[AuditStamped](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[AuditCleared](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterReadModel[ContextAuditOnly](registry); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

type ClearValue struct {
	Labels     *[]events.Tag      `json:"labels" chronicle:"value(AuditCleared,value=null)"`
	Attributes *map[string]string `json:"attributes" chronicle:"value(AuditCleared,value=null)"`
}

func assertCollectionClearRefusal(t *testing.T, err error, path, directive string) {
	t.Helper()
	var located *projections.DeclarationError
	if !errors.Is(err, chronicle.ErrUnsupported) || !errors.As(err, &located) || located.Path != path || located.Directive != directive {
		t.Fatalf("expected located unsupported collection clear: %v", err)
	}
}

func TestCollectionNullFormsRefuseBeforeRegistration(t *testing.T) {
	event := mustEvent[AuditCleared](t)
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = projections.Compile(projections.ModelBound(mustModel[ClearValue](t)), catalog)
	assertCollectionClearRefusal(t, err, "labels", "value")
	for _, path := range []string{"labels", "attributes"} {
		for _, value := range []bool{false, true} {
			b := projections.NewBuilder("null", mustModel[FluentAudit](t))
			projections.From(b, event, func(f *projections.FromBuilder[FluentAudit, AuditCleared]) {
				if path == "labels" {
					if value {
						projections.Value(f, projections.Path[FluentAudit, *[]events.Tag](path), nil)
					} else {
						projections.Clear(f, projections.Path[FluentAudit, *[]events.Tag](path))
					}
				} else {
					if value {
						projections.Value(f, projections.Path[FluentAudit, *map[string]string](path), nil)
					} else {
						projections.Clear(f, projections.Path[FluentAudit, *map[string]string](path))
					}
				}
			})
			_, err := b.Build()
			directive := "clear"
			if value {
				directive = "value"
			}
			assertCollectionClearRefusal(t, err, path, directive)
		}
	}
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[AuditStamped](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[AuditCleared](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterReadModel[AuditID](registry); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if client != nil {
		_ = client.Close()
		t.Fatal("unsupported declaration published a client")
	}
	assertCollectionClearRefusal(t, err, "labels", "clear")
}

type GlobalTags struct {
	Labels *[]events.Tag `json:"labels" chronicle:"no-auto;every(context=Tags)"`
}
type AllTags struct {
	Labels []string `json:"labels" chronicle:"no-auto;all(context=tags)"`
}

func TestContextCollectionEveryAndAll(t *testing.T) {
	event := mustEvent[AuditStamped](t)
	for _, all := range []bool{false, true} {
		var bound projections.Definition
		if all {
			bound = mustCompile(t, projections.ModelBound(mustModel[AllTags](t), projections.FromEvent(event)), event.Descriptor())
		} else {
			bound = mustCompile(t, projections.ModelBound(mustModel[GlobalTags](t), projections.FromEvent(event)), event.Descriptor())
		}
		b := projections.NewBuilder("global", mustModel[FluentAudit](t))
		projections.From(b, event, nil)
		define := func(g *projections.EveryBuilder[FluentAudit]) {
			projections.EveryContext(g, projections.Path[FluentAudit, *[]events.Tag]("labels"), map[bool]string{false: "Tags", true: "tags"}[all])
		}
		if all {
			projections.All(b, define)
		} else {
			projections.Every(b, define)
		}
		decl, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		fluent := mustCompile(t, decl, event.Descriptor()).KernelDefinition()
		if !proto.Equal(bound.KernelDefinition().All, fluent.All) || bound.KernelDefinition().SubscribesToAllEvents != all || fluent.SubscribesToAllEvents != all || len(fluent.From) != 1 {
			t.Fatal("global context or subscriptions differ")
		}
	}
}

type contextTarget[V any] struct {
	Value V `json:"value" chronicle:"context(AuditCleared,from=Tags)"`
}
type nullTarget[V any] struct {
	Value V `json:"value" chronicle:"clear(AuditCleared)"`
}

func checkContextNullTarget[V any](t *testing.T, contextOK, nullOK bool) {
	t.Helper()
	event := mustEvent[AuditCleared](t)
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		declaration projections.Declaration
		want        bool
	}{
		{projections.ModelBound(mustModel[contextTarget[V]](t, readmodels.WithIdentifier("context-target"), readmodels.WithContainerName("context-target"))), contextOK},
		{projections.ModelBound(mustModel[nullTarget[V]](t, readmodels.WithIdentifier("null-target"), readmodels.WithContainerName("null-target"))), nullOK},
	} {
		_, err := projections.Compile(tc.declaration, catalog)
		if (err == nil) != tc.want {
			t.Fatalf("error=%v want acceptance=%v", err, tc.want)
		}
	}
}
func TestContextCollectionTargetAndNullBoundaries(t *testing.T) {
	t.Run("strings", func(t *testing.T) { checkContextNullTarget[*[]string](t, true, false) })
	t.Run("numbers", func(t *testing.T) { checkContextNullTarget[*[]int](t, false, false) })
	t.Run("uuids", func(t *testing.T) { checkContextNullTarget[*[]uuid.UUID](t, false, false) })
	t.Run("optional elements", func(t *testing.T) { checkContextNullTarget[*[]*string](t, false, false) })
	t.Run("dates", func(t *testing.T) { checkContextNullTarget[*[]time.Time](t, false, false) })
	t.Run("array", func(t *testing.T) { checkContextNullTarget[*[2]string](t, false, false) })
	t.Run("map", func(t *testing.T) { checkContextNullTarget[*map[string]string](t, false, false) })
	t.Run("slice", func(t *testing.T) { checkContextNullTarget[[]string](t, true, false) })
	t.Run("bare map", func(t *testing.T) { checkContextNullTarget[map[string]string](t, false, false) })
	t.Run("scalar", func(t *testing.T) { checkContextNullTarget[*uuid.UUID](t, false, true) })
	t.Run("object", func(t *testing.T) { checkContextNullTarget[*struct{ Name string }](t, false, false) })
}

type contextNamingModel struct {
	ID     uuid.UUID     `chronicle:"key"`
	Labels *[]events.Tag `chronicle:"no-auto"`
	CorrID uuid.UUID     `json:"customCorr"`
}

func TestContextCollectionNamingTypedPathsAndFrozenCallback(t *testing.T) {
	event := mustEvent[AuditStamped](t)
	model := mustModel[contextNamingModel](t)
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	b := projections.NewBuilder("naming", model)
	projections.From(b, event, func(f *projections.FromBuilder[contextNamingModel, AuditStamped]) {
		calls++
		projections.Context(f, projections.Path[contextNamingModel, *[]events.Tag]("Labels"), "Tags")
		projections.Context(f, projections.Path[contextNamingModel, uuid.UUID]("customCorr"), "correlationId")
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
		model, err := definition.Model().WithNamingPolicy(policy)
		if err != nil {
			t.Fatal(err)
		}
		rebound, err := definition.Rebind(model, catalog, catalog)
		if err != nil {
			t.Fatal(err)
		}
		wire := rebound.KernelDefinition()
		path, id := "Labels", "Id"
		if policy == serialization.CamelCase {
			path, id = "labels", "id"
		}
		if rebound.KeyField() != id || wire.From[0].Value.Properties[path] != "$eventContext(Tags)" || wire.From[0].Value.Properties["customCorr"] != "$eventContext(correlationId)" {
			t.Fatalf("rebound names/context: %s", protojson.Format(wire))
		}
	}
	if calls != 1 {
		t.Fatal("compilation repeated authoring callback")
	}
	for _, bad := range []func(*projections.FromBuilder[contextNamingModel, AuditStamped]){
		func(f *projections.FromBuilder[contextNamingModel, AuditStamped]) {
			projections.Context(f, projections.Path[contextNamingModel, *[]events.Tag]("labels"), "Tags")
		},
		func(f *projections.FromBuilder[contextNamingModel, AuditStamped]) {
			projections.Context(f, projections.Path[contextNamingModel, *[]string]("Labels"), "Tags")
		},
	} {
		b := projections.NewBuilder("wrong", model)
		projections.From(b, event, bad)
		if _, err := b.Build(); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("wrong typed/path target accepted: %v", err)
		}
	}
}

func TestComplexContextRefusalAndScalarOnlyKeys(t *testing.T) {
	event := mustEvent[AuditStamped](t)
	for _, path := range []string{"CausedBy", "causedBy", "EventType", "eventType", "NamedTags", "namedTags", "Causation", "causation", "invented", "Tags.value", "Tags[0]", "Tags()"} {
		t.Run(path, func(t *testing.T) {
			b := projections.NewBuilder("unsupported", mustModel[FluentAudit](t))
			projections.From(b, event, func(f *projections.FromBuilder[FluentAudit, AuditStamped]) {
				projections.Context(f, projections.Path[FluentAudit, *[]events.Tag]("labels"), path)
			})
			_, err := b.Build()
			var located *projections.DeclarationError
			if !errors.As(err, &located) || located.Path != "labels" || located.Directive != "context" {
				t.Fatalf("unlocated failure: %v", err)
			}
			known := path == "CausedBy" || path == "causedBy" || path == "EventType" || path == "eventType" || path == "NamedTags" || path == "namedTags" || path == "Causation" || path == "causation"
			if known && !errors.Is(err, chronicle.ErrUnsupported) || !known && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("wrong identity: %v", err)
			}
		})
	}
	type key struct {
		Labels string `json:"labels"`
	}
	for _, option := range []projections.FromOption{projections.UsingKeyFromContext("Tags"), projections.UsingParentKeyFromContext("tags"), projections.UsingCompositeKey(func(k *projections.CompositeKeyBuilder[key, AuditStamped]) {
		projections.KeyPartFromContext(k, projections.Path[key, string]("labels"), "Tags")
	})} {
		b := projections.NewBuilder("keys", mustModel[FluentAudit](t))
		projections.From(b, event, nil, option)
		if _, err := b.Build(); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("collection key accepted: %v", err)
		}
	}
}
