//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type ContextAuditStamped struct {
	Labels     []events.Tag       `json:"labels"`
	Attributes *map[string]string `json:"attributes"`
	Marker     string             `json:"marker"`
}
type ContextAuditCleared struct {
	Marker string `json:"marker"`
}
type ContextAuditInitialized struct {
	Marker string `json:"marker"`
}
type ContextAudit struct {
	ID         uuid.UUID          `json:"id" chronicle:"key"`
	Labels     *[]events.Tag      `json:"labels" chronicle:"no-auto;context(ContextAuditStamped,from=Tags);clear(ContextAuditCleared)"`
	Attributes *map[string]string `json:"attributes" chronicle:"not-projected;set(ContextAuditStamped,from=attributes);clear(ContextAuditCleared)"`
	CorrID     uuid.UUID          `json:"corrId" chronicle:"context(ContextAuditStamped,from=correlationId)"`
	Marker     string             `json:"marker" chronicle:"set(ContextAuditInitialized)"`
}
type ContextFluentAudit struct {
	ID         uuid.UUID          `json:"id" chronicle:"key"`
	Labels     *[]events.Tag      `json:"labels" chronicle:"no-auto"`
	Attributes *map[string]string `json:"attributes" chronicle:"not-projected"`
	CorrID     uuid.UUID          `json:"corrId"`
	Marker     string             `json:"marker"`
}

func TestKernelProjectionContextCollectionClear(t *testing.T) {
	f := newKernelFixture(t)
	// This ordinary Go pointer-container profile follows C# typed nullability,
	// not the stronger promise of present raw JSON null. No read overlay is used.
	registry := chronicle.NewRegistry()
	stamped, err := chronicle.RegisterEvent[ContextAuditStamped](registry)
	if err != nil {
		t.Fatal(err)
	}
	cleared, err := chronicle.RegisterEvent[ContextAuditCleared](registry)
	if err != nil {
		t.Fatal(err)
	}
	initialized, err := chronicle.RegisterEvent[ContextAuditInitialized](registry)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := chronicle.RegisterReadModel[ContextAudit](registry)
	if err != nil {
		t.Fatal(err)
	}
	fluent, err := chronicle.RegisterReadModel[ContextFluentAudit](registry)
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("context-fluent", fluent)
	projections.From(builder, stamped, func(from *projections.FromBuilder[ContextFluentAudit, ContextAuditStamped]) {
		projections.Context(from, projections.Path[ContextFluentAudit, *[]events.Tag]("labels"), "Tags")
		projections.Map(from, projections.Path[ContextFluentAudit, *map[string]string]("attributes"), projections.Path[ContextAuditStamped, *map[string]string]("attributes"))
		projections.Context(from, projections.Path[ContextFluentAudit, uuid.UUID]("corrId"), "correlationId")
	})
	projections.From(builder, cleared, func(from *projections.FromBuilder[ContextFluentAudit, ContextAuditCleared]) {
		projections.Clear(from, projections.Path[ContextFluentAudit, *[]events.Tag]("labels"))
		projections.Clear(from, projections.Path[ContextFluentAudit, *map[string]string]("attributes"))
	})
	projections.From(builder, initialized, func(from *projections.FromBuilder[ContextFluentAudit, ContextAuditInitialized]) {
		projections.Map(from, projections.Path[ContextFluentAudit, string]("marker"), projections.Path[ContextAuditInitialized, string]("marker"))
	})
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(stamped.Descriptor(), cleared.Descriptor(), initialized.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	var previous proto.Message
	for _, decl := range []projections.Declaration{projections.ModelBound(bound), declaration} {
		definition, err := projections.Compile(decl, catalog)
		if err != nil {
			t.Fatal(err)
		}
		wire := definition.KernelDefinition()
		if wire.InitialModelState != "{}" {
			t.Fatal("default request changed")
		}
		wire.Identifier, wire.ReadModel = "same-projection", "same-model"
		if previous != nil && !proto.Equal(previous, wire) {
			t.Fatalf("startup front ends differ: %v / %v", previous, wire)
		}
		previous = wire
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source, correlation := uuid.New(), uuid.New()
	ids := []readmodels.Identifier{bound.Identifier(), fluent.Identifier()}
	for _, id := range ids {
		instance, err := store.ReadModels().Get(f.ctx, id, readmodels.Key(source.String()))
		if err != nil || instance.Exists {
			t.Fatalf("registration created an instance: %+v %v", instance, err)
		}
	}
	// Registration absence and the actual initial array seed are separate from
	// clear-state evidence: the kernel seeds labels=[] only when creating a row.
	appendSuccessfully(t, f.ctx, store, events.SourceID(source.String()), ContextAuditInitialized{Marker: "initial-array"})
	initialLeft := awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), bound), readmodels.Key(source.String()), func(v ContextAudit) bool { return v.Marker == "initial-array" })
	initialRight := awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), fluent), readmodels.Key(source.String()), func(v ContextFluentAudit) bool { return v.Marker == "initial-array" })
	if !initialLeft.Exists || !initialRight.Exists || initialLeft.Value.ID != source || initialRight.Value.ID != source || initialLeft.Value.Labels == nil || initialRight.Value.Labels == nil || len(*initialLeft.Value.Labels) != 0 || len(*initialRight.Value.Labels) != 0 {
		t.Fatal("initial array seed was not observed")
	}
	for _, id := range ids {
		raw, err := store.ReadModels().Get(f.ctx, id, readmodels.Key(source.String()))
		if err != nil || !raw.Exists {
			t.Fatalf("initial raw instance: %v %v", raw.Exists, err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw.Value, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["labels"]) != "[]" || string(fields["marker"]) != `"initial-array"` {
			t.Fatalf("initial seed/marker: %s", raw.Value)
		}
		t.Logf("initial-array %s raw=%s", id, raw.Value)
	}
	for _, step := range []struct {
		marker     string
		tags       []events.Tag
		attributes map[string]string
		clear      bool
	}{
		{"nonempty", []events.Tag{"second", "first"}, map[string]string{"source": "payload"}, false},
		{"empty", []events.Tag{}, map[string]string{}, false},
		{"clear", nil, nil, true},
		{"clear-again", nil, nil, true},
		{"rewrite", []events.Tag{"new", "ordered"}, map[string]string{"source": "rewrite"}, false},
	} {
		t.Run(step.marker, func(t *testing.T) {
			if step.clear {
				appendSuccessfully(t, f.ctx, store, events.SourceID(source.String()), ContextAuditCleared{Marker: step.marker})
			} else {
				appendSuccessfully(t, f.ctx, store, events.SourceID(source.String()), ContextAuditStamped{Labels: []events.Tag{"wrong-payload"}, Attributes: &step.attributes, Marker: step.marker}, eventsequences.WithTags(step.tags...), eventsequences.WithCorrelation(metadata.CorrelationID(correlation)))
			}
			// Each marker proves this event was processed, including the empty case.
			left := awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), bound), readmodels.Key(source.String()), func(v ContextAudit) bool { return v.Marker == step.marker })
			right := awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), fluent), readmodels.Key(source.String()), func(v ContextFluentAudit) bool { return v.Marker == step.marker })
			if !left.Exists || !right.Exists || left.Value.ID != source || right.Value.ID != source || left.Value.CorrID != correlation || right.Value.CorrID != correlation {
				t.Fatal("identity/correlation/document lost")
			}
			if step.clear {
				// No schema/default restoration after either processed clear marker.
				if left.Value.Labels != nil || left.Value.Attributes != nil || right.Value.Labels != nil || right.Value.Attributes != nil {
					t.Fatalf("clear restored a default/stale value: %+v / %+v", left.Value, right.Value)
				}
			} else if left.Value.Labels == nil || left.Value.Attributes == nil || !reflect.DeepEqual(*left.Value.Labels, step.tags) || !reflect.DeepEqual(*left.Value.Attributes, step.attributes) || !reflect.DeepEqual(left.Value.Labels, right.Value.Labels) || !reflect.DeepEqual(left.Value.Attributes, right.Value.Attributes) {
				t.Fatalf("context/payload values = %+v / %+v", left.Value, right.Value)
			}
			for _, id := range ids {
				raw, err := store.ReadModels().Get(f.ctx, id, readmodels.Key(source.String()))
				if err != nil || !raw.Exists {
					t.Fatalf("raw instance: %v %v", raw.Exists, err)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(raw.Value, &fields); err != nil {
					t.Fatal(err)
				}
				var retained struct {
					ID     uuid.UUID `json:"id"`
					CorrID uuid.UUID `json:"corrId"`
					Marker string    `json:"marker"`
				}
				if err := json.Unmarshal(raw.Value, &retained); err != nil || retained.ID != source || retained.CorrID != correlation || retained.Marker != step.marker {
					t.Fatalf("raw identity/correlation/processed marker lost: %s %v", raw.Value, err)
				}
				t.Logf("%s %s raw=%s", step.marker, id, raw.Value)
				for _, path := range []string{"id", "corrId", "marker"} {
					if _, present := fields[path]; !present {
						t.Fatalf("missing %s in %s", path, raw.Value)
					}
				}
				if step.clear {
					for _, path := range []string{"labels", "attributes"} {
						if value, present := fields[path]; present && string(value) != "null" {
							t.Fatalf("clear requires omitted or null %s, got %s", path, raw.Value)
						}
					}
					continue
				}
				labels, err := json.Marshal(step.tags)
				if err != nil {
					t.Fatal(err)
				}
				attributes, err := json.Marshal(step.attributes)
				if err != nil {
					t.Fatal(err)
				}
				if string(fields["labels"]) != string(labels) || string(fields["attributes"]) != string(attributes) {
					t.Fatalf("raw %s want labels=%s attributes=%s", raw.Value, labels, attributes)
				}
			}
		})
	}
}

type ContextRuntimeStamped struct {
	Marker string `json:"marker"`
}
type ContextRuntimeCleared struct {
	Marker string `json:"marker"`
}
type ContextRuntimeScalar struct {
	ID     uuid.UUID `json:"id" chronicle:"key"`
	CorrID uuid.UUID `json:"corrId" chronicle:"context(ContextRuntimeStamped,from=correlationId)"`
	Note   *string   `json:"note" chronicle:"no-auto;value(ContextRuntimeStamped,value=\"present\");clear(ContextRuntimeCleared)"`
	Marker string    `json:"marker"`
}

func TestKernelProjectionContextClearRuntimeAdmission(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ContextRuntimeStamped](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ContextRuntimeCleared](registry); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := readmodels.Define[ContextAudit]()
	if err != nil {
		t.Fatal(err)
	}
	before := len(store.ReadModels().Catalog().Descriptors())
	refused, err := store.RegisterProjection(f.ctx, projections.ModelBound(collection))
	if !errors.Is(err, chronicle.ErrUnsupported) || refused.Published || len(store.ReadModels().Catalog().Descriptors()) != before {
		t.Fatalf("runtime collection admission widened: %+v %v", refused, err)
	}
	var calls atomic.Int32
	model, err := readmodels.Define[ContextRuntimeScalar](readmodels.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		calls.Add(1)
		return compliance.Classification{}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	frozen := calls.Load()
	registered, err := store.RegisterProjection(f.ctx, projections.ModelBound(model))
	if err != nil || !registered.Published || !registered.Outcome.IsSuccess() {
		t.Fatalf("runtime scalar: %+v %v", registered, err)
	}
	source, correlation := uuid.New(), uuid.New()
	appendSuccessfully(t, f.ctx, store, events.SourceID(source.String()), ContextRuntimeStamped{Marker: "runtime-stamp"}, eventsequences.WithCorrelation(metadata.CorrelationID(correlation)))
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), model), readmodels.Key(source.String()), func(v ContextRuntimeScalar) bool {
		return v.Marker == "runtime-stamp" && v.Note != nil && v.CorrID == correlation
	})
	appendSuccessfully(t, f.ctx, store, events.SourceID(source.String()), ContextRuntimeCleared{Marker: "runtime-clear"})
	instance := awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), model), readmodels.Key(source.String()), func(v ContextRuntimeScalar) bool { return v.Marker == "runtime-clear" })
	if !instance.Exists || instance.Value.Note != nil || instance.Value.ID != source || instance.Value.CorrID != correlation || calls.Load() != frozen {
		t.Fatal("runtime scalar clear/frozen metadata regressed")
	}
	// Preserve existing scalar clear semantics; do not turn an absent wire property
	// into an explicit null, or advertise raw-null preservation for this route.
	raw, err := store.ReadModels().Get(f.ctx, model.Identifier(), readmodels.Key(source.String()))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw.Value, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["note"]; present {
		t.Fatalf("existing scalar read omission changed: %s", raw.Value)
	}
}
