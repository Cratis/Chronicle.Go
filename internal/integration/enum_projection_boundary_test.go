//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type boundaryStatus int32
type BoundaryEntered struct {
	Status boundaryStatus `json:"status"`
	Ready  bool
}
type BoundaryUpdated struct {
	Status boundaryStatus `json:"sTaTuS"`
	Ready  bool
}
type boundaryCaseView struct {
	ID     string `json:"id"`
	Status boundaryStatus
	Ready  bool
}
type boundaryJoinView boundaryCaseView
type boundaryVariantView boundaryCaseView
type boundaryVariantGroup struct{}

// The event and model names differ deliberately. This exercises the packaged
// kernel's OrdinalIgnoreCase selection, not merely the SDK's compiled metadata.
func TestKernelEnumProjectionCaseAndLoweredVariants(t *testing.T) {
	f := newKernelFixture(t)
	f.storeName = chronicle.StoreName("eb-" + uuid.NewString()[:8])
	codecs, err := serialization.NewCodecs(serialization.Enum(
		serialization.EnumMember[boundaryStatus]{Name: "Zero", Value: 0},
		serialization.EnumMember[boundaryStatus]{Name: "One", Value: 1},
	))
	if err != nil {
		t.Fatal(err)
	}
	r := chronicle.NewRegistry()
	entered, err := chronicle.RegisterEvent[BoundaryEntered](r, events.WithID("ebe"), events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := chronicle.RegisterEvent[BoundaryUpdated](r, events.WithID("ebu"), events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := chronicle.RegisterReadModel[boundaryCaseView](r, readmodels.WithIdentifier("ebc"), readmodels.WithContainerName("ebc"), readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	joined, err := chronicle.RegisterReadModel[boundaryJoinView](r, readmodels.WithIdentifier("ebj"), readmodels.WithContainerName("ebj"), readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	variant, err := chronicle.RegisterReadModel[boundaryVariantView](r, readmodels.WithIdentifier("ebv"), readmodels.WithContainerName("ebv"), readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("ebc", plain)
	projections.From(b, entered, nil)
	addBoundaryProjection(t, r, b)
	j := projections.NewBuilder("ebj", joined)
	projections.From(j, entered, nil)
	projections.Join(j, updated, projections.Path[boundaryJoinView, string]("id"), nil)
	addBoundaryProjection(t, r, j)
	v := projections.NewBuilder("ebv", variant, projections.VariantOf[boundaryVariantGroup](), projections.VariantKey(projections.Path[boundaryVariantView, string]("id")), projections.EntersOn(entered))
	projections.From(v, updated, nil)
	addBoundaryProjection(t, r, v)
	store, err := f.client(r, chronicle.WithNamingPolicy(serialization.PreservePropertyNames)).EventStore(f.ctx, f.storeName, chronicle.WithNamespace("eb"))
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, f.ctx, store, "subject", BoundaryEntered{Status: 1})
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), plain), "subject", func(v boundaryCaseView) bool { return v.ID == "subject" && v.Status == 1 && !v.Ready })
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), joined), "subject", func(v boundaryJoinView) bool { return v.ID == "subject" && v.Status == 1 && !v.Ready })
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), variant), "subject", func(v boundaryVariantView) bool { return v.ID == "subject" && v.Status == 1 && !v.Ready })
	appendSuccessfully(t, f.ctx, store, "subject", BoundaryUpdated{Status: 0, Ready: true})
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), joined), "subject", func(v boundaryJoinView) bool { return v.ID == "subject" && v.Status == 0 && v.Ready })
	awaitProjection(t, f.ctx, readmodels.For(store.ReadModels(), variant), "subject", func(v boundaryVariantView) bool { return v.ID == "subject" && v.Status == 0 && v.Ready })
	for _, model := range []struct {
		id     readmodels.Identifier
		status string
		ready  bool
	}{{"ebc", "One", false}, {"ebj", "Zero", true}, {"ebv", "Zero", true}} {
		raw, err := store.ReadModels().Get(f.ctx, model.id, "subject")
		if err != nil || !raw.Exists {
			t.Fatalf("raw model %s: %v", model.id, err)
		}
		var value struct {
			ID     string `json:"id"`
			Status string
			Ready  bool
		}
		if err := json.Unmarshal(raw.Value, &value); err != nil || value.ID != "subject" || value.Status != model.status || value.Ready != model.ready {
			t.Fatalf("raw model %s: %s; %v", model.id, raw.Value, err)
		}
		t.Logf("kernel OrdinalIgnoreCase model %s: %s", model.id, raw.Value)
	}
}

func addBoundaryProjection[M any](t *testing.T, registry *chronicle.Registry, builder *projections.Builder[M]) {
	t.Helper()
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
}
