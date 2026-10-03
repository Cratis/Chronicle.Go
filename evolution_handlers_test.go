// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type historicalReactor struct{ handled *string }

func (r *historicalReactor) Observe(e evolutionV1) { *r.handled = e.FullName }

type historicalModel struct {
	ID   string
	Name string
}
type historicalReducer struct{}

func (*historicalReducer) Fold(e evolutionV1, _ *historicalModel) historicalModel {
	return historicalModel{Name: e.FullName}
}

func TestHistoricalConcreteHandlersKeepTheirOwnExpansion(t *testing.T) {
	registry, _, old := evolutionRegistry(t)
	catalog := catalogFor(t, registry)
	model, err := readmodels.Define[historicalModel]()
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	handled := ""
	reactor, err := reactors.Define[*historicalReactor](func() *historicalReactor { return &historicalReactor{&handled} })
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reactors.Compile(reactor, catalog, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if refs := plan.EventTypes(); len(refs) != 1 || refs[0] != old.Ref() {
		t.Fatalf("historical subscription: %v", refs)
	}
	descriptor, ok := plan.Descriptor(old.Ref().ID)
	if !ok || descriptor.Ref() != old.Ref() {
		t.Fatal("historical codec was replaced by current")
	}
	ec, content, err := observerruntime.DecodeContent(descriptor, events.Context{EventType: events.TypeRef{ID: old.Ref().ID, Generation: 2}}, []byte(`{"FirstName":"wrong"}`), map[events.Generation]json.RawMessage{1: json.RawMessage(`{"full_name":"Ada"}`)})
	if err != nil || ec.EventType != old.Ref() {
		t.Fatalf("historical delivery context: %+v %v", ec, err)
	}
	lease, err := plan.Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Invoke(t.Context(), content, ec, nil); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if handled != "Ada" {
		t.Fatal(handled)
	}
	reducer, err := reducers.Define[*historicalReducer](model, func() *historicalReducer { return &historicalReducer{} })
	if err != nil {
		t.Fatal(err)
	}
	fold, err := reducers.Compile(reducer, catalog, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if refs := fold.EventTypes(); len(refs) != 1 || refs[0] != old.Ref() {
		t.Fatalf("historical fold subscription: %v", refs)
	}
	reducerDescriptor, ok := fold.Descriptor(old.Ref().ID)
	if !ok || reducerDescriptor.Ref() != old.Ref() {
		t.Fatal("wrong reducer codec")
	}
	fallback, decoded, err := observerruntime.DecodeContent(reducerDescriptor, events.Context{EventType: events.TypeRef{ID: old.Ref().ID, Generation: 2}}, []byte(`{"full_name":"raw fallback"}`), nil)
	if err != nil || fallback.EventType.Generation != 2 || decoded.(*evolutionV1).FullName != "raw fallback" {
		t.Fatalf("raw fallback: %v %v %v", fallback, decoded, err)
	}
}
