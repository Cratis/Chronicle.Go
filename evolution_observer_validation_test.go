// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type mixedGenerationReactor struct{}

func (*mixedGenerationReactor) Live(evolutionV2)   {}
func (*mixedGenerationReactor) Replay(evolutionV1) {}

type mixedGenerationReducer struct{}

func (*mixedGenerationReducer) Current(evolutionV2, *historicalModel) historicalModel {
	return historicalModel{}
}
func (*mixedGenerationReducer) Old(evolutionV1, *historicalModel) historicalModel {
	return historicalModel{}
}

func TestObserverRejectsMixedGenerationBindings(t *testing.T) {
	registry, _, _ := evolutionRegistry(t)
	catalog := catalogFor(t, registry)
	model, err := readmodels.Define[historicalModel]()
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	d, err := reactors.Define[*mixedGenerationReactor](func() *mixedGenerationReactor { return &mixedGenerationReactor{} }, reactors.Replay("Replay"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reactors.Compile(d, catalog, models, nil); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("mixed live/replay generations: %v", err)
	}
	explicit, err := reactors.DefineHandlers("mixed", []reactors.Handler{reactors.On(func(context.Context, evolutionV2) error { return nil }), reactors.On(func(context.Context, evolutionV1) error { return nil }).DuringReplay()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reactors.Compile(explicit, catalog, models, nil); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("mixed callbacks: %v", err)
	}
	reducer, err := reducers.Define[*mixedGenerationReducer](model, func() *mixedGenerationReducer { return &mixedGenerationReducer{} })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reducers.Compile(reducer, catalog, models, nil); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("mixed reducer: %v", err)
	}
}
