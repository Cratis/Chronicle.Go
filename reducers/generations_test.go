// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers_test

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type FoldedGeneration struct{ Value string }
type PreviousFoldedGeneration FoldedGeneration
type GenerationState struct{ Value string }
type HistoricalFold struct{}

func (HistoricalFold) Fold(PreviousFoldedGeneration, *GenerationState) *GenerationState {
	panic("must not run")
}

// The kernel releases protected values in every generation, so protected historical
// observers are admitted like plain ones (Cratis/Chronicle#4623).
func TestReducerGenerationAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                                            string
		currentProtected, previousProtected, historical bool
	}{
		{"protected historical", true, true, true},
		{"current protection alone", true, false, true},
		{"historical protection alone", false, true, true},
		{"plain historical", false, false, true},
		{"protected current", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := reducerGenerationCatalog(t, tc.currentProtected, tc.previousProtected)
			model, models := generationModels(t)
			handler := reducers.On(func(context.Context, FoldedGeneration, *GenerationState, events.Context) (*GenerationState, error) {
				panic("must not run")
			})
			if tc.historical {
				handler = reducers.On(func(context.Context, PreviousFoldedGeneration, *GenerationState, events.Context) (*GenerationState, error) {
					panic("must not run")
				})
			}
			declaration, err := reducers.DefineHandlers(model, "generation", []reducers.Handler{handler})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := reducers.Compile(declaration, catalog, models, nil)
			if err != nil || plan == nil {
				t.Fatalf("compile = %v, %v", plan, err)
			}
		})
	}
}

func TestReducerHistoricalDiscoveryAndBatchAreAdmittedForProtectedEvents(t *testing.T) {
	catalog := reducerGenerationCatalog(t, true, false)
	model, models := generationModels(t)
	factory := func() HistoricalFold { t.Fatal("constructor called"); return HistoricalFold{} }
	discovered, err := reducers.Define[HistoricalFold](model, factory, reducers.WithID("discovered"))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := reducers.DefineHandlers(model, "batch", []reducers.Handler{reducers.OnBatch(func(context.Context, *observation.EventBatch, *GenerationState, observation.BatchContext) (*GenerationState, error) {
		t.Fatal("batch called")
		return nil, nil
	}, observation.Observes[PreviousFoldedGeneration]())})
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range []reducers.Declaration{discovered, batch} {
		t.Run(string(declaration.Identifier()), func(t *testing.T) {
			plan, err := reducers.Compile(declaration, catalog, models, nil)
			if err != nil || plan == nil {
				t.Fatalf("compile = %v, %v", plan, err)
			}
		})
	}
}

func reducerGenerationCatalog(t *testing.T, currentProtected, previousProtected bool) *events.Catalog {
	t.Helper()
	current, err := events.Define[FoldedGeneration](events.WithGeneration(2), events.WithProtection(compliance.Property("Value", compliance.Classification{PII: currentProtected})))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[PreviousFoldedGeneration](current, 1, events.WithProtection(compliance.Property("Value", compliance.Classification{Encrypted: previousProtected})))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(previous.Descriptor(), current.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func generationModels(t *testing.T) (readmodels.Model[GenerationState], *readmodels.Catalog) {
	t.Helper()
	model, err := readmodels.Define[GenerationState]()
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return model, models
}
