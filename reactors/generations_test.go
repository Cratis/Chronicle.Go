// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

type ObservedGeneration struct{ Value string }
type PreviousObservedGeneration ObservedGeneration
type HistoricalObserver struct{}

func (HistoricalObserver) Observe(PreviousObservedGeneration) { panic("must not run") }

func TestReactorGenerationAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                                            string
		currentProtected, previousProtected, historical bool
		wantErr                                         error
	}{
		{"protected historical", true, true, true, faults.ErrUnsupported},
		{"current protection alone", true, false, true, faults.ErrUnsupported},
		{"historical protection alone", false, true, true, faults.ErrUnsupported},
		{"plain historical", false, false, true, nil},
		{"protected current", true, true, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := reactorGenerationCatalog(t, tc.currentProtected, tc.previousProtected)
			models, err := readmodels.NewCatalog()
			if err != nil {
				t.Fatal(err)
			}
			handler := reactors.On(func(context.Context, ObservedGeneration) error { panic("must not run") })
			if tc.historical {
				handler = reactors.On(func(context.Context, PreviousObservedGeneration) error { panic("must not run") })
			}
			declaration, err := reactors.DefineHandlers("generation", []reactors.Handler{handler})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := reactors.Compile(declaration, catalog, models, nil)
			assertReactorGenerationAdmission(t, plan, err, tc.wantErr)
		})
	}
}

func TestReactorHistoricalDiscoveryReplayAndBatchRefuseBeforeActivation(t *testing.T) {
	catalog := reactorGenerationCatalog(t, true, false)
	models, err := readmodels.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	factory := func() HistoricalObserver { t.Fatal("constructor called"); return HistoricalObserver{} }
	discovered, err := reactors.Define[HistoricalObserver](factory, reactors.WithID("discovered"))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := reactors.Define[HistoricalObserver](factory, reactors.WithID("replay"), reactors.Replay("Observe"))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := reactors.DefineHandlers("batch", []reactors.Handler{reactors.OnBatch(func(context.Context, *observation.EventBatch, observation.BatchContext) error {
		t.Fatal("batch called")
		return nil
	}, observation.Observes[PreviousObservedGeneration]())})
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range []reactors.Declaration{discovered, replay, batch} {
		t.Run(string(declaration.Identifier()), func(t *testing.T) {
			plan, err := reactors.Compile(declaration, catalog, models, nil)
			if !errors.Is(err, faults.ErrUnsupported) || plan != nil {
				t.Fatalf("compile = %v, %v", plan, err)
			}
		})
	}
}

func assertReactorGenerationAdmission(t *testing.T, plan *reactors.Plan, err, wantErr error) {
	t.Helper()
	if !errors.Is(err, wantErr) {
		t.Fatalf("compile = %v", err)
	}
	if wantErr == nil {
		return
	}
	if plan != nil {
		t.Fatalf("refused plan = %v", plan)
	}
	var detail *reactors.DeclarationError
	if !errors.As(err, &detail) {
		t.Fatalf("declaration error = %v", err)
	}
	if detail.Reactor != "generation" {
		t.Fatalf("reactor = %v", detail.Reactor)
	}
}

func reactorGenerationCatalog(t *testing.T, currentProtected, previousProtected bool) *events.Catalog {
	t.Helper()
	current, err := events.Define[ObservedGeneration](events.WithGeneration(2), events.WithProtection(compliance.Property("Value", compliance.Classification{PII: currentProtected})))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[PreviousObservedGeneration](current, 1, events.WithProtection(compliance.Property("Value", compliance.Classification{Encrypted: previousProtected})))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(previous.Descriptor(), current.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
