// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type AccountClosed struct{}
type auditMarker struct{}
type foldingFailure struct{}

var errFold = errors.New("fold failed")

type foldAccount struct{ numbers *[]events.SequenceNumber }

func (r *foldAccount) Open(event AccountOpened, current *Account, ec events.Context) *Account {
	*r.numbers = append(*r.numbers, ec.SequenceNumber)
	return &Account{Name: event.Name}
}
func (*foldAccount) Close(AccountClosed, *Account) *Account { return nil }
func (*foldAccount) Fail(foldingFailure, *Account) (*Account, error) {
	return &Account{Name: "partial"}, errFold
}
func reducerRegistry(t *testing.T) (*chronicle.Registry, readmodels.Model[Account], *[]events.SequenceNumber) {
	t.Helper()
	registry := eventRegistry(t)
	if _, err := chronicle.RegisterEvent[AccountClosed](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[auditMarker](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[foldingFailure](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[Account](registry)
	if err != nil {
		t.Fatal(err)
	}
	numbers := new([]events.SequenceNumber)
	if err := chronicle.RegisterReducer[*foldAccount](registry, model, func() *foldAccount { return &foldAccount{numbers: numbers} }); err != nil {
		t.Fatal(err)
	}
	return registry, model, numbers
}
func TestReadModelScenarioFoldsRegisteredConventionsAndSelectsInstances(t *testing.T) {
	registry, _, numbers := reducerRegistry(t)
	s := chronicletest.NewReadModelScenario[Account](t, chronicletest.Config{Registry: registry})
	if err := s.Given(t.Context(), "a", AccountOpened{Name: "Ada"}, auditMarker{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(t.Context(), "b", AccountOpened{Name: "Grace"}); err != nil {
		t.Fatal(err)
	}
	instances, err := s.Instances(t.Context())
	if err != nil || len(instances) != 2 {
		t.Fatalf("instances: %+v %v", instances, err)
	}
	if instances["a"].Name != "Ada" || instances["b"].Name != "Grace" {
		t.Fatal(instances)
	}
	if len(*numbers) != 2 || (*numbers)[0] != 0 || (*numbers)[1] != 2 {
		t.Fatalf("synthetic sequence numbers: %v", *numbers)
	}
	if _, err = s.Instance(t.Context()); !errors.Is(err, chronicletest.ErrAmbiguousInstance) {
		t.Fatalf("ambiguous instance: %v", err)
	}
	selected, err := s.InstanceFor(t.Context(), "b")
	if err != nil || !selected.Exists || selected.Value.Name != "Grace" {
		t.Fatalf("selection: %+v %v", selected, err)
	}
	if err = s.Given(t.Context(), "a", AccountClosed{}); err != nil {
		t.Fatal(err)
	}
	selected, err = s.InstanceFor(t.Context(), "a")
	if err != nil || selected.Exists {
		t.Fatalf("deletion: %+v %v", selected, err)
	}
}
func TestReadModelScenarioPropagatesFoldFailureWithoutPartialResults(t *testing.T) {
	registry, _, _ := reducerRegistry(t)
	s := chronicletest.NewReadModelScenario[Account](t, chronicletest.Config{Registry: registry})
	if err := s.Given(t.Context(), "a", AccountOpened{Name: "Ada"}, foldingFailure{}); err != nil {
		t.Fatal(err)
	}
	instance, err := s.Instance(t.Context())
	if !errors.Is(err, errFold) || instance.Exists {
		t.Fatalf("partial fold: %+v %v", instance, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = s.Instances(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
func TestReadModelScenarioInlineProjectionOverridesReducerWithoutMutatingRegistry(t *testing.T) {
	registry, model, _ := reducerRegistry(t)
	event, err := events.Define[AccountOpened]()
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("inline-account", model)
	projections.From(builder, event, nil)
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	_, err = chronicletest.OpenReadModelScenario[Account](t.Context(), chronicletest.Config{Registry: registry}, chronicletest.ReadModelOptions[Account]{Projection: &declaration})
	if !errors.Is(err, chronicletest.ErrFidelityUnavailable) {
		t.Fatalf("inline override did not select projection: %v", err)
	}
	// The original registry must still compile and select the reducer.
	s := chronicletest.NewReadModelScenario[Account](t, chronicletest.Config{Registry: registry})
	if err = s.Given(t.Context(), "a", AccountOpened{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	value, err := s.Instance(t.Context())
	if err != nil || !value.Exists {
		t.Fatalf("registry mutated: %+v %v", value, err)
	}
}

type ProjectedAccount struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(AccountOpened,from=Name)"`
}

func TestReadModelScenarioRefusesModelBoundProjectionWithoutKernel(t *testing.T) {
	registry := eventRegistry(t)
	if _, err := chronicle.RegisterReadModel[ProjectedAccount](registry); err != nil {
		t.Fatal(err)
	}
	_, err := chronicletest.OpenReadModelScenario[ProjectedAccount](t.Context(), chronicletest.Config{Registry: registry})
	if !errors.Is(err, chronicletest.ErrFidelityUnavailable) {
		t.Fatalf("substitute projection: %v", err)
	}
}
func TestReadModelScenarioTypedCallbacksShareFoldPlanAndInitialState(t *testing.T) {
	registry := eventRegistry(t)
	model, err := chronicle.RegisterReadModel[Account](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err = chronicle.RegisterReducerHandlers(registry, model, "explicit", []reducers.Handler{reducers.On(func(_ context.Context, event AccountOpened, current *Account, _ events.Context) (*Account, error) {
		return &Account{Name: current.Name + event.Name}, nil
	})}); err != nil {
		t.Fatal(err)
	}
	initial := Account{Name: "Hello "}
	s := chronicletest.NewReadModelScenario[Account](t, chronicletest.Config{Registry: registry}, chronicletest.ReadModelOptions[Account]{Initial: &initial})
	initial.Name = "mutated"
	if err = s.Given(t.Context(), "a", AccountOpened{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	value, err := s.Instance(t.Context())
	if err != nil || value.Value.Name != "Hello Ada" {
		t.Fatalf("initial state: %+v %v", value, err)
	}
}
