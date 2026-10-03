//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

func kernelConfig(registry *chronicle.Registry) chronicletest.Config {
	return chronicletest.Config{Registry: registry, Engine: chronicletest.Kernel, ConnectionString: os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING"), Development: true}
}
func kernelContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func TestKernelReadModelScenarioDiscoversProjectionAndRejectsAmbiguousInstance(t *testing.T) {
	registry := eventRegistry(t)
	if _, err := chronicle.RegisterReadModel[ProjectedAccount](registry); err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewReadModelScenario[ProjectedAccount](t, kernelConfig(registry))
	ctx := kernelContext(t)
	chronicletest.RequireFidelity(t, s.Fidelity(), chronicletest.ProjectionExecution)
	if err := s.Given(ctx, "a", AccountOpened{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(ctx, "b", AccountOpened{Name: "Grace"}); err != nil {
		t.Fatal(err)
	}
	instances, err := s.Instances(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(instances) != 2 || instances["a"].Name != "Ada" || instances["b"].Name != "Grace" {
		t.Fatalf("kernel projection: %+v", instances)
	}
	if _, err = s.Instance(ctx); !errors.Is(err, chronicletest.ErrAmbiguousInstance) {
		t.Fatalf("ambiguous projection: %v", err)
	}
	selected, err := s.InstanceFor(ctx, "b")
	if err != nil || selected.Value.Name != "Grace" {
		t.Fatalf("selection: %+v %v", selected, err)
	}
	if !errors.Is(s.Fidelity().Require(chronicletest.ObserverLifecycle), chronicletest.ErrFidelityUnavailable) {
		t.Fatal("replay claimed observer catch-up")
	}
}
func TestKernelReadModelScenarioInlineProjectionOverridesReducer(t *testing.T) {
	registry, model, _ := reducerRegistry(t)
	event, err := chronicle.RegisterEvent[AccountRenamed](registry)
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("inline-account", model)
	projections.From(builder, event, func(from *projections.FromBuilder[Account, AccountRenamed]) {
		projections.Value(from, projections.Path[Account, string]("Name"), "inline")
	}, projections.UsingConstantKey("catalog"))
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewReadModelScenario[Account](t, kernelConfig(registry), chronicletest.ReadModelOptions[Account]{Projection: &declaration})
	ctx := kernelContext(t)
	if err = s.Given(ctx, "ignored-source", AccountRenamed{Name: "not-inline"}); err != nil {
		t.Fatal(err)
	}
	instance, err := s.InstanceFor(ctx, readmodels.Key("catalog"))
	if err != nil || !instance.Exists || instance.Value.Name != "inline" {
		t.Fatalf("inline precedence/custom key: %+v %v", instance, err)
	}
}
func TestKernelEventScenarioEnforcesConstraints(t *testing.T) {
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[AccountOpened](registry)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := constraints.UniqueValues("unique-name").On(event.Descriptor(), "Name").Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddConstraint(definition); err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewEventScenario(t, kernelConfig(registry))
	ctx := kernelContext(t)
	chronicletest.RequireFidelity(t, s.Fidelity(), chronicletest.Constraints)
	if err = s.Given(ctx, "first", AccountOpened{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	result, err := s.EventLog().Append(ctx, "second", AccountOpened{Name: "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	var violation *eventsequences.ConstraintError
	if !errors.As(result.Err(), &violation) || result.Disposition != eventsequences.Rejected {
		t.Fatalf("kernel constraint not enforced: %+v", result)
	}
	history, err := s.EventLog().ReadSource(ctx, "second", eventsequences.SourceFilter{})
	if err != nil || len(history) != 0 {
		t.Fatalf("rejection persisted: %+v %v", history, err)
	}
	if !errors.Is(s.Fidelity().Require(chronicletest.Encryption), chronicletest.ErrFidelityUnavailable) {
		t.Fatal("scenario claimed untested encryption support")
	}
}

type liveScenarioReactor struct{ handled chan string }

func (r *liveScenarioReactor) On(event AccountOpened) { r.handled <- event.Name }
func TestKernelEventScenarioOwnsRealObserverLifecycle(t *testing.T) {
	registry := eventRegistry(t)
	handled := make(chan string, 2)
	if err := chronicle.RegisterReactor[*liveScenarioReactor](registry, func() *liveScenarioReactor { return &liveScenarioReactor{handled} }, reactors.WithID("scenario-lifecycle")); err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewEventScenario(t, kernelConfig(registry))
	ctx := kernelContext(t)
	chronicletest.RequireFidelity(t, s.Fidelity(), chronicletest.ObserverLifecycle)
	if err := s.Given(ctx, "first", AccountOpened{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-handled:
		if value != "Ada" {
			t.Fatal(value)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := s.Store.UnregisterReactor(ctx, "scenario-lifecycle"); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(ctx, "first", AccountOpened{Name: "Grace"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-handled:
		t.Fatalf("unregistered observer invoked: %s", value)
	default:
	}
}
