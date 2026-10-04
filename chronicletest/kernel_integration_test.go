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
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
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

// This witnesses append admission and synchronous bounded replay, not observer
// catch-up or asynchronous replay-job completion (Chronicle.Go#60).
func TestKernelReadModelStrictEventSubscription(t *testing.T) {
	registry := eventRegistry(t)
	if _, err := chronicle.RegisterEvent[auditMarker](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterReadModel[ProjectedAccount](registry); err != nil {
		t.Fatal(err)
	}
	for _, strict := range []bool{true, false} {
		name := "default"
		if strict {
			name = "strict"
		}
		t.Run(name, func(t *testing.T) {
			ctx := kernelContext(t)
			config := kernelConfig(registry)
			// The combined MongoDB database name includes store + "+es+" +
			// namespace. Keep both isolated coordinates short (42 bytes total).
			config.Store = chronicle.StoreName("ss-" + uuid.NewString()[:16])
			config.Namespace = chronicle.Namespace("ns-" + uuid.NewString()[:16])
			// New* skips only an omitted endpoint; configured failures are fatal.
			if config.ConnectionString == "" {
				t.Skip("set CHRONICLE_INTEGRATION_CONNECTION_STRING")
			}
			s, err := chronicletest.OpenReadModelScenario[ProjectedAccount](ctx, config, chronicletest.ReadModelOptions[ProjectedAccount]{StrictEventSubscription: strict})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := s.Given(ctx, "source", AccountOpened{Name: "A"}); err != nil {
				t.Fatal(err)
			}
			err = s.Given(ctx, "source", auditMarker{})
			if strict {
				if !errors.Is(err, chronicletest.ErrUnsubscribedEventSeeded) {
					t.Fatalf("registered marker was not rejected: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := s.Given(ctx, "source", AccountOpened{Name: "B"}); err != nil {
				t.Fatal(err)
			}
			reader, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithConnectionString(config.ConnectionString), chronicle.WithDevelopmentDefaults())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			})
			store, err := reader.EventStore(ctx, config.Store, chronicle.WithNamespace(config.Namespace))
			if err != nil {
				t.Fatal(err)
			}
			history, err := store.EventLog().ReadSource(ctx, "source", eventsequences.SourceFilter{})
			if err != nil {
				t.Fatal(err)
			}
			wantCount := 3
			if strict {
				wantCount = 2
			}
			if len(history) != wantCount {
				t.Fatalf("persisted seed count = %d, want %d", len(history), wantCount)
			}
			for i, event := range history {
				if event.Context.Store != config.Store || event.Context.Namespace != config.Namespace || event.Context.SourceID != "source" || event.Context.SequenceNumber != events.SequenceNumber(i) {
					t.Fatalf("history coordinates/order: %+v", event.Context)
				}
				if strict && event.Context.EventType.ID != "AccountOpened" {
					t.Fatal("strict rejection persisted the marker")
				}
			}
			first, err := events.Decode[AccountOpened](store.EventTypes(), history[0])
			if err != nil || first.Name != "A" {
				t.Fatalf("first accepted seed: %+v %v", first, err)
			}
			last, err := events.Decode[AccountOpened](store.EventTypes(), history[len(history)-1])
			if err != nil || last.Name != "B" {
				t.Fatalf("last accepted seed: %+v %v", last, err)
			}
			if !strict && history[1].Context.EventType.ID != "auditMarker" {
				t.Fatal("default behavior did not persist unrelated marker")
			}
			values, err := s.Instances(ctx)
			if err != nil || len(values) != 1 || values["source"].ID != "source" || values["source"].Name != "B" {
				t.Fatalf("typed bounded replay: %+v %v", values, err)
			}
			isolated, err := reader.EventStore(ctx, config.Store, chronicle.WithNamespace("other"))
			if err != nil {
				t.Fatal(err)
			}
			present, err := isolated.EventLog().HasEvents(ctx, "source")
			if err != nil || present {
				t.Fatalf("history crossed namespace: %v %v", present, err)
			}
			t.Logf("%s: history=%d ordered A/B, marker accepted=%t, replay[source]={id:source name:B}, namespace isolation passed", name, len(history), !strict)
		})
	}
}

func TestKernelReadModelScenarioDiscoversProjectionAndRejectsAmbiguousInstance(t *testing.T) {
	registry := eventRegistry(t)
	if _, err := chronicle.RegisterReadModel[ProjectedAccount](registry); err != nil {
		t.Fatal(err)
	}
	config := kernelConfig(registry)
	config.Store = chronicle.StoreName("borrowed-" + uuid.NewString())
	s := chronicletest.NewReadModelScenario[ProjectedAccount](t, config)
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
	for _, layer := range []chronicletest.Layer{chronicletest.ObserverLifecycle, chronicletest.DeliveryMetadata, chronicletest.EffectAcceptance} {
		if !errors.Is(s.Fidelity().Require(layer), chronicletest.ErrFidelityUnavailable) {
			t.Fatalf("replay claimed %s", layer)
		}
	}
	// A new scenario borrowing a populated store must not replay unseeded history.
	empty := chronicletest.NewReadModelScenario[ProjectedAccount](t, config)
	emptyValues, err := empty.Instances(ctx)
	if err != nil || emptyValues == nil || len(emptyValues) != 0 {
		t.Fatalf("empty borrowed scenario replayed history: %+v %v", emptyValues, err)
	}
	emptyInstance, err := empty.Instance(ctx)
	if err != nil || emptyInstance.Exists {
		t.Fatalf("empty borrowed instance: %+v %v", emptyInstance, err)
	}
	emptyInstance, err = empty.InstanceFor(ctx, "a")
	if err != nil || emptyInstance.Exists {
		t.Fatalf("empty borrowed keyed instance: %+v %v", emptyInstance, err)
	}
}
func TestKernelReadModelScenarioInlineProjectionOverridesReducer(t *testing.T) {
	registry, model, numbers := reducerRegistry(t)
	event, err := events.Define[AccountOpened]()
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("inline-account", model)
	projections.From(builder, event, func(from *projections.FromBuilder[Account, AccountOpened]) {
		projections.Value(from, projections.Path[Account, string]("Name"), "inline")
	}) // Default event-source key is supported by the safe replay profile.
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewReadModelScenario[Account](t, kernelConfig(registry), chronicletest.ReadModelOptions[Account]{Projection: &declaration})
	ctx := kernelContext(t)
	chronicletest.RequireFidelity(t, s.Fidelity(), chronicletest.ProjectionExecution)
	if err = s.Given(ctx, "source-account", AccountOpened{Name: "not-inline"}); err != nil {
		t.Fatal(err)
	}
	instance, err := s.InstanceFor(ctx, readmodels.Key("source-account"))
	if err != nil || !instance.Exists || instance.Value.Name != "inline" {
		t.Fatalf("inline precedence/source key: %+v %v", instance, err)
	}
	if len(*numbers) != 0 {
		t.Fatalf("overridden reducer ran: %v", *numbers)
	}
	// Both producers handle AccountOpened but produce different state. The
	// original registry still folds through its reducer, not the inline fixture.
	original := chronicletest.NewReadModelScenario[Account](t, chronicletest.Config{Registry: registry})
	if err = original.Given(ctx, "source-account", AccountOpened{Name: "not-inline"}); err != nil {
		t.Fatal(err)
	}
	reduced, err := original.InstanceFor(ctx, "source-account")
	if err != nil || !reduced.Exists || reduced.Value.Name != "not-inline" {
		t.Fatalf("original reducer registry mutated: %+v %v", reduced, err)
	}
}

func TestKernelReadModelScenarioRefusesCustomKeyProjectionReplay(t *testing.T) {
	registry, model, _ := reducerRegistry(t)
	event, err := events.Define[AccountOpened]()
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("custom-key-account", model)
	projections.From(builder, event, func(from *projections.FromBuilder[Account, AccountOpened]) {
		projections.Value(from, projections.Path[Account, string]("Name"), "inline")
	}, projections.UsingConstantKey("catalog"))
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewReadModelScenario[Account](t, kernelConfig(registry), chronicletest.ReadModelOptions[Account]{Projection: &declaration})
	ctx := kernelContext(t)
	if err = s.Given(ctx, "ignored-source", AccountOpened{Name: "not-inline"}); err != nil {
		t.Fatal(err)
	}
	// C# scenarios can select a custom projected key, but the 19.29.4 safe
	// replay admission profile requires SOURCE keys. No partial map or guessed
	// source-key instance may replace the requested custom-key result.
	if instances, err := s.Instances(ctx); instances != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("custom-key replay admitted: %+v %v", instances, err)
	}
	if instance, err := s.InstanceFor(ctx, "catalog"); instance.Exists || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("custom-key selection admitted: %+v %v", instance, err)
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
