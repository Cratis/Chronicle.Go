// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/serialization"
)

func eventRegistry(t *testing.T) *chronicle.Registry {
	t.Helper()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[AccountOpened](registry); err != nil {
		t.Fatal(err)
	}
	return registry
}
func TestEventScenarioUsesProductionNamingAndIsolatedStorage(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, chronicletest.CSharpScenarioNaming} {
		t.Run(map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "preserve", serialization.CamelCase: "camel"}[policy], func(t *testing.T) {
			var tb testing.TB = t
			s := chronicletest.NewEventScenario(tb, chronicletest.Config{Registry: eventRegistry(t), Naming: policy})
			if err := s.Given(t.Context(), "first", AccountOpened{Name: "Ada"}); err != nil {
				t.Fatal(err)
			}
			result, err := s.EventLog().AppendMany(t.Context(), "second", []any{AccountOpened{Name: "Grace"}, AccountOpened{Name: "Lin"}})
			if err != nil {
				t.Fatal(err)
			}
			if err = result.Err(); err != nil {
				t.Fatal(err)
			}
			history, err := s.EventLog().ReadSource(t.Context(), "second", eventsequences.SourceFilter{})
			if err != nil || len(history) != 2 {
				t.Fatalf("history: %v %v", history, err)
			}
			property := "Name"
			if policy == serialization.CamelCase {
				property = "name"
			}
			if !strings.Contains(string(history[0].Content), `"`+property+`":"Grace"`) {
				t.Fatalf("naming not preserved: %s", history[0].Content)
			}
			if history[0].Context.SequenceNumber != 1 || history[1].Context.SequenceNumber != 2 {
				t.Fatal("positions are not monotonic")
			}
			isolated, err := s.Client.EventStore(t.Context(), s.Store.Name(), chronicle.WithNamespace("other"))
			if err != nil {
				t.Fatal(err)
			}
			present, err := isolated.EventLog().HasEvents(t.Context(), "first")
			if err != nil || present {
				t.Fatalf("namespace leaked: %v %v", present, err)
			}
			next, err := s.EventLog().Next(t.Context())
			if err != nil || next != 3 {
				t.Fatalf("next: %v %v", next, err)
			}
		})
	}
}
func TestEventScenarioSeedsRepeatedSourcesWithoutInventingConcurrencyChecks(t *testing.T) {
	s := chronicletest.NewEventScenario(t, chronicletest.Config{Registry: eventRegistry(t)})
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "Ada"}, AccountOpened{Name: "Grace"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "Lin"}); err != nil {
		t.Fatal(err)
	}
	history, err := s.EventLog().ReadSource(t.Context(), "one", eventsequences.SourceFilter{})
	if err != nil || len(history) != 3 {
		t.Fatalf("repeated seeds: %+v %v", history, err)
	}
}

func TestEventScenarioRefusesStrictFidelityAndConcurrency(t *testing.T) {
	s := chronicletest.NewEventScenario(t, chronicletest.Config{Registry: eventRegistry(t)})
	for _, layer := range []chronicletest.Layer{chronicletest.Constraints, chronicletest.Concurrency, chronicletest.Encryption, chronicletest.ObserverLifecycle} {
		if !errors.Is(s.Fidelity().Require(layer), chronicletest.ErrFidelityUnavailable) {
			t.Fatalf("claimed %s", layer)
		}
	}
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	history, err := s.EventLog().ReadHistory(t.Context(), "one", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.EventLog().Append(t.Context(), "one", AccountOpened{Name: "Grace"}, eventsequences.WithScope(eventsequences.Scope{Filter: history.Filter, Expectation: history.Expectation}))
	if err == nil {
		t.Fatal("substitute accepted a concurrency assertion")
	}
	loaded, err := s.EventLog().ReadFrom(t.Context(), 0, eventsequences.FromFilter{})
	if err != nil || len(loaded) != 1 {
		t.Fatalf("rejected append mutated storage: %v %v", loaded, err)
	}
}
func TestEventScenarioRefusesDiscoveredConstraintsBeforeConnecting(t *testing.T) {
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[AccountOpened](registry)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := constraints.UniqueValues("name").On(event.Descriptor(), "Name").Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddConstraint(definition); err != nil {
		t.Fatal(err)
	}
	_, err = chronicletest.OpenEventScenario(t.Context(), chronicletest.Config{Registry: registry})
	if !errors.Is(err, chronicletest.ErrFidelityUnavailable) {
		t.Fatalf("constraint fidelity: %v", err)
	}
}
func TestEventScenarioCancellationCloseAndMissingKernel(t *testing.T) {
	s := chronicletest.NewEventScenario(t, chronicletest.Config{Registry: eventRegistry(t)})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Given(ctx, events.SourceID("one"), AccountOpened{Name: "Ada"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(t.Context(), "one", AccountOpened{}); !errors.Is(err, chronicle.ErrClosed) {
		t.Fatalf("use after close: %v", err)
	}
	_, err := chronicletest.OpenEventScenario(t.Context(), chronicletest.Config{Engine: chronicletest.Kernel})
	if !errors.Is(err, chronicletest.ErrKernelUnavailable) {
		t.Fatalf("missing endpoint silently substituted: %v", err)
	}
}
