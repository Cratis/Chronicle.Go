// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/serialization"
)

type derivedReactor struct {
	received *derivedfixtures.MembersChanged
}

func (r *derivedReactor) Changed(_ context.Context, event derivedfixtures.MembersChanged) {
	*r.received = event
}

func TestDerivedEventReadsAndReactorDispatchUseCompiledFamily(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		t.Run(map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "preserve", serialization.CamelCase: "camel"}[policy], func(t *testing.T) {
			codecs, err := derivedfixtures.Codecs()
			if err != nil {
				t.Fatal(err)
			}
			registry := chronicle.NewRegistry()
			if _, err := chronicle.RegisterEvent[derivedfixtures.MembersChanged](registry, events.WithCodecs(codecs)); err != nil {
				t.Fatal(err)
			}
			var received derivedfixtures.MembersChanged
			if err := chronicle.RegisterReactor[*derivedReactor](registry, func() *derivedReactor { return &derivedReactor{received: &received} }); err != nil {
				t.Fatal(err)
			}
			scenario := chronicletest.NewReactorScenario[*derivedReactor](t, chronicletest.Config{Registry: registry, Naming: policy})
			if err := scenario.Given(t.Context(), "source", derivedfixtures.Sample()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(received, derivedfixtures.Sample()) {
				t.Fatalf("reactor = %#v", received)
			}
			eventRegistry := chronicle.NewRegistry()
			if _, err := chronicle.RegisterEvent[derivedfixtures.MembersChanged](eventRegistry, events.WithCodecs(codecs)); err != nil {
				t.Fatal(err)
			}
			log := chronicletest.NewEventScenario(t, chronicletest.Config{Registry: eventRegistry, Naming: policy})
			if err := log.Given(t.Context(), "source", derivedfixtures.Sample()); err != nil {
				t.Fatal(err)
			}
			history, err := log.EventLog().ReadSource(t.Context(), "source", eventsequences.SourceFilter{})
			if err != nil || len(history) != 1 {
				t.Fatalf("history: %v %v", history, err)
			}
			artifacts, err := log.Client.Artifacts(log.Store.Name())
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := events.Decode[derivedfixtures.MembersChanged](artifacts.Events, history[0])
			if err != nil || !reflect.DeepEqual(decoded, derivedfixtures.Sample()) {
				t.Fatalf("typed event read: %#v %v", decoded, err)
			}
		})
	}
}
