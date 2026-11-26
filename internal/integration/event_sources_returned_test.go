//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
)

type HostedSourceTrigger struct{ Value string }

type HostedNamedSourceReactor struct{}

func (*HostedNamedSourceReactor) On(trigger HostedSourceTrigger) eventsequences.Entry {
	return hostedSourceEntry("returned-source", trigger.Value)
}

func TestKernelReactorReturnedEntryPreservesNamedEventSource(t *testing.T) {
	f := newKernelFixture(t)
	registry := integrationRegistry[HostedSourceEvent](t)
	if _, err := chronicle.RegisterEvent[HostedSourceTrigger](registry, events.WithID("hosted-source-trigger")); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactor[*HostedNamedSourceReactor](registry, func() *HostedNamedSourceReactor { return &HostedNamedSourceReactor{} }, reactors.WithID("named-source-return")); err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	registerHostedSource(t, f, store)
	if _, err := store.WaitForRegistration(f.ctx); err != nil {
		t.Fatal(err)
	}
	assertHostedSourceCommitted(t, appendHostedSourceVariant(f.ctx, store.EventLog(), hostedSourceEntry("returned-source", "seed"), "single"))
	result, err := store.EventLog().Append(f.ctx, "trigger-source", HostedSourceTrigger{"returned"})
	assertHostedSourceCommitted(t, hostedSingleSourceOutcome(result, err))
	waitOperations(t, f.ctx, func() bool { return len(readHostedSource(t, f, store.EventLog(), "returned-source")) == 2 })
	appended := readHostedSource(t, f, store.EventLog(), "returned-source")
	for _, event := range appended {
		assertHostedSourceContext(t, event.Context)
	}
	var returned HostedSourceEvent
	if err := json.Unmarshal(appended[1].Content, &returned); err != nil {
		t.Fatal(err)
	}
	if returned.Value != "returned" {
		t.Fatal("returned source content lost", returned)
	}
}
