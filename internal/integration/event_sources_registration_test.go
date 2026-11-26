//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/eventsources"
	"github.com/cratis/chronicle.go/eventsources"
	"google.golang.org/protobuf/proto"
)

type HostedSourceEvent struct{ Value string }

func newHostedSourceStore(t *testing.T) (*kernelFixture, *chronicle.EventStore) {
	t.Helper()
	f := newKernelFixture(t)
	store, err := f.client(integrationRegistry[HostedSourceEvent](t)).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	return f, store
}

func hostedSourceDefinition() eventsources.Definition {
	return eventsources.Definition{Name: "Billing", Description: "billing events", Concurrency: eventsources.EventSourceID | eventsources.EventSourceType,
		Streams: []eventsources.Stream{{Name: "changes", Description: "account changes", Concurrency: eventsources.EventSourceID | eventsources.EventStreamID}}}
}

func registerHostedSource(t *testing.T, f *kernelFixture, store *chronicle.EventStore) {
	t.Helper()
	if err := store.EventSources().Register(f.ctx, hostedSourceDefinition()); err != nil {
		t.Fatal(err)
	}
}

func hostedSourceRegistrations(t *testing.T, f *kernelFixture) map[string]*contracts.EventSourceDetailsResponse {
	t.Helper()
	// Raw readback is test-only; the public source facade intentionally exposes no query.
	result, err := contracts.NewEventSourcesClient(f.conn).AllEventSources(f.ctx, &contracts.AllEventSourcesRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("missing source registration response")
	}
	if !result.IsAuthorized {
		t.Fatal("source readback unauthorized")
	}
	if len(result.ValidationResults) != 0 || len(result.ExceptionMessages) != 0 {
		t.Fatal("source readback failed", result)
	}
	registrations := make(map[string]*contracts.EventSourceDetailsResponse, len(result.Data))
	for _, definition := range result.Data {
		if definition == nil {
			t.Fatal("nil source registration")
		}
		registrations[definition.Name] = definition
	}
	return registrations
}

func assertHostedSourceRegistration(t *testing.T, actual *contracts.EventSourceDetailsResponse, expected eventsources.Definition) {
	t.Helper()
	if actual == nil {
		t.Fatalf("registration %q missing", expected.Name)
	}
	if actual.Id == "" {
		t.Fatal("source registration has no identity")
	}
	want := &contracts.EventSourceDetailsResponse{Id: actual.Id, Name: string(expected.Name), Description: expected.Description,
		Owner: contracts.EventSourceOwner_Client, Concurrency: contracts.ConcurrencyDimensions(expected.Concurrency)}
	for _, stream := range expected.Streams {
		want.Streams = append(want.Streams, &contracts.EventStreamDefinition{Name: stream.Name, Description: stream.Description, Concurrency: contracts.ConcurrencyDimensions(stream.Concurrency)})
	}
	if !proto.Equal(actual, want) {
		t.Fatalf("registration = %v, want %v", actual, want)
	}
}

func TestKernelEventSourceRegistrationUpsertsWithoutRemovingAbsentNames(t *testing.T) {
	f, store := newHostedSourceStore(t)
	billing := hostedSourceDefinition()
	audit := eventsources.Definition{Name: "Audit", Description: "retained source"}
	if err := store.EventSources().Register(f.ctx, billing, audit); err != nil {
		t.Fatal(err)
	}
	first := hostedSourceRegistrations(t, f)
	assertHostedSourceRegistration(t, first["Billing"], billing)
	assertHostedSourceRegistration(t, first["Audit"], audit)

	// A facade from another namespace still updates the store-wide registration.
	other, err := f.client(integrationRegistry[HostedSourceEvent](t)).EventStore(f.ctx, f.storeName, chronicle.WithNamespace("other"))
	if err != nil {
		t.Fatal(err)
	}
	billing.Description = "replacement definition"
	billing.Concurrency = eventsources.EventStreamType
	billing.Streams = []eventsources.Stream{{Name: "replacement", Description: "replacement stream", Concurrency: eventsources.EventStreamID}}
	if err := other.EventSources().Register(f.ctx, billing); err != nil {
		t.Fatal(err)
	}
	updated := hostedSourceRegistrations(t, f)
	assertHostedSourceRegistration(t, updated["Billing"], billing)
	assertHostedSourceRegistration(t, updated["Audit"], audit)
	if updated["Billing"].Id != first["Billing"].Id {
		t.Fatal("upsert replaced the source identity")
	}
}
