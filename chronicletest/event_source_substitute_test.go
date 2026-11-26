// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/alerts"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsources"
)

func TestSubstituteEventSourceIsMetadataNotRegistryValidation(t *testing.T) {
	for _, sourceType := range []string{"", "Default", "Explicit"} {
		t.Run(sourceType, func(t *testing.T) {
			transport := newSubstituteTransport()
			t.Cleanup(func() {
				if err := transport.Close(); err != nil {
					t.Error(err)
				}
			})
			kernel := &substituteKernel{events: make(map[sequenceKey][]*sequences.AppendedEventResponse)}
			sequences.RegisterEventSequencesServer(transport, kernel)
			client := sequences.NewEventSequencesClient(transport)
			_, err := client.Append(t.Context(), &sequences.AppendRequest{EventStore: "store", Namespace: "Default", EventSequenceId: "event-log", EventSourceId: "A", EventSource: "Unregistered", EventSourceType: sourceType, EventStreamType: "changes", EventStreamId: "west", EventType: &sequences.EventType{Id: "opened", Generation: 1}, Content: `{}`})
			if err != nil {
				t.Fatal(err)
			}
			wantType := sourceType
			if wantType == "" || wantType == "Default" {
				wantType = "Unregistered"
			}
			result, err := client.ForEventSourceIdAndEventTypes(t.Context(), &sequences.ForEventSourceIdAndEventTypesRequest{EventStore: "store", Namespace: "Default", EventSequenceId: "event-log", EventSourceId: "A", EventSourceType: wantType})
			if err != nil || len(result.GetData()) != 1 {
				t.Fatalf("result=%v error=%v", result, err)
			}
			assertSubstitutedSourceContext(t, result.Data[0].Context, wantType)
			assertSubstituteSourceServicesUnsupported(t, transport)
		})
	}
}

func assertSubstitutedSourceContext(t *testing.T, context *sequences.EventContext, wantType string) {
	t.Helper()
	for field, values := range map[string][2]string{
		"source": {context.EventSource, "Unregistered"}, "type": {context.EventSourceType, wantType},
		"stream type": {context.EventStreamType, "changes"}, "stream ID": {context.EventStreamId, "west"},
	} {
		if values[0] != values[1] {
			t.Errorf("%s = %q, want %q", field, values[0], values[1])
		}
	}
}

func assertSubstituteSourceServicesUnsupported(t *testing.T, transport *substituteTransport) {
	t.Helper()
	registry, err := eventsources.New("store", transport)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(t.Context(), eventsources.Definition{Name: "Unregistered"}); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("registration = %v", err)
	}
	incidents, err := alerts.New("store", "Default", transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := incidents.GetOpenIncidents(t.Context(), alerts.Query{}); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("alerts = %v", err)
	}
	if err := incidents.Subscribe(t.Context(), func(alerts.Incident) {}); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("subscribe = %v", err)
	}
}
