//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type BinaryArrayControl struct{ Chunks [][]byte }
type BinaryWitnessNested struct{ Inner []byte }
type BinaryChanged struct {
	Payload  []byte
	Optional *[]byte
	Nested   *BinaryWitnessNested
}

// Read models and AutoMap now qualify only emitted root binary leaves. Nested
// remains on BinaryChanged to retain the existing event-history round-trip check.
type BinaryWitnessModel struct {
	ID       string `json:"id"`
	Payload  []byte
	Optional *[]byte
}

func TestKernelBinaryEventsProjectionAndReadModel(t *testing.T) {
	data, err := os.ReadFile("../../serialization/testdata/binary/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	var capture enumKernelCapture
	if err := json.Unmarshal(data, &capture); err != nil {
		t.Fatal(err)
	}
	readbacks := 0
	for _, profile := range capture.Profiles {
		policy := serialization.PreservePropertyNames
		if strings.Contains(profile.NamingPolicy, "CamelCase") {
			policy = serialization.CamelCase
		}
		t.Run(profile.NamingPolicy, func(t *testing.T) {
			fixture := newKernelFixture(t)
			fixture.storeName = chronicle.StoreName("binary-" + uuid.NewString()[:8])
			registry := chronicle.NewRegistry()
			event, err := chronicle.RegisterEvent[BinaryChanged](registry)
			if err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[BinaryWitnessModel](registry)
			if err != nil {
				t.Fatal(err)
			}
			builder := projections.NewBuilder("binary", model)
			projections.From(builder, event, nil)
			projection, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.AddProjection(projection); err != nil {
				t.Fatal(err)
			}
			client := fixture.client(registry, chronicle.WithNamingPolicy(policy), chronicle.WithEventTypeGenerationValidation(true))
			store, err := client.EventStore(fixture.ctx, fixture.storeName)
			if err != nil {
				t.Fatal(err)
			}
			artifacts, err := client.Artifacts(fixture.storeName)
			if err != nil {
				t.Fatal(err)
			}
			descriptor, ok := artifacts.Events.LookupRef(event.Ref())
			if !ok {
				t.Fatal("binary event not registered")
			}
			reader := readmodels.For(store.ReadModels(), model)
			output := binaryEvidenceDirectory(t)
			writes := 0
			for _, c := range profile.Cases {
				if c.DeclaredType != "BinaryEvent" || c.Operation != "EventSerializer.Serialize" {
					continue
				}
				if c.ID == "chunks" {
					// Array admission is refused, not silently decoded or asserted equal.
					if _, err := chronicle.RegisterEvent[BinaryArrayControl](chronicle.NewRegistry()); !errors.Is(err, chronicle.ErrUnsupported) {
						t.Fatal("binary array event registration was not refused", err)
					}
					if _, err := chronicle.RegisterReadModel[BinaryArrayControl](chronicle.NewRegistry()); !errors.Is(err, chronicle.ErrUnsupported) {
						t.Fatal("binary array model registration was not refused", err)
					}
					c.ID = "nested"
				}
				writes++
				var payload string
				if c.Result.Status != "accepted" || json.Unmarshal(c.Result.Output, &payload) != nil {
					t.Fatal("invalid packaged write")
				}
				if c.ID == "nested" {
					// Retain the captured C# byte strings; remove only the refused
					// collection so the nested leaf is exercised independently.
					var properties map[string]json.RawMessage
					if err := json.Unmarshal([]byte(payload), &properties); err != nil {
						t.Fatal(err)
					}
					chunks := "Chunks"
					if policy == serialization.CamelCase {
						chunks = "chunks"
					}
					if _, ok := properties[chunks]; !ok {
						t.Fatal("captured chunks property is missing")
					}
					delete(properties, chunks)
					data, err := json.Marshal(properties)
					if err != nil {
						t.Fatal(err)
					}
					payload = string(data)
				}
				var original BinaryChanged
				if err := descriptor.Unmarshal([]byte(payload), &original); err != nil {
					t.Fatal(err)
				}
				if c.ID == "nested" {
					want := BinaryChanged{Payload: []byte{}, Nested: &BinaryWitnessNested{Inner: []byte{1, 2}}}
					if !reflect.DeepEqual(original, want) {
						t.Fatalf("captured nested case = %#v, want %#v", original, want)
					}
					original = want
				}
				for _, producer := range []string{"go", "csharp"} {
					t.Run(c.ID+"/"+producer, func(t *testing.T) {
						source := events.SourceID(producer + "-" + c.ID)
						if producer == "go" {
							appendSuccessfully(t, fixture.ctx, store, source, original)
						} else {
							response, err := sequences.NewEventSequencesClient(fixture.conn).Append(fixture.ctx, &sequences.AppendRequest{
								EventStore: string(fixture.storeName), Namespace: string(chronicle.DefaultNamespace), EventSequenceId: "event-log", EventSourceId: string(source),
								EventType: &sequences.EventType{Id: string(event.Ref().ID), Generation: 1}, Content: payload,
								CorrelationId: wire.Guid(metadata.CorrelationID(uuid.New())), Occurred: &sequences.SerializableDateTimeOffset{Value: wire.DateTimeOffset(time.Now().UTC())},
								CausedBy: &sequences.Identity{}, ConcurrencyScope: &sequences.ConcurrencyScope{SequenceNumber: uint64(events.Unavailable)},
							})
							if err != nil {
								t.Fatal(err)
							}
							if err := wire.CheckEnvelope(response); err != nil {
								t.Fatal(err)
							}
							if response.Response == nil || !response.Response.IsSuccess {
								t.Fatalf("captured C# binary append failed for %s", c.ID)
							}
						}
						stored, err := store.EventLog().ReadSource(fixture.ctx, source, eventsequences.SourceFilter{})
						if err != nil || len(stored) != 1 {
							t.Fatalf("binary event read failed: %v", err)
						}
						decoded, err := events.Decode[BinaryChanged](artifacts.Events, stored[0])
						if err != nil || !reflect.DeepEqual(decoded, original) {
							t.Fatalf("binary event lost bytes: %v", err)
						}
						writeBinaryEvidence(t, output, string(source)+".event.kernel.json", []byte(stored[0].Content))
						readbacks++
						captureBinaryModel(t, fixture, model.Identifier(), source, stored[0].Context.SequenceNumber, output)
						awaitProjection(t, fixture.ctx, reader, readmodels.Key(source), func(value BinaryWitnessModel) bool {
							return reflect.DeepEqual(value.Payload, original.Payload) && reflect.DeepEqual(value.Optional, original.Optional)
						})
						raw, err := store.ReadModels().Get(fixture.ctx, model.Identifier(), readmodels.Key(source))
						if err != nil || !raw.Exists {
							t.Fatalf("binary model read failed: %v", err)
						}
						writeBinaryEvidence(t, output, string(source)+".model.kernel.json", raw.Value)
						readbacks++
					})
				}
			}
			if writes != 11 {
				t.Fatalf("incomplete binary writes: %d", writes)
			}
		})
	}
	if readbacks != 88 {
		t.Fatalf("binary event/model readbacks = %d, want 88", readbacks)
	}
	t.Logf("verified %d Go/C# binary event and AutoMap model readbacks", readbacks)
}
