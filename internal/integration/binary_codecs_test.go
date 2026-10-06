//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
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

type BinaryWitnessNested struct{ Inner []byte }
type BinaryChanged struct {
	Payload  []byte
	Optional *[]byte
	Chunks   [][]byte
	Nested   *BinaryWitnessNested
}
type BinaryWitnessModel struct {
	ID       string `json:"id"`
	Payload  []byte
	Optional *[]byte
	Chunks   [][]byte
	Nested   *BinaryWitnessNested
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
			output := t.TempDir()
			writes := 0
			for _, c := range profile.Cases {
				if c.DeclaredType != "BinaryEvent" || c.Operation != "EventSerializer.Serialize" {
					continue
				}
				writes++
				var payload string
				if c.Result.Status != "accepted" || json.Unmarshal(c.Result.Output, &payload) != nil {
					t.Fatal("invalid packaged write")
				}
				var original BinaryChanged
				if err := descriptor.Unmarshal([]byte(payload), &original); err != nil {
					t.Fatal(err)
				}
				for _, producer := range []string{"go", "csharp"} {
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
					if err := os.WriteFile(filepath.Join(output, string(source)+".event.kernel.json"), []byte(stored[0].Content), 0600); err != nil {
						t.Fatal(err)
					}
					readbacks++
					awaitProjection(t, fixture.ctx, reader, readmodels.Key(source), func(value BinaryWitnessModel) bool {
						got := BinaryChanged{value.Payload, value.Optional, value.Chunks, value.Nested}
						return reflect.DeepEqual(got, original)
					})
					raw, err := store.ReadModels().Get(fixture.ctx, model.Identifier(), readmodels.Key(source))
					if err != nil || !raw.Exists {
						t.Fatalf("binary model read failed: %v", err)
					}
					if err := os.WriteFile(filepath.Join(output, string(source)+".model.kernel.json"), raw.Value, 0600); err != nil {
						t.Fatal(err)
					}
					readbacks++
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
