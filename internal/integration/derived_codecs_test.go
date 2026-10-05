//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type DerivedItemAdded struct {
	ItemID  string                 `json:"itemId"`
	OrderID string                 `json:"orderId"`
	Member  derivedfixtures.Member `json:"member"`
}
type DerivedItem struct {
	ID     string                 `json:"id" chronicle:"key"`
	Member derivedfixtures.Member `json:"member"`
}
type DerivedCollection struct {
	ID      string                   `json:"id"`
	Members []derivedfixtures.Member `chronicle:"set(MembersChanged)"`
}

type DerivedCatalog struct {
	ID      string                   `json:"id"`
	Members []derivedfixtures.Member `chronicle:"set(MembersChanged)"`
	Items   []DerivedItem            `json:"items" chronicle:"children(DerivedItemAdded,key=itemId,parent-key=orderId)"`
}

func TestKernelDerivedCodecsCollectionsOrdinaryChildrenAndReplay(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		name := map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "DefaultNamingPolicy", serialization.CamelCase: "CamelCaseNamingPolicy"}[policy]
		t.Run(name, func(t *testing.T) {
			fixture := newKernelFixture(t)
			codecs, err := derivedfixtures.Codecs()
			if err != nil {
				t.Fatal(err)
			}
			registry := chronicle.NewRegistry()
			if _, err := chronicle.RegisterEvent[derivedfixtures.MembersChanged](registry, events.WithCodecs(codecs)); err != nil {
				t.Fatal(err)
			}
			if _, err := chronicle.RegisterEvent[DerivedItemAdded](registry, events.WithCodecs(codecs)); err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[DerivedCatalog](registry, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			collectionModel, err := chronicle.RegisterReadModel[DerivedCollection](registry, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			client := fixture.client(registry, chronicle.WithNamingPolicy(policy))
			store, err := client.EventStore(fixture.ctx, fixture.storeName)
			if err != nil {
				t.Fatal(err)
			}
			artifacts, err := client.Artifacts(fixture.storeName)
			if err != nil {
				t.Fatal(err)
			}
			original := derivedfixtures.Sample()
			appendSuccessfully(t, fixture.ctx, store, "go", original)
			payload, err := os.ReadFile("../../serialization/testdata/derived/" + name + ".payload.json")
			if err != nil {
				t.Fatal(err)
			}
			response, err := sequences.NewEventSequencesClient(fixture.conn).Append(fixture.ctx, &sequences.AppendRequest{
				EventStore: string(fixture.storeName), Namespace: string(chronicle.DefaultNamespace), EventSequenceId: "event-log", EventSourceId: "csharp", EventType: &sequences.EventType{Id: "MembersChanged", Generation: 1}, Content: string(payload),
				CorrelationId: wire.Guid(metadata.CorrelationID(uuid.New())), Occurred: &sequences.SerializableDateTimeOffset{Value: wire.DateTimeOffset(time.Now().UTC())}, CausedBy: &sequences.Identity{}, ConcurrencyScope: &sequences.ConcurrencyScope{},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := wire.CheckEnvelope(response); err != nil {
				t.Fatal(err)
			}
			if response.Response == nil || !response.Response.IsSuccess {
				t.Fatal("captured C# append failed", response)
			}
			for _, source := range []events.SourceID{"go", "csharp"} {
				stored, err := store.EventLog().ReadSource(fixture.ctx, source, eventsequences.SourceFilter{})
				if err != nil || len(stored) != 1 {
					t.Fatalf("stored %s: %v %v", source, stored, err)
				}
				decoded, err := events.Decode[derivedfixtures.MembersChanged](artifacts.Events, stored[0])
				if err != nil || !reflect.DeepEqual(decoded, original) {
					t.Fatalf("kernel roundtrip %s: %#v %v", source, decoded, err)
				}
				if directory := os.Getenv("CHRONICLE_DERIVED_CAPTURE_DIRECTORY"); directory != "" {
					if err := os.WriteFile(filepath.Join(directory, name+"."+string(source)+".kernel.json"), stored[0].Content, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			reader := readmodels.For(store.ReadModels(), model)
			awaitProjection(t, fixture.ctx, reader, "go", func(value DerivedCatalog) bool { return reflect.DeepEqual(value.Members, original.Members) })
			appendSuccessfully(t, fixture.ctx, store, "go", DerivedItemAdded{ItemID: "line", OrderID: "go", Member: original.Primary})
			awaitProjection(t, fixture.ctx, reader, "go", func(value DerivedCatalog) bool {
				return len(value.Items) == 1 && reflect.DeepEqual(value.Items[0].Member, original.Primary)
			})
			updated := derivedfixtures.RobotValue{Count: 84}
			appendSuccessfully(t, fixture.ctx, store, "go", DerivedItemAdded{ItemID: "line", OrderID: "go", Member: updated})
			awaitProjection(t, fixture.ctx, reader, "go", func(value DerivedCatalog) bool { return len(value.Items) == 1 && value.Items[0].Member == updated })
			// The merged replay policy rejects relationships, even when live
			// concrete child updates work. Do not bypass it to exercise codecs.
			if result, err := store.ReadModels().ReplayProjection(fixture.ctx, model.Identifier(), 4); !errors.Is(err, chronicle.ErrUnsupported) || result != nil {
				t.Fatalf("relationship replay must refuse: %v", err)
			}
			replayed, err := store.ReadModels().ReplayProjection(fixture.ctx, collectionModel.Identifier(), 4)
			if err != nil {
				t.Fatal(err)
			}
			d, _ := artifacts.ReadModels.LookupIdentifier(collectionModel.Identifier())
			found := false
			for _, raw := range replayed {
				value, err := d.Unmarshal(raw)
				if err != nil {
					t.Fatal(err)
				}
				collection := value.(*DerivedCollection)
				if collection.ID == "go" {
					found = reflect.DeepEqual(collection.Members, original.Members)
				}
			}
			if !found {
				t.Fatal("replay lost derived collection")
			}
		})
	}
}
