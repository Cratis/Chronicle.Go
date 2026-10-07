//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	readmodelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedchildrenfixtures"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

func TestKernelSingleDerivedChildrenCreateJoinRemoveReaddAndReplay(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		name := map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "DefaultNamingPolicy", serialization.CamelCase: "CamelCaseNamingPolicy"}[policy]
		t.Run(name, func(t *testing.T) {
			f := newKernelFixture(t)
			registry := chronicle.NewRegistry()
			if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemAdded](registry); err != nil {
				t.Fatal(err)
			}
			if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemRemoved](registry); err != nil {
				t.Fatal(err)
			}
			if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemRenamed](registry); err != nil {
				t.Fatal(err)
			}
			codecs, err := derivedchildrenfixtures.Codecs()
			if err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[derivedchildrenfixtures.Catalog](registry, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.AddProjection(projections.ModelBound(model, projections.WithIdentifier("derived-children"))); err != nil {
				t.Fatal(err)
			}
			store, err := f.client(registry, chronicle.WithNamingPolicy(policy)).EventStore(f.ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			reader := readmodels.For(store.ReadModels(), model)
			absent, err := reader.Get(f.ctx, "order")
			if err != nil || absent.Exists {
				t.Fatalf("registration fabricated presence: %v %v", absent, err)
			}
			appendSuccessfully(t, f.ctx, store, "order", derivedchildrenfixtures.ItemAdded{ItemID: "line", OrderID: "order", Name: "created"})
			matches := func(name string) func(derivedchildrenfixtures.Catalog) bool {
				return func(value derivedchildrenfixtures.Catalog) bool {
					if len(value.Items) != 1 {
						return false
					}
					line, ok := value.Items[0].(*derivedchildrenfixtures.Line)
					return ok && line.ItemID == "line" && line.Name == name
				}
			}
			awaitProjection(t, f.ctx, reader, "order", matches("created"))
			appendSuccessfully(t, f.ctx, store, "line", derivedchildrenfixtures.ItemRenamed{Name: "updated"})
			awaitProjection(t, f.ctx, reader, "order", matches("updated"))
			appendSuccessfully(t, f.ctx, store, "order", derivedchildrenfixtures.ItemRemoved{ItemID: "line", OrderID: "order"})
			awaitProjection(t, f.ctx, reader, "order", func(value derivedchildrenfixtures.Catalog) bool { return len(value.Items) == 0 })
			appended, err := store.EventLog().AppendWithMetadata(f.ctx, "line", derivedchildrenfixtures.ItemRenamed{Name: "readded"})
			if err != nil || appended.Result().Err() != nil {
				t.Fatal("join append failed", err)
			}
			// Await a server observation barrier before asserting that this join
			// did not recreate the child. A read racing the observer is not proof.
			completed, err := appended.WaitForCompletion(f.ctx, store.Observers(), 10*time.Second)
			if err != nil || !completed.IsSuccess() || completed.Trivial() {
				t.Fatalf("join observation barrier: %+v %v", completed, err)
			}
			empty, err := reader.Get(f.ctx, "order")
			if err != nil || len(empty.Value.Items) != 0 {
				t.Fatalf("join recreated child: %+v %v", empty, err)
			}
			appendSuccessfully(t, f.ctx, store, "order", derivedchildrenfixtures.ItemAdded{ItemID: "line", OrderID: "order", Name: "new creation"})
			readded := awaitProjection(t, f.ctx, reader, "order", matches("readded"))
			raw, err := store.ReadModels().Get(f.ctx, model.Identifier(), "order")
			if err != nil || !strings.Contains(string(raw.Value), `"_derivedTypeId":"line"`) {
				t.Fatalf("raw discriminator: %s %v", raw.Value, err)
			}
			if result, err := store.ReadModels().ReplayProjection(f.ctx, model.Identifier(), 4); !errors.Is(err, chronicle.ErrUnsupported) || result != nil {
				t.Fatalf("relationship ReplayGetAll must refuse: %v", err)
			}
			t.Run("observer_replay", func(t *testing.T) {
				// A projection replay rebuilds the read model into a new
				// container and records a read-model occurrence for it. The
				// occurrence, not an unchanged read-back, is the replay evidence.
				explorer := readmodelcontracts.NewReadModelsClient(f.conn)
				request := &readmodelcontracts.GetOccurrencesRequest{EventStore: string(f.storeName), Namespace: string(chronicle.DefaultNamespace), Type: &readmodelcontracts.ReadModelType{Identifier: string(model.Identifier()), Generation: uint32(model.Descriptor().Generation())}}
				before, err := explorer.GetOccurrences(f.ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				handle, err := store.Observers().Replay(f.ctx, "derived-children", events.EventLog)
				if err != nil {
					t.Fatal(err)
				}
				final, err := store.Jobs().WaitForTerminalOrAbsent(f.ctx, handle.ID(), 30*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if final != nil && final.Status() != jobs.CompletedSuccessfully {
					t.Fatalf("replay job %s status=%d", handle.ID(), final.Status())
				}
				ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
				defer cancel()
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					after, err := explorer.GetOccurrences(ctx, request)
					if err != nil {
						t.Fatal("replay occurrence unavailable", err)
					}
					if len(after.Occurrences) > len(before.Occurrences) {
						latest := after.Occurrences[len(after.Occurrences)-1]
						t.Logf("replay job %s recorded occurrence container=%s", handle.ID(), latest.ContainerName)
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("replay recorded no read-model occurrence", ctx.Err())
					case <-ticker.C:
					}
				}
				replayed := awaitProjection(t, f.ctx, reader, "order", matches("readded"))
				if !reflect.DeepEqual(readded.Value, replayed.Value) {
					t.Fatalf("replay changed child: %+v", replayed)
				}
				raw, err := store.ReadModels().Get(f.ctx, model.Identifier(), "order")
				if err != nil || !strings.Contains(string(raw.Value), `"_derivedTypeId":"line"`) {
					t.Fatalf("raw discriminator after replay: %s %v", raw.Value, err)
				}
			})
		})
	}
}

// The kernel adds a child for any non-join child From whose identity is absent
// (ProjectionEventContextExtensions.Project), so a keyed update after removal
// or before creation recreates the child. It must carry the discriminator so
// the whole instance still decodes.
func TestKernelDerivedChildKeyedUpdateRecreatesADecodableChild(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		name := map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "DefaultNamingPolicy", serialization.CamelCase: "CamelCaseNamingPolicy"}[policy]
		t.Run(name, func(t *testing.T) {
			f := newKernelFixture(t)
			registry := chronicle.NewRegistry()
			if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemAdded](registry); err != nil {
				t.Fatal(err)
			}
			if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemRemoved](registry); err != nil {
				t.Fatal(err)
			}
			if _, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemRenamed](registry); err != nil {
				t.Fatal(err)
			}
			updated, err := chronicle.RegisterEvent[derivedchildrenfixtures.ItemUpdated](registry)
			if err != nil {
				t.Fatal(err)
			}
			codecs, err := derivedchildrenfixtures.Codecs()
			if err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[derivedchildrenfixtures.Catalog](registry, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.AddProjection(projections.ModelBound(model, projections.WithIdentifier("derived-children-update"), projections.WithNodes(projections.Node[derivedchildrenfixtures.Line](
				projections.FromEvent(updated,
					projections.UsingKey(projections.Path[derivedchildrenfixtures.ItemUpdated, string]("ItemId")),
					projections.UsingParentKey(projections.Path[derivedchildrenfixtures.ItemUpdated, string]("OrderId"))),
			)))); err != nil {
				t.Fatal(err)
			}
			store, err := f.client(registry, chronicle.WithNamingPolicy(policy)).EventStore(f.ctx, f.storeName)
			if err != nil {
				t.Fatal(err)
			}
			reader := readmodels.For(store.ReadModels(), model)
			observe := func(source events.SourceID, value any) {
				t.Helper()
				appended, err := store.EventLog().AppendWithMetadata(f.ctx, source, value)
				if err != nil || appended.Result().Err() != nil {
					t.Fatal("append failed", err)
				}
				completed, err := appended.WaitForCompletion(f.ctx, store.Observers(), 10*time.Second)
				if err != nil || !completed.IsSuccess() || completed.Trivial() {
					t.Fatalf("observation barrier: %+v %v", completed, err)
				}
			}
			recreated := func(key readmodels.Key) {
				t.Helper()
				instance, err := reader.Get(f.ctx, key)
				if err != nil {
					t.Fatalf("read back %s: %v", key, err)
				}
				if !instance.Exists || len(instance.Value.Items) != 1 {
					t.Fatalf("kernel did not recreate the child of %s: %+v", key, instance)
				}
				line, ok := instance.Value.Items[0].(*derivedchildrenfixtures.Line)
				if !ok || line.ItemID != "line" {
					t.Fatalf("recreated child of %s: %#v", key, instance.Value.Items[0])
				}
				raw, err := store.ReadModels().Get(f.ctx, model.Identifier(), key)
				if err != nil || !strings.Contains(string(raw.Value), `"_derivedTypeId":"line"`) {
					t.Fatalf("raw discriminator of %s: %s %v", key, raw.Value, err)
				}
				t.Logf("kernel recreated %s child: %s", key, raw.Value)
			}
			appendSuccessfully(t, f.ctx, store, "order", derivedchildrenfixtures.ItemAdded{ItemID: "line", OrderID: "order", Name: "created"})
			awaitProjection(t, f.ctx, reader, "order", func(value derivedchildrenfixtures.Catalog) bool { return len(value.Items) == 1 })
			appendSuccessfully(t, f.ctx, store, "order", derivedchildrenfixtures.ItemRemoved{ItemID: "line", OrderID: "order"})
			awaitProjection(t, f.ctx, reader, "order", func(value derivedchildrenfixtures.Catalog) bool { return len(value.Items) == 0 })
			observe("order", derivedchildrenfixtures.ItemUpdated{ItemID: "line", OrderID: "order", Name: "after removal"})
			recreated("order")
			observe("fresh", derivedchildrenfixtures.ItemUpdated{ItemID: "line", OrderID: "fresh", Name: "before creation"})
			recreated("fresh")
		})
	}
}
