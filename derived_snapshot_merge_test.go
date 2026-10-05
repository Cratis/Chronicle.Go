// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type DerivedSnapshotChanged struct{ Members []derivedfixtures.Member }
type DerivedSnapshotEmbedded struct{ Members []string }
type derivedSnapshotModel struct {
	DerivedSnapshotEmbedded
	ID      string
	Members []derivedfixtures.Member `chronicle:"set(DerivedSnapshotChanged)"`
}

func TestDerivedInitialStateRebindKeepsCapturedPlansAndOriginalHandles(t *testing.T) {
	registry := NewRegistry()
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterEvent[DerivedSnapshotChanged](registry, events.WithCodecs(codecs)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	model, err := RegisterReadModel[derivedSnapshotModel](registry, readmodels.WithCodecs(codecs), readmodels.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		calls++
		return compliance.Classification{}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	frozenCalls := calls
	initial := derivedSnapshotModel{ID: "owner", Members: derivedfixtures.Sample().Members}
	if err := registry.AddProjection(projections.ModelBound(model, projections.WithInitialValues(initial))); err != nil {
		t.Fatal(err)
	}
	initial.Members[0] = derivedfixtures.RobotValue{Count: 999}
	*codecs = serialization.Codecs{} // Declaration options own the original family set.
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		client, err := NewClient(WithSkipKeepAlive(), WithRegistry(registry), WithRegistryForStore("selected", registry), WithNamingPolicy(policy))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		})
		for _, store := range []StoreName{"default", "selected"} {
			artifacts, err := client.Artifacts(store)
			if err != nil {
				t.Fatal(err)
			}
			state := artifacts.Projections[0].KernelDefinition().InitialModelState
			d, ok := artifacts.ReadModels.LookupIdentifier(model.Identifier())
			if !ok {
				t.Fatal("captured model missing")
			}
			decoded, err := d.Unmarshal([]byte(state))
			want := derivedSnapshotModel{ID: "owner", Members: derivedfixtures.Sample().Members}
			if err != nil || !reflect.DeepEqual(decoded, &want) || !strings.Contains(state, `"_derivedTypeId":"human"`) || strings.Contains(state, "999") {
				t.Fatalf("rebound state: %s %v", state, err)
			}
			service, err := readmodels.New("store", "tenant", artifacts.ReadModels, &clientTransport{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := readmodels.For(service, model).Get(ctx, "owner"); !errors.Is(err, context.Canceled) {
				t.Fatal("original handle lost across rebind", err)
			}
		}
	}
	if calls != frozenCalls || calls == 0 {
		t.Fatal("capturing or rebinding reran classification providers")
	}
}

type DerivedSnapshotPromoted struct{ Name string }
type derivedSnapshotCollision struct {
	DerivedSnapshotPromoted
	Other string `json:"name"`
}
type derivedSnapshotVariant struct{ Nested derivedSnapshotCollision }
type derivedSnapshotCollisionModel struct{ Member any }

func TestDerivedInitialStateRejectsNestedPromotionLossBeforeIO(t *testing.T) {
	registry := NewRegistry()
	codecs, err := serialization.NewCodecs(serialization.Derived[any, derivedSnapshotVariant]("variant"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := RegisterEvent[DecisionCodecChanged](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[derivedSnapshotCollisionModel](registry, readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	initial := derivedSnapshotCollisionModel{Member: derivedSnapshotVariant{Nested: derivedSnapshotCollision{DerivedSnapshotPromoted: DerivedSnapshotPromoted{Name: "promoted"}, Other: "other"}}}
	if err := registry.AddProjection(projections.ModelBound(model, projections.FromEvent(event), projections.WithInitialValues(initial))); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry), WithNamingPolicy(serialization.CamelCase))
	if client != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("snapshot rebound a lost nested field", err)
	}
}
