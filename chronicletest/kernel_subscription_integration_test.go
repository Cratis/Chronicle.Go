//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
	"github.com/google/uuid"
)

// Witnesses membership only, not relationship delivery, sink catch-up or replay.
// ProjectionsManager.cs:446-471 at v19.29.4 subscribes with Projection.EventTypes.
func TestKernelStrictMembershipMatchesProjectionObserverEventTypes(t *testing.T) {
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Skip("set CHRONICLE_INTEGRATION_CONNECTION_STRING")
	}
	cases := []struct {
		kind string
		want []events.TypeID
	}{
		{"children", []events.TypeID{"subscriptionAdded", "subscriptionJoined", "subscriptionRemoved"}},
		{"join", []events.TypeID{"subscriptionAdded", "subscriptionRemoved"}},
		{"nested", []events.TypeID{"subscriptionAdded", "subscriptionRemoved"}},
		{"join removal", []events.TypeID{"subscriptionAdded"}},
		{"custom key", []events.TypeID{"subscriptionAdded", "subscriptionRemoved"}},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			registry := subscriptionRegistry(t, tc.kind)
			config := Config{Registry: registry, Engine: Kernel, ConnectionString: endpoint, Development: true,
				Store: chronicle.StoreName("sm-" + uuid.NewString()[:16]), Namespace: chronicle.Namespace("ns-" + uuid.NewString()[:16])}
			eventScenario, err := OpenEventScenario(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := eventScenario.Close(); err != nil {
					t.Error(err)
				}
			})
			observed := projectionObserverIDs(t, ctx, eventScenario.Store)
			strict, err := OpenReadModelScenario[subscriptionModel](ctx, config, ReadModelOptions[subscriptionModel]{StrictEventSubscription: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := strict.Close(); err != nil {
					t.Error(err)
				}
			})
			var admitted []events.TypeID
			descriptors := strict.artifacts.Events.Descriptors()
			if len(descriptors) == 0 {
				t.Fatal("no registered events to test")
			}
			for _, descriptor := range descriptors {
				value := reflect.New(descriptor.GoType()).Elem().Interface()
				err := strict.Given(ctx, "source", value)
				if err == nil {
					admitted = append(admitted, descriptor.Ref().ID)
				} else if !errors.Is(err, ErrUnsubscribedEventSeeded) {
					t.Fatalf("seed %s: %v", descriptor.Ref().ID, err)
				}
			}
			slices.Sort(admitted)
			admitted = slices.Compact(admitted)
			if !slices.Equal(admitted, tc.want) || !slices.Equal(observed, tc.want) {
				t.Fatalf("admitted=%v observer=%v golden=%v", admitted, observed, tc.want)
			}
			t.Logf("admitted == projection observer EventTypes == golden: %v", tc.want)
		})
	}
}

func projectionObserverIDs(t *testing.T, ctx context.Context, store *chronicle.EventStore) []events.TypeID {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		observers, err := store.Observers().List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, observer := range observers {
			if observer.Type() != observation.Projection || len(observer.EventTypes()) == 0 {
				continue
			}
			var ids []events.TypeID
			for _, ref := range observer.EventTypes() {
				ids = append(ids, ref.ID)
			}
			slices.Sort(ids)
			return slices.Compact(ids)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("projection observer did not expose EventTypes: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
