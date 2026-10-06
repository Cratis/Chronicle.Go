// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
)

func TestStrictProjectionAdmitsRelationshipProfilesWithKernelMembership(t *testing.T) {
	for _, kind := range []string{"children", "nested", "join", "join removal", "custom key", "mixed all"} {
		t.Run(kind, func(t *testing.T) {
			f := newSubscriptionFixture(t, true, kind)
			selected := f.scenario.subscription
			if err := selected.admit("subscriptionAdded"); err != nil {
				t.Fatal(err)
			}
			if kind == "children" || kind == "nested" || kind == "join" {
				if err := selected.admit("subscriptionRemoved"); err != nil {
					t.Fatalf("relationship-only member: %v", err)
				}
			}
			if kind == "children" {
				if err := selected.admit("subscriptionJoined"); err != nil {
					t.Fatalf("child join removal: %v", err)
				}
			}
			err := selected.admit("subscriptionMarker")
			if kind == "mixed all" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrUnsubscribedEventSeeded) {
				t.Fatalf("marker admission = %v", err)
			}
			if kind == "join removal" && !errors.Is(selected.admit("subscriptionRemoved"), ErrUnsubscribedEventSeeded) {
				t.Fatal("root join removal became a member")
			}
		})
	}
}

func TestStrictRelationshipScenarioRejectsBeforeReplayRefusal(t *testing.T) {
	f := newSubscriptionFixture(t, true, "children")
	beforeUnary, beforeStreams := f.connection.unaryCalls.Load(), f.connection.streamCalls.Load()
	rejectedCodecCalls.Store(0)
	rejectedErrorHooks.Store(0)
	for _, value := range []any{subscriptionMarker{}, subscriptionHostileEvent{Secret: "dummy"}} {
		if err := f.scenario.Given(t.Context(), "source", value); !errors.Is(err, ErrUnsubscribedEventSeeded) {
			t.Fatalf("unsubscribed seed = %v", err)
		}
	}
	if f.connection.unaryCalls.Load() != beforeUnary || f.connection.streamCalls.Load() != beforeStreams || f.providers.Load() != 0 || rejectedCodecCalls.Load() != 0 || rejectedErrorHooks.Load() != 0 || f.appendServer.calls != 0 || len(f.scenario.history) != 0 {
		t.Fatal("rejected seed performed an effect")
	}
	if err := f.scenario.Given(t.Context(), "source", subscriptionRemoved{}); err != nil {
		t.Fatalf("child-only seed = %v", err)
	}
	if f.appendServer.calls != 1 || len(f.scenario.history) != 1 {
		t.Fatal("child-only seed was not appended")
	}
	beforeUnary, beforeStreams = f.connection.unaryCalls.Load(), f.connection.streamCalls.Load()
	values, err := f.scenario.Instances(t.Context())
	if values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("relationship replay = %v, %v", values, err)
	}
	if f.connection.unaryCalls.Load() != beforeUnary || f.connection.streamCalls.Load() != beforeStreams {
		t.Fatal("unsupported read invoked replay RPC")
	}
}

func TestStrictProjectionRefusesProtectedChildSubscription(t *testing.T) {
	for _, generation := range []string{"current", "historical"} {
		t.Run(generation, func(t *testing.T) {
			r := chronicle.NewRegistry()
			root, err := chronicle.RegisterEvent[subscriptionRemoved](r)
			if err != nil {
				t.Fatal(err)
			}
			protection := events.WithProtection(compliance.Property("Name", compliance.Classification{PII: true}))
			options := []events.TypeOption{events.WithGeneration(2)}
			if generation == "current" {
				options = append(options, protection)
			}
			childEvent, err := chronicle.RegisterEvent[subscriptionAdded](r, options...)
			if err != nil {
				t.Fatal(err)
			}
			if generation == "historical" {
				if _, err := chronicle.RegisterEventGeneration[subscriptionMarker](r, childEvent, 1, protection); err != nil {
					t.Fatal(err)
				}
			}
			model, err := chronicle.RegisterReadModel[subscriptionModel](r)
			if err != nil {
				t.Fatal(err)
			}
			b := projections.NewBuilder("protected-child", model)
			projections.From(b, root, nil)
			projections.Children(b, projections.Path[subscriptionModel, []subscriptionChild]("children"), func(child *projections.Builder[subscriptionChild]) {
				projections.From(child, childEvent, nil)
			})
			d, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
			if err := r.AddProjection(d); err != nil {
				t.Fatal(err)
			}
			assertStrictUnsupportedBeforeIO[subscriptionModel](t, r)
		})
	}
}
