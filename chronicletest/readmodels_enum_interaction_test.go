// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/clientoptions"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type subscriptionEnum int32

func (subscriptionEnum) MarshalJSON() ([]byte, error) {
	rejectedCodecCalls.Add(1)
	return nil, subscriptionHostileError{}
}
func (subscriptionEnum) MarshalText() ([]byte, error) {
	rejectedCodecCalls.Add(1)
	return nil, subscriptionHostileError{}
}

type subscriptionEnumSeed struct {
	State  subscriptionEnum
	Secret subscriptionSecret
}
type subscriptionEnumEntered struct{ State subscriptionEnum }
type subscriptionEnumModel struct {
	ID    string `chronicle:"key"`
	State subscriptionEnum
}

func subscriptionEnumCodecs(t *testing.T) *serialization.Codecs {
	t.Helper()
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[subscriptionEnum]{Name: "One", Value: 1}))
	if err != nil {
		t.Fatal(err)
	}
	return codecs
}

func TestStrictProjectionRejectsEnumSeedBeforeEncodingOrProviders(t *testing.T) {
	f := newSubscriptionFixture(t, true, "ordinary")
	beforeUnary, beforeStreams := f.connection.unaryCalls.Load(), f.connection.streamCalls.Load()
	rejectedCodecCalls.Store(0)
	rejectedErrorHooks.Store(0)
	// Even an invalid member in a registered unrelated event is a subscription
	// error first; neither its enum nor its hostile ordinary concept is encoded.
	for _, state := range []subscriptionEnum{1, 99} {
		if err := f.scenario.Given(t.Context(), "source", subscriptionEnumSeed{State: state, Secret: "dummy"}); !errors.Is(err, ErrUnsubscribedEventSeeded) {
			t.Fatalf("unsubscribed enum seed = %v", err)
		}
	}
	if f.connection.unaryCalls.Load() != beforeUnary || f.connection.streamCalls.Load() != beforeStreams || f.appendServer.calls != 0 || f.providers.Load() != 0 || len(f.scenario.history) != 0 || rejectedCodecCalls.Load() != 0 || rejectedErrorHooks.Load() != 0 {
		t.Fatal("strict enum seed caused effects")
	}
}

func TestStrictEnumVariantRefusesBeforeConnection(t *testing.T) {
	codecs := subscriptionEnumCodecs(t)
	r := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[subscriptionEnumEntered](r, events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[subscriptionEnumModel](r, readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(model, projections.VariantOf[subscriptionVariantIdentity](), projections.EntersOn(event))); err != nil {
		t.Fatal(err)
	}
	scenario, err := OpenReadModelScenario[subscriptionEnumModel](t.Context(), Config{Registry: r, Engine: Kernel, ConnectionString: "chronicle://127.0.0.1:1"}, ReadModelOptions[subscriptionEnumModel]{StrictEventSubscription: true})
	if scenario != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("strict enum variant = %v, %v", scenario, err)
	}
	connection := &subscriptionConnection{substituteTransport: substituteConnection()}
	client, err := chronicle.NewClient(chronicle.WithRegistry(r), clientoptions.Connection[chronicle.ClientOption](connection), chronicle.WithNoAuthentication(), chronicle.WithNamingPolicy(serialization.CamelCase))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	})
	artifacts, err := client.Artifacts("enum-variant")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := strictProjectionSubscription(artifacts.Projections[0], artifacts.Events)
	if selected != nil || !errors.Is(err, chronicle.ErrUnsupported) || !artifacts.Projections[0].IsVariant() || connection.unaryCalls.Load() != 0 || connection.streamCalls.Load() != 0 {
		t.Fatal("variant classification or pre-connect refusal lost")
	}
}
