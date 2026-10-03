// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/serialization"
)

type derivedReducerModel struct {
	ID     string
	Member derivedfixtures.Member
}

func TestActiveReducerUsesDerivedEventAndInitialStatePlans(t *testing.T) {
	registry := NewRegistry()
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterEvent[derivedfixtures.MembersChanged](registry, events.WithCodecs(codecs)); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[derivedReducerModel](registry, readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	want := derivedReducerModel{ID: "source", Member: derivedfixtures.Sample().Primary}
	if err := RegisterReducerHandlers(registry, model, "fold", []reducers.Handler{reducers.On(func(_ context.Context, event derivedfixtures.MembersChanged, current *derivedReducerModel, _ events.Context) (*derivedReducerModel, error) {
		if !reflect.DeepEqual(current, &want) || !reflect.DeepEqual(event, derivedfixtures.Sample()) {
			return nil, errors.New("derived fold bypassed registered plan")
		}
		return current, nil
	})}); err != nil {
		t.Fatal(err)
	}
	client, _, ctx, server, _ := reducerFixture(t, registry, WithNamingPolicy(serialization.CamelCase))
	receiveOpening(t, ctx, server.registrations)
	artifacts, err := client.Artifacts("store")
	if err != nil {
		t.Fatal(err)
	}
	d, _ := artifacts.ReadModels.LookupIdentifier(model.Identifier())
	initial, err := d.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	event, _ := artifacts.Events.Lookup(derivedfixtures.Sample())
	data, err := event.Marshal(derivedfixtures.Sample())
	if err != nil {
		t.Fatal(err)
	}
	server.operations <- &contracts.ReduceOperationMessage{Partition: "source", InitialState: string(initial), Events: []*contracts.AppendedEvent{foldEvent("MembersChanged", 0, string(data))}}
	result := receiveOpening(t, ctx, server.results)
	if result.State != contracts.ObservationState_Success {
		t.Fatal(result)
	}
	got, err := d.Unmarshal([]byte(result.ReadModelState))
	if err != nil || !reflect.DeepEqual(got, &want) {
		t.Fatalf("reducer: %#v %v", got, err)
	}
}

type DecisionDerivedChanged struct {
	Member derivedfixtures.Member `json:"member"`
}
type DecisionDerivedModel struct {
	ID     string                 `json:"id" chronicle:"key"`
	Member derivedfixtures.Member `json:"member" chronicle:"set(DecisionDerivedChanged)"`
}

func TestDecisionDerivedDecodeFailureIssuesNoModelOrToken(t *testing.T) {
	for name, document := range map[string]string{
		"valid":                                  `{"id":"source","member":{"count":42,"_derivedTypeId":"robot"}}`,
		"missing discriminator":                  `{"id":"source","member":{"count":42}}`,
		"duplicate parent":                       `{"_id":"source","member":{"children":[{"_derivedTypeId":"robot","_derivedTypeId":"human"}],"children":[],"_derivedTypeId":"human"}}`,
		"duplicate root before ID normalization": `{"_id":"source","member":{"_derivedTypeId":"human"},"member":{"_derivedTypeId":"robot"}}`,
		"unknown array duplicate":                `{"_id":"source","unknown":[[{"secret":1,"secret":2}]]}`,
	} {
		t.Run(name, func(t *testing.T) {
			registry := NewRegistry()
			codecs, err := derivedfixtures.Codecs()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := RegisterEvent[DecisionDerivedChanged](registry, events.WithCodecs(codecs)); err != nil {
				t.Fatal(err)
			}
			model, err := RegisterReadModel[DecisionDerivedModel](registry, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			kernel := &supervisedKernel{}
			client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
			raw := &decisionCodecConn{ClientConnInterface: &decisionProfileConn{ClientConnInterface: client.config.borrowed, version: "19.29.4", protocol: "19.29.4"}, document: document}
			client.config.borrowed = raw
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			read, err := readmodels.DecisionsFor(store.ReadModels(), model).GetDetached(ctx, "source")
			if raw.cleaned.Load() != 1 {
				t.Fatal("decision cleanup missing")
			}
			if name == "valid" {
				if err != nil || read.Token.IsZero() || read.Instance.Value.Member != (derivedfixtures.RobotValue{Count: 42}) {
					t.Fatalf("decision: %+v %v", read, err)
				}
			} else if !errors.Is(err, ErrProtocol) || !read.Token.IsZero() || read.Instance.Exists {
				t.Fatalf("failed decision leaked state: %+v %v", read, err)
			}
		})
	}
}
