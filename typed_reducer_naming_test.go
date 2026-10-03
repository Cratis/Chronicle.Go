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
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/serialization"
)

type plannedReducerModel struct {
	ID         string
	ExternalID string `json:"ID"`
	Items      []string
	Optional   *[]string
}

func TestActiveReducerInitialStateUsesFrozenNamingPlan(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		t.Run([]string{"preserve", "camel", "legacy"}[policy], func(t *testing.T) {
			r := NewRegistry()
			if _, err := RegisterEvent[FoldChanged](r); err != nil {
				t.Fatal(err)
			}
			m, err := RegisterReadModel[plannedReducerModel](r)
			if err != nil {
				t.Fatal(err)
			}
			want := plannedReducerModel{ID: "source", ExternalID: "external", Items: []string{}}
			if err := RegisterReducerHandlers(r, m, "fold", []reducers.Handler{reducers.On(func(_ context.Context, _ FoldChanged, current *plannedReducerModel, _ events.Context) (*plannedReducerModel, error) {
				if !reflect.DeepEqual(current, &want) {
					return nil, errors.New("initial state did not use registered naming/nullability plan")
				}
				return current, nil
			})}); err != nil {
				t.Fatal(err)
			}
			client, _, ctx, server, _ := reducerFixture(t, r, WithNamingPolicy(policy))
			receiveOpening(t, ctx, server.registrations)
			artifacts, err := client.Artifacts("store")
			if err != nil {
				t.Fatal(err)
			}
			d, _ := artifacts.ReadModels.LookupType(reflect.TypeFor[plannedReducerModel]())
			data, err := d.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			server.operations <- &contracts.ReduceOperationMessage{Partition: "source", InitialState: string(data), Events: []*contracts.AppendedEvent{foldEvent("FoldChanged", 0, `{"amount":1}`)}}
			result := receiveOpening(t, ctx, server.results)
			if result.State != contracts.ObservationState_Success {
				t.Fatal(result)
			}
			got, err := d.Unmarshal([]byte(result.ReadModelState))
			if err != nil || !reflect.DeepEqual(got, &want) {
				t.Fatalf("fold = %+v, %v", got, err)
			}
		})
	}
}
