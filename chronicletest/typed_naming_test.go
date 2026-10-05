// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/serialization"
)

type scenarioPlannedModel struct {
	ID         string
	ExternalID string `json:"ID"`
	Items      []string
	Optional   *[]string
}

func TestReducerScenarioUsesFrozenPlanForInitialAndResult(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		for _, initial := range []bool{false, true} {
			name := "result"
			if initial {
				name = "initial"
			}
			t.Run([]string{"preserve", "camel", "legacy"}[policy]+"/"+name, func(t *testing.T) {
				r := eventRegistry(t)
				m, err := chronicle.RegisterReadModel[scenarioPlannedModel](r)
				if err != nil {
					t.Fatal(err)
				}
				want := scenarioPlannedModel{ID: "a", ExternalID: "external", Items: []string{}}
				if err := chronicle.RegisterReducerHandlers(r, m, "planned", []reducers.Handler{reducers.On(func(_ context.Context, _ AccountOpened, current *scenarioPlannedModel, _ events.Context) (*scenarioPlannedModel, error) {
					if initial && !reflect.DeepEqual(current, &want) {
						return nil, errors.New("scenario initial state bypassed naming plan")
					}
					return &want, nil
				})}); err != nil {
					t.Fatal(err)
				}
				options := chronicletest.ReadModelOptions[scenarioPlannedModel]{}
				if initial {
					options.Initial = &want
				}
				s := chronicletest.NewReadModelScenario[scenarioPlannedModel](t, chronicletest.Config{Registry: r, Naming: policy}, options)
				if err := s.Given(t.Context(), "a", AccountOpened{Name: "Ada"}); err != nil {
					t.Fatal(err)
				}
				got, err := s.Instance(t.Context())
				if err != nil || !got.Exists || !reflect.DeepEqual(got.Value, want) {
					t.Fatalf("scenario = %+v, %v", got, err)
				}
			})
		}
	}
}
