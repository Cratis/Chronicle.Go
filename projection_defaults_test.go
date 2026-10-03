// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/serialization"
)

type DefaultsRegisteredEvent struct{ Name string }
type DefaultsRegisteredChild struct{ City string }
type DefaultsRegisteredModel struct {
	ID       string
	Name     string
	Children []DefaultsRegisteredChild
	Lookup   map[string]DefaultsRegisteredChild
	Address  DefaultsRegisteredChild
	Explicit string `json:"stable"`
	Note     *string
}

type DefaultsPromoted struct{ Name string }
type DefaultsShadowedModel struct {
	DefaultsPromoted
	Other string `json:"name"`
}

func TestProjectionInitialStateRejectsLostFieldBeforeClientIO(t *testing.T) {
	options := map[string]projections.Option{
		"scalar promoted field": projections.WithInitialValue(projections.Path[DefaultsShadowedModel, string]("Name"), "promoted"),
		"whole model":           projections.WithInitialValues(DefaultsShadowedModel{DefaultsPromoted: DefaultsPromoted{Name: "promoted"}, Other: "other"}),
	}
	for name, option := range options {
		t.Run(name, func(t *testing.T) {
			registry := chronicle.NewRegistry()
			event, err := chronicle.RegisterEvent[DefaultsRegisteredEvent](registry)
			if err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[DefaultsShadowedModel](registry)
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.AddProjection(projections.ModelBound(model, projections.FromEvent(event), option)); err != nil {
				t.Fatal(err)
			}
			// Construction must fail even offline: no Connect, resolver or RPC
			// is needed to detect the naming-policy promotion change.
			client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithNamingPolicy(serialization.CamelCase))
			var configuration *projections.DeclarationError
			if client != nil || !errors.Is(err, chronicle.ErrInvalidConfiguration) || !errors.As(err, &configuration) || configuration.GoField != "DefaultsPromoted.Name" {
				t.Fatalf("client=%v error=%v", client, err)
			}
		})
	}
}

func TestProjectionInitialStateUsesEveryFrozenStoreNamingPlan(t *testing.T) {
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[DefaultsRegisteredEvent](registry)
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[DefaultsRegisteredModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	values := DefaultsRegisteredModel{Children: []DefaultsRegisteredChild{{City: "Oslo"}}, Lookup: map[string]DefaultsRegisteredChild{"City": {City: "Bergen"}}, Address: DefaultsRegisteredChild{City: "Trondheim"}, Explicit: "fixed"}
	if err = registry.AddProjection(projections.ModelBound(model, projections.FromEvent(event), projections.WithInitialValues(values), projections.WithInitialValue(projections.Path[DefaultsRegisteredModel, *string]("Note"), nil))); err != nil {
		t.Fatal(err)
	}
	values.Children[0].City = "mutated"
	values.Lookup["City"] = DefaultsRegisteredChild{City: "mutated"}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithRegistryForStore("selected", registry), chronicle.WithNamingPolicy(policy))
		if err != nil {
			t.Fatal(err)
		}
		for _, store := range []chronicle.StoreName{"default", "selected"} {
			artifacts, err := client.Artifacts(store)
			if err != nil {
				t.Fatal(err)
			}
			state := artifacts.Projections[0].KernelDefinition().InitialModelState
			want := `{"Address":{"City":"Trondheim"},"Children":[{"City":"Oslo"}],"Id":"","Lookup":{"City":{"City":"Bergen"}},"Name":"","Note":null,"stable":"fixed"}`
			if policy != serialization.PreservePropertyNames {
				want = `{"address":{"city":"Trondheim"},"children":[{"city":"Oslo"}],"id":"","lookup":{"City":{"city":"Bergen"}},"name":"","note":null,"stable":"fixed"}`
			}
			if state != want {
				t.Errorf("store=%s policy=%v state=%s", store, policy, state)
			}
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
