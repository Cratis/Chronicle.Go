// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type rootIDModel struct {
	ID         string
	CustomerID string
	Nested     struct{ ID string }
}

func TestReadModelPlanTranslatesOnlyUntaggedRootID(t *testing.T) {
	for _, tc := range []struct {
		policy                                    serialization.NamingPolicy
		root, customer, nested, nestedID, eventID string
	}{
		{serialization.PreservePropertyNames, "Id", "CustomerID", "Nested", "ID", "ID"},
		{serialization.CamelCase, "id", "customerID", "nested", "ID", "ID"},
		{serialization.LegacyGoCamelCase, "id", "customerID", "nested", "id", "id"},
	} {
		t.Run(tc.root+"/"+tc.eventID, func(t *testing.T) {
			plan, err := serialization.CompileReadModel(reflect.TypeFor[rootIDModel](), tc.policy)
			if err != nil {
				t.Fatal(err)
			}
			value := rootIDModel{ID: "owner", CustomerID: "customer"}
			value.Nested.ID = "child"
			data, err := plan.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]any
			if err := json.Unmarshal(data, &payload); err != nil {
				t.Fatal(err)
			}
			if len(payload) != 3 || payload[tc.root] != "owner" || payload[tc.customer] != "customer" || payload[tc.nested].(map[string]any)[tc.nestedID] != "child" {
				t.Fatalf("payload = %s", data)
			}
			fields := plan.Fields()
			if fields[0].GoField != "ID" || fields[0].Name != tc.root || fields[0].Path != tc.root {
				t.Fatalf("ID metadata = %+v", fields[0])
			}
			var schema struct {
				Properties map[string]any
				Required   []string
			}
			if err := json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
				t.Fatal(err)
			}
			if len(schema.Properties) != 3 || schema.Properties[tc.root] == nil || schema.Required[0] != tc.root {
				t.Fatalf("schema = %s", plan.Schema())
			}
			eventPlan, err := serialization.Compile(reflect.TypeFor[rootIDModel](), tc.policy)
			if err != nil {
				t.Fatal(err)
			}
			eventData, err := eventPlan.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var event map[string]any
			if err := json.Unmarshal(eventData, &event); err != nil {
				t.Fatal(err)
			}
			if event[tc.eventID] != "owner" {
				t.Fatalf("event naming changed: %s", eventData)
			}
		})
	}
}

func TestReadModelPlanHonorsEveryExplicitJSONTag(t *testing.T) {
	for _, tag := range []string{`json:"ID"`, `json:"customerId"`, `json:",omitempty"`, `json:""`} {
		typ := reflect.StructOf([]reflect.StructField{{Name: "ID", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(tag)}})
		for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
			model, err := serialization.CompileReadModel(typ, policy)
			if err != nil {
				t.Fatal(err)
			}
			ordinary, err := serialization.Compile(typ, policy)
			if err != nil {
				t.Fatal(err)
			}
			if model.Schema() != ordinary.Schema() || !reflect.DeepEqual(model.Fields(), ordinary.Fields()) {
				t.Fatalf("explicit %s changed under policy %v", tag, policy)
			}
		}
	}
}

func TestReadModelPlanChecksFinalPropertyNames(t *testing.T) {
	type distinctNames struct {
		ID    string
		Other string `json:"ID"`
	}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		plan, err := serialization.CompileReadModel(reflect.TypeFor[distinctNames](), policy)
		if err != nil {
			t.Fatal(err)
		}
		data, err := plan.Marshal(distinctNames{ID: "owner", Other: "other"})
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]string
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Fatal(err)
		}
		name := "id"
		if policy == serialization.PreservePropertyNames {
			name = "Id"
		}
		if len(payload) != 2 || payload[name] != "owner" || payload["ID"] != "other" {
			t.Fatalf("final names: %s", data)
		}
	}
}

func TestReadModelPlanRejectsTranslatedRootIDCollisions(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		name := "id"
		if policy == serialization.PreservePropertyNames {
			name = "Id"
		}
		typ := reflect.StructOf([]reflect.StructField{
			{Name: "ID", Type: reflect.TypeFor[string]()},
			{Name: "Other", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"` + name + `"`)},
		})
		if _, err := serialization.CompileReadModel(typ, policy); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("policy %v collision: %v", policy, err)
		}
	}
	type twoIDs struct {
		ID string
		Id string
	}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		if _, err := serialization.CompileReadModel(reflect.TypeFor[twoIDs](), policy); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("policy %v Go-name collision: %v", policy, err)
		}
	}
}
