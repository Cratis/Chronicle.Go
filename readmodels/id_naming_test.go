// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func checkIDNaming[T any](t *testing.T, names map[serialization.NamingPolicy]string) {
	t.Helper()
	model, err := Define[T]()
	if err != nil {
		t.Fatal(err)
	}
	for policy, name := range names {
		descriptor, err := model.Descriptor().WithNamingPolicy(policy)
		if err != nil {
			t.Fatal(err)
		}
		if got := idProperty(descriptor); got != name {
			t.Fatalf("policy %v: ID path = %q, want %q", policy, got, name)
		}
		data, err := normalizeID([]byte(`{"_id":"owner","__subject":"lineage"}`), descriptor)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]string
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		if fields[name] != "owner" || fields["__subject"] != "lineage" {
			t.Fatalf("policy %v: %s", policy, data)
		}
		if _, ok := fields["_id"]; ok {
			t.Fatal("sink alias retained")
		}
	}
}

func TestReadModelIDPathsRebindFromPlan(t *testing.T) {
	type protectedModel struct {
		ID   string
		Name string
	}
	model, err := Define[protectedModel](WithIndexes("Id"), WithPII("Name"), WithSubjectProperty("Id"))
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		d, err := model.Descriptor().WithNamingPolicy(policy)
		if err != nil {
			t.Fatal(err)
		}
		fields := d.Fields()
		if d.Indexes()[0] != fields[0].Path || d.definition.config.subject != fields[0].Path || d.definition.config.pii[0] != fields[1].Path {
			t.Fatalf("policy %v configuration not bound to fields: %+v / %+v", policy, d.definition.config, fields)
		}
		data, err := d.Marshal(protectedModel{ID: "owner", Name: "ciphertext"})
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]string
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Fatal(err)
		}
		if payload[d.definition.config.subject] != "owner" || payload[d.definition.config.pii[0]] != "ciphertext" {
			t.Fatalf("payload: %s", data)
		}
	}
	if _, err := Define[protectedModel](WithPII("Id")); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("key PII: %v", err)
	}
}

func TestReadModelDescriptorRejectsRootIDCollision(t *testing.T) {
	type collision struct {
		ID    string
		Other string `json:"Id"`
	}
	if _, err := Define[collision](); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("default collision: %v", err)
	}
	type policyCollision struct {
		ID    string
		Other string `json:"id"`
	}
	model, err := Define[policyCollision]()
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.CamelCase, serialization.LegacyGoCamelCase} {
		if _, err := model.Descriptor().WithNamingPolicy(policy); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("policy %v collision: %v", policy, err)
		}
	}
}

func TestRootIDNamingUsesBoundPlan(t *testing.T) {
	type acronymModel struct{ ID string }
	type pascalModel struct{ Id string }
	type taggedModel struct {
		ID string `json:"customerId"`
	}
	type keyedModel struct {
		ID  string
		Key string `json:"customerKey" chronicle:"key"`
	}
	type nestedModel struct{ Child struct{ ID string } }
	t.Run("acronym", func(t *testing.T) {
		checkIDNaming[acronymModel](t, map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "Id", serialization.CamelCase: "id", serialization.LegacyGoCamelCase: "id"})
	})
	t.Run("pascal", func(t *testing.T) {
		checkIDNaming[pascalModel](t, map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "Id", serialization.CamelCase: "id"})
	})
	t.Run("explicit tag", func(t *testing.T) {
		checkIDNaming[taggedModel](t, map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "customerId", serialization.CamelCase: "customerId"})
	})
	t.Run("declared key", func(t *testing.T) {
		checkIDNaming[keyedModel](t, map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "customerKey", serialization.CamelCase: "customerKey"})
	})
	model, err := Define[nestedModel]()
	if err != nil {
		t.Fatal(err)
	}
	if idProperty(model.Descriptor()) != "" {
		t.Fatal("nested ID selected as root ID")
	}
}
