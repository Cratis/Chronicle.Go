//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

func assertEnumKernelNames(t *testing.T, capture enumKernelCapture, policy serialization.NamingPolicy, field, raw string, expected json.RawMessage) {
	t.Helper()
	var payload, snapshot map[string]any
	if json.Unmarshal([]byte(raw), &payload) != nil || json.Unmarshal(expected, &snapshot) != nil {
		t.Fatal("invalid enum JSON")
	}
	key := field
	if policy == serialization.CamelCase {
		if key == "Value" {
			key = "value"
		} else {
			key = "values"
		}
	}
	var names func(any) any
	names = func(value any) any {
		switch value := value.(type) {
		case nil:
			return nil
		case []any:
			result := make([]any, len(value))
			for i, item := range value {
				result[i] = names(item)
			}
			return result
		case map[string]any:
			for _, e := range capture.Enums {
				if e.Name != value["declaredType"] {
					continue
				}
				for _, member := range e.Members {
					if member.Numeric == value["numeric"] {
						return member.Name
					}
				}
			}
		}
		t.Fatal("expected enum member not captured")
		return nil
	}
	want := names(snapshot[field])
	if !reflect.DeepEqual(payload[key], want) {
		t.Fatalf("kernel did not preserve declared enum names: %s want %v", raw, want)
	}
}
