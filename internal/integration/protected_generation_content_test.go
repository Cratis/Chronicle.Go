// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
)

var (
	errProtectedJSON      = errors.New("invalid alternate JSON object")
	errProtectedFields    = errors.New("missing, empty, or non-string protected field")
	errProtectedPlaintext = errors.New("protected fixture plaintext released")
)

// This predicate and its regressions deliberately have no integration build tag:
// the kernel witness must not accept missing values or JSON-escaped plaintext.
func protectedGenerationContentError(content json.RawMessage, fields []string) error {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(content, &document); err != nil {
		return errProtectedJSON
	}
	if document == nil {
		return errProtectedJSON
	}
	if len(fields) == 0 {
		return errProtectedFields
	}
	values := make([]string, 0, len(fields))
	for _, field := range fields {
		value, err := protectedGenerationField(document[field])
		if err != nil {
			return err
		}
		values = append(values, value)
	}
	if slices.ContainsFunc(values, releasedProtectedFixtureValue) {
		return errProtectedPlaintext
	}
	return nil
}

func protectedGenerationField(raw json.RawMessage) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", errProtectedFields
	}
	if strings.TrimSpace(value) == "" {
		return "", errProtectedFields
	}
	return value, nil
}

func releasedProtectedFixtureValue(value string) bool {
	switch value {
	case "Ada", "Lovelace", "Grace", "Hopper":
		return true
	}
	for _, plaintext := range []string{"Ada Lovelace", "Grace Hopper", "migrated-secret", "current-secret"} {
		if strings.Contains(value, plaintext) {
			return true
		}
	}
	return false
}

func TestProtectedGenerationContentPredicate(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		fields        []string
		wantErr       error
	}{
		{"missing name", `{"Secret":"ENC:abc123"}`, nil, errProtectedFields},
		{"missing secret", `{"Name":"ENC:def456"}`, nil, errProtectedFields},
		{"empty name", `{"Name":"","Secret":"ENC:abc123"}`, nil, errProtectedFields},
		{"empty secret", `{"Name":"ENC:def456","Secret":""}`, nil, errProtectedFields},
		{"empty object", `{}`, nil, errProtectedFields},
		{"no expected fields", `{"Name":"ENC:def456","Secret":"ENC:abc123"}`, []string{}, errProtectedFields},
		{"whitespace field", `{"Name":"ENC:def456","Secret":"  "}`, nil, errProtectedFields},
		{"null field", `{"Name":null,"Secret":"ENC:abc123"}`, nil, errProtectedFields},
		{"non-string field", `{"Name":123,"Secret":"ENC:abc123"}`, nil, errProtectedFields},
		{"escaped secret", `{"Name":"ENC:def456","Secret":"\u0063urrent-secret"}`, nil, errProtectedPlaintext},
		{"escaped name", `{"Name":"\u0041da Lovelace","Secret":"ENC:abc123"}`, nil, errProtectedPlaintext},
		{"embedded plaintext", `{"Name":"ENC:def456","Secret":"prefix migrated-secret suffix"}`, nil, errProtectedPlaintext},
		{"ciphertext-looking values", `{"Name":"ENC:def456","Secret":"ENC:abc123"}`, nil, nil},
		{"invalid JSON", `{`, nil, errProtectedJSON},
		{"null document", `null`, nil, errProtectedJSON},
		{"current missing last name", `{"FirstName":"ENC:def456","Secret":"ENC:abc123"}`, []string{"FirstName", "LastName", "Secret"}, errProtectedFields},
		{"current escaped first name", `{"FirstName":"\u0047race","LastName":"ENC:def456","Secret":"ENC:abc123"}`, []string{"FirstName", "LastName", "Secret"}, errProtectedPlaintext},
		{"current ciphertext", `{"FirstName":"ENC:ghi789","LastName":"ENC:def456","Secret":"ENC:abc123"}`, []string{"FirstName", "LastName", "Secret"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := tc.fields
			if fields == nil {
				fields = []string{"Name", "Secret"}
			}
			if err := protectedGenerationContentError(json.RawMessage(tc.content), fields); !errors.Is(err, tc.wantErr) {
				t.Fatalf("predicate = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
