// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"os"
	"testing"
)

// Typed overlays preserve raw results (including omitted fields) while each
// regression mutates a fresh in-memory copy of the immutable package capture.
type enumCaptureValidationDocument struct {
	enumCaptureFixture
	Isolation json.RawMessage                `json:"isolation,omitempty"`
	Profiles  []enumCaptureValidationProfile `json:"profiles"`
}

type enumCaptureValidationProfile struct {
	enumCaptureProfile
	Schemas []enumCaptureValidationSchema `json:"schemas"`
	Cases   []enumCaptureValidationCase   `json:"cases"`
}

type enumCaptureValidationSchema struct {
	enumCaptureSchema
	Result json.RawMessage `json:"result"`
}

type enumCaptureValidationCase struct {
	enumCaptureCase
	Result map[string]json.RawMessage `json:"result"`
}

func TestEnumCaptureValidationRejectsIncompleteEvidence(t *testing.T) {
	data, err := os.ReadFile("testdata/enum/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseEnumCaptureFixture(data); err != nil {
		t.Fatalf("original capture: %v", err)
	}
	mutations := []struct {
		name   string
		mutate func(*testing.T, *enumCaptureValidationProfile)
	}{
		{"missing-reserialize", func(t *testing.T, p *enumCaptureValidationProfile) {
			delete(acceptedEnumValidationCase(t, p, "EventSerializer.Deserialize").Result, "reserialize")
		}},
		{"null-reserialize", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "EventSerializer.Deserialize").Result["reserialize"] = json.RawMessage(`null`)
		}},
		{"missing-toJson", func(t *testing.T, p *enumCaptureValidationProfile) {
			delete(acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").Result, "toJson")
		}},
		{"null-toJson", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").Result["toJson"] = json.RawMessage(`null`)
		}},
		{"missing-reserialize-status", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "EventSerializer.Deserialize").Result["reserialize"] = json.RawMessage(`{}`)
		}},
		{"missing-toJson-status", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").Result["toJson"] = json.RawMessage(`{}`)
		}},
		{"invalid-reserialize-error", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "EventSerializer.Deserialize").Result["reserialize"] = json.RawMessage(`{"status":"error"}`)
		}},
		{"invalid-toJson-error", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").Result["toJson"] = json.RawMessage(`{"status":"error"}`)
		}},
		{"renamed-deserialize-operation", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "EventSerializer.Deserialize").Operation = "EventSerializer.OtherDeserialize"
		}},
		{"renamed-expando-operation", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").Operation = "ExpandoObjectConverter.Other"
		}},
		{"deserialize-schemaAPI", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "EventSerializer.Deserialize").SchemaAPI = "Generate"
		}},
		{"serialize-schemaAPI", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "EventSerializer.Serialize").SchemaAPI = "GenerateForReadModel"
		}},
		{"wrong-unique-schema-operation", func(_ *testing.T, p *enumCaptureValidationProfile) { p.Schemas[0].Operation = "OtherGenerate" }},
		{"missing-schema", func(_ *testing.T, p *enumCaptureValidationProfile) { p.Schemas = p.Schemas[1:] }},
		{"extra-schema", func(_ *testing.T, p *enumCaptureValidationProfile) {
			extra := p.Schemas[0]
			extra.Operation = "OtherGenerate"
			p.Schemas = append(p.Schemas, extra)
		}},
		{"schema-declared-type-mismatch", func(_ *testing.T, p *enumCaptureValidationProfile) { p.Schemas[0].DeclaredType = "Scalar<OtherEnum>" }},
		{"duplicate-schema-replaces-required-pair", func(_ *testing.T, p *enumCaptureValidationProfile) { p.Schemas[1] = p.Schemas[0] }},
		{"expando-wrong-schemaAPI", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").SchemaAPI = "OtherGenerate"
		}},
		{"expando-missing-schemaAPI", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").SchemaAPI = ""
		}},
		{"expando-type-unlinked", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").DeclaredType = "Scalar<OtherEnum>"
		}},
		{"expando-empty-type", func(t *testing.T, p *enumCaptureValidationProfile) {
			acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject").DeclaredType = ""
		}},
		{"error-with-reserialize", func(t *testing.T, p *enumCaptureValidationProfile) {
			c := acceptedEnumValidationCase(t, p, "EventSerializer.Deserialize")
			c.Result = map[string]json.RawMessage{"status": json.RawMessage(`"error"`), "category": json.RawMessage(`"System.OverflowException"`), "reserialize": c.Result["reserialize"]}
		}},
		{"error-with-toJson", func(t *testing.T, p *enumCaptureValidationProfile) {
			c := acceptedEnumValidationCase(t, p, "ExpandoObjectConverter.ToExpandoObject/ToJsonObject")
			c.Result = map[string]json.RawMessage{"status": json.RawMessage(`"error"`), "category": json.RawMessage(`"System.OverflowException"`), "toJson": c.Result["toJson"]}
		}},
	}
	for policy := range 2 {
		for _, mutation := range mutations {
			t.Run([]string{"default", "camelCase"}[policy]+"/"+mutation.name, func(t *testing.T) {
				var document enumCaptureValidationDocument
				if err := json.Unmarshal(data, &document); err != nil {
					t.Fatal(err)
				}
				mutation.mutate(t, &document.Profiles[policy])
				mutated, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := parseEnumCaptureFixture(mutated); err == nil {
					t.Fatal("incomplete capture accepted")
				}
			})
		}
	}
}

func TestEnumCaptureValidationRequiresRecordedRegistryZero(t *testing.T) {
	data, err := os.ReadFile("testdata/enum/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		name      string
		isolation json.RawMessage
	}{
		{"missing-isolation", nil},
		{"null-isolation", json.RawMessage(`null`)},
		{"wrong-type-isolation", json.RawMessage(`0`)},
		{"missing-registryCalls", json.RawMessage(`{}`)},
		{"null-registryCalls", json.RawMessage(`{"registryCalls":null}`)},
		{"nonzero-registryCalls", json.RawMessage(`{"registryCalls":1}`)},
		{"negative-registryCalls", json.RawMessage(`{"registryCalls":-1}`)},
		{"string-registryCalls", json.RawMessage(`{"registryCalls":"0"}`)},
		{"boolean-registryCalls", json.RawMessage(`{"registryCalls":false}`)},
		{"fractional-registryCalls", json.RawMessage(`{"registryCalls":0.5}`)},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			var document enumCaptureValidationDocument
			if err := json.Unmarshal(data, &document); err != nil {
				t.Fatal(err)
			}
			document.Isolation = mutation.isolation
			mutated, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseEnumCaptureFixture(mutated); err == nil {
				t.Fatal("unrecorded or nonzero registry count accepted")
			}
		})
	}
}

func TestEnumCaptureValidationAcceptsIndependentFailures(t *testing.T) {
	data, err := os.ReadFile("testdata/enum/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := parseEnumCaptureFixture(data)
	if err != nil {
		t.Fatalf("original capture: %v", err)
	}
	for _, profile := range fixture.Profiles {
		failures := 0
		for _, c := range profile.Cases {
			if c.Result.Status == "accepted" && c.Result.Reserialize != nil && c.Result.Reserialize.Status == "error" {
				failures++
			}
		}
		if failures != 9 {
			t.Fatalf("%s captured reserialization failures = %d, want 9", profile.NamingPolicy, failures)
		}
	}
	for policy := range 2 {
		for _, operation := range []string{"original-copy", "EventSerializer.Deserialize", "ExpandoObjectConverter.ToExpandoObject/ToJsonObject", "Generate", "GenerateForReadModel"} {
			t.Run([]string{"default", "camelCase"}[policy]+"/"+operation, func(t *testing.T) {
				var document enumCaptureValidationDocument
				if err := json.Unmarshal(data, &document); err != nil {
					t.Fatal(err)
				}
				profile := &document.Profiles[policy]
				failure := json.RawMessage(`{"status":"error","category":"System.OverflowException","innerCategory":null}`)
				switch operation {
				case "EventSerializer.Deserialize":
					acceptedEnumValidationCase(t, profile, operation).Result["reserialize"] = failure
				case "ExpandoObjectConverter.ToExpandoObject/ToJsonObject":
					acceptedEnumValidationCase(t, profile, operation).Result["toJson"] = failure
				case "Generate":
					profile.Schemas[0].Result = failure
				case "GenerateForReadModel":
					profile.Schemas[1].Result = failure
				}
				mutated, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := parseEnumCaptureFixture(mutated); err != nil {
					t.Fatalf("complete captured failure rejected: %v", err)
				}
			})
		}
	}
}

func acceptedEnumValidationCase(t *testing.T, profile *enumCaptureValidationProfile, operation string) *enumCaptureValidationCase {
	t.Helper()
	for i := range profile.Cases {
		c := &profile.Cases[i]
		if c.Operation == operation && string(c.Result["status"]) == `"accepted"` {
			return c
		}
	}
	t.Fatalf("original capture has no accepted %s", operation)
	return nil
}
