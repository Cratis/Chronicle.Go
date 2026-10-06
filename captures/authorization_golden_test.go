// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

func inboundAuthenticationCases(t *testing.T, operation string, count int) []map[string]any {
	t.Helper()
	data, provenance, inventory := authenticationFiles(t)
	if authenticationHash(data) != provenance.RawSHA256 {
		t.Fatal("authentication profile: raw SHA256")
	}
	if err := validateAuthenticationFixture(data, provenance, inventory); err != nil {
		t.Fatal(err)
	}
	parsed, err := authenticationJSON(data)
	if err != nil {
		t.Fatal("authentication profile: JSON")
	}
	var cases []map[string]any
	for _, entry := range parsed.(map[string]any)["cases"].([]any) {
		item := entry.(map[string]any)
		if item["surface"] == "inbound" && item["operation"] == operation {
			cases = append(cases, item)
		}
	}
	if len(cases) != count {
		t.Fatal("authentication profile: selected case count")
	}
	return cases
}

func syntheticWebhookOptions(id string) []WebhookOption {
	basic := WithBasicAuth("synthetic-user", "synthetic-password")
	bearer := WithBearerToken("synthetic-token")
	oauth := WithOAuth("https://synthetic.invalid/oauth", "synthetic-client", "synthetic-secret")
	switch id {
	case "basic":
		return []WebhookOption{basic}
	case "bearer":
		return []WebhookOption{bearer}
	case "oauth":
		return []WebhookOption{oauth}
	case "basic-then-bearer":
		return []WebhookOption{basic, bearer}
	case "bearer-then-basic":
		return []WebhookOption{bearer, basic}
	case "basic-then-oauth":
		return []WebhookOption{basic, oauth}
	default:
		return nil
	}
}

func TestSourceAuthorizationMatchesActualPackage(t *testing.T) {
	for _, item := range inboundAuthenticationCases(t, "JsonSerializer.SourceAuthorization", 8) {
		id := item["id"].(string)
		t.Run(item["namingPolicy"].(string)+"/"+id, func(t *testing.T) {
			authorization, present := Webhook("/synthetic-capture", syntheticWebhookOptions(id)...).Authorization()
			if present != (id != "none") {
				t.Fatal("authorization presence differs")
			}
			got, err := authorization.kernelJSON()
			if err != nil || !bytes.Equal(got, []byte(item["rawJSON"].(string))) {
				t.Fatal("authorization bytes differ from actual package")
			}
		})
	}
}

func TestWebhookSourceAuthorizationMatchesActualSerialize(t *testing.T) {
	for _, item := range inboundAuthenticationCases(t, "EventSerializer.Serialize", 16) {
		id := item["id"].(string)
		t.Run(item["namingPolicy"].(string)+"/"+id, func(t *testing.T) {
			source := Webhook("/synthetic-capture", syntheticWebhookOptions(id)...)
			definition, err := new(Builder).From(source).Key("id").Build("SyntheticCapture")
			if len(syntheticWebhookOptions(id)) > 1 {
				// Go-specific refusal: C# keeps only the last choice. Go must
				// never silently discard any supplied authorization option.
				if !errors.Is(err, faults.ErrInvalidConfiguration) {
					t.Fatal("replacement authorization was not refused")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			authorization, present := definition.Authorization()
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(item["rawJSON"].(string)), &fields); err != nil {
				t.Fatal("source fixture JSON")
			}
			key := "Authorization"
			if item["namingPolicy"] == "CamelCaseNamingPolicy" {
				key = "authorization"
			}
			if id == "default-null" {
				if present || fields[key] != nil {
					t.Fatal("default authorization was not absent")
				}
				return
			}
			// Explicit None is not authorable; compare its zero-value bytes
			// only, never turn it into a present source authorization.
			if present != (id != "explicit-none") {
				t.Fatal("authorization presence differs")
			}
			got, err := authorization.kernelJSON()
			if err != nil || !bytes.Equal(got, fields[key]) {
				t.Fatal("nested authorization bytes differ from actual package")
			}
		})
	}
}

func TestSourceAuthorizationHasNoDecoder(t *testing.T) {
	for _, item := range inboundAuthenticationCases(t, "EventSerializer.Deserialize", 24) {
		t.Run(item["namingPolicy"].(string)+"/"+item["id"].(string), func(t *testing.T) {
			raw := []byte(item["rawJSON"].(string))
			var source Source
			if err := json.Unmarshal(raw, &source); !errors.Is(err, faults.ErrUnsupported) {
				t.Fatal("source decoder did not fail closed")
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal("source fixture JSON")
			}
			key := "Authorization"
			if item["namingPolicy"] == "CamelCaseNamingPolicy" {
				key = "authorization"
			}
			authorization, _ := Webhook("/synthetic-capture", WithBearerToken("synthetic-token")).Authorization()
			original := authorization
			if err := json.Unmarshal(fields[key], &authorization); !errors.Is(err, faults.ErrUnsupported) || authorization != original {
				t.Fatal("authorization decoder accepted input or changed credentials")
			}
		})
	}
}
