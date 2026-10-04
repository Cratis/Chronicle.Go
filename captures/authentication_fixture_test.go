// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/webhooks"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const authenticationFixtureRoot = "testdata/authentication/"
const authenticationProvenanceSHA256 = "f9deb351748480f4f41317f3f6b774ca88b73e19d0ef5a0f2541d6b456ac06e7"

type authenticationProvenance struct {
	RawSHA256       string         `json:"rawSHA256"`
	InventorySHA256 string         `json:"inventorySHA256"`
	Metadata        map[string]any `json:"metadata"`
}

type authenticationIdentity struct{ surface, policy, operation, id string }

type authenticationInventory struct {
	Count  int `json:"count"`
	Groups []struct {
		Surface        string   `json:"surface"`
		NamingPolicies []string `json:"namingPolicies"`
		Operation      string   `json:"operation"`
		Cases          []string `json:"cases"`
	} `json:"groups"`
}

func TestAuthenticationPackageEvidence(t *testing.T) {
	data, provenance, inventory := authenticationFiles(t)
	if err := validateAuthenticationFixture(data, provenance, inventory); err != nil {
		t.Fatal(err)
	}
	if authenticationHash(data) != provenance.RawSHA256 {
		t.Fatal("authentication profile: raw SHA256")
	}
	entries, ok := provenance.Metadata["harness"].([]any)
	if !ok || len(entries) != 5 {
		t.Fatal("authentication profile: harness inventory")
	}
	for _, entry := range entries {
		item, ok := entry.(map[string]any)
		if !ok {
			t.Fatal("authentication profile: harness entry")
		}
		name, ok := item["name"].(string)
		if !ok || strings.ContainsAny(name, "/\\") {
			t.Fatal("authentication profile: harness name")
		}
		file, err := os.ReadFile(authenticationFixtureRoot + "capture/" + name)
		if err != nil || authenticationHash(file) != item["sha256"] {
			t.Fatal("authentication profile: harness SHA256")
		}
	}
}

func authenticationFiles(t *testing.T) ([]byte, authenticationProvenance, authenticationInventory) {
	t.Helper()
	read := func(name string) []byte {
		t.Helper()
		data, err := os.ReadFile(authenticationFixtureRoot + name)
		if err != nil {
			t.Fatal("authentication fixture: file unavailable")
		}
		return data
	}
	var p authenticationProvenance
	var inventory authenticationInventory
	provenance := read("provenance.json")
	// Pins anchor provenance independently from the observation being admitted.
	if authenticationHash(provenance) != authenticationProvenanceSHA256 {
		t.Fatal("authentication fixture: provenance SHA256")
	}
	if err := json.Unmarshal(provenance, &p); err != nil {
		t.Fatal("authentication fixture: provenance JSON")
	}
	manifest := read("case-inventory.json")
	if authenticationHash(manifest) != p.InventorySHA256 {
		t.Fatal("authentication fixture: inventory SHA256")
	}
	if err := json.Unmarshal(manifest, &inventory); err != nil || inventory.Count != 55 {
		t.Fatal("authentication fixture: inventory")
	}
	return read("profile.json"), p, inventory
}

func authenticationHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// No credential or arbitrary fixture value is ever included in a diagnostic.
func authenticationInvalid(field string) error {
	return fmt.Errorf("authentication fixture: %s", field)
}

func validateAuthenticationFixture(data []byte, p authenticationProvenance, inventory authenticationInventory) error {
	parsed, err := authenticationJSON(data)
	if err != nil {
		return authenticationInvalid("JSON")
	}
	document, ok := parsed.(map[string]any)
	if !ok {
		return authenticationInvalid("document")
	}
	cases, ok := document["cases"].([]any)
	if !ok || len(cases) != inventory.Count {
		return authenticationInvalid("case count")
	}
	delete(document, "cases")
	if !reflect.DeepEqual(document, p.Metadata) {
		return authenticationInvalid("provenance")
	}
	required := map[authenticationIdentity]bool{}
	for _, group := range inventory.Groups {
		for _, policy := range group.NamingPolicies {
			for _, id := range group.Cases {
				key := authenticationIdentity{group.Surface, policy, group.Operation, id}
				if required[key] {
					return authenticationInvalid("duplicate inventory")
				}
				required[key] = true
			}
		}
	}
	if len(required) != 55 {
		return authenticationInvalid("inventory count")
	}
	for index, value := range cases {
		item, ok := value.(map[string]any)
		if !ok {
			return authenticationInvalid("case object")
		}
		text := func(key string) string { s, _ := item[key].(string); return s }
		key := authenticationIdentity{text("surface"), text("namingPolicy"), text("operation"), text("id")}
		if !required[key] {
			return authenticationInvalid(fmt.Sprintf("case[%d] identity", index))
		}
		delete(required, key)
		want, err := expectedAuthenticationCase(key)
		if err != nil {
			return err
		}
		if key.surface == "outgoing" && key.id != "oauth-unavailable" {
			result, ok := item["result"].(map[string]any)
			if !ok {
				return authenticationInvalid("request result")
			}
			encoded, ok := result["bytesBase64"].(string)
			if !ok || validateAuthenticationWire(encoded, key.id) != nil {
				return authenticationInvalid(fmt.Sprintf("case[%d] request bytes", index))
			}
			result["bytesBase64"] = "validated-request-bytes"
		}
		if err := normalizeAuthenticationJSON(item); err != nil {
			return authenticationInvalid(fmt.Sprintf("case[%d] nested JSON", index))
		}
		if !reflect.DeepEqual(item, want) {
			return authenticationInvalid(fmt.Sprintf("case[%d] fields", index))
		}
	}
	return nil
}

// Fixture reader only, not a Go SourceAuthorization decoder. Reject duplicate
// properties before ordinary JSON decoding could silently keep the last value.
func authenticationJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var read func() (any, error)
	read = func() (any, error) {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return token, nil
		}
		switch delimiter {
		case '{':
			value := map[string]any{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("property")
				}
				if _, exists := value[key]; exists {
					return nil, errors.New("duplicate property")
				}
				value[key], err = read()
				if err != nil {
					return nil, err
				}
			}
			_, err := decoder.Token()
			return value, err
		case '[':
			value := []any{}
			for decoder.More() {
				item, err := read()
				if err != nil {
					return nil, err
				}
				value = append(value, item)
			}
			_, err := decoder.Token()
			return value, err
		default:
			return nil, errors.New("delimiter")
		}
	}
	value, err := read()
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing input")
	}
	return value, nil
}

func normalizeAuthenticationJSON(value map[string]any) error {
	for key, item := range value {
		if key == "rawJSON" || key == "decodedJSON" {
			text, ok := item.(string)
			if !ok {
				return authenticationInvalid("raw JSON type")
			}
			parsed, err := authenticationJSON([]byte(text))
			if err != nil {
				return authenticationInvalid("raw JSON")
			}
			value[key] = parsed
		} else if child, ok := item.(map[string]any); ok {
			if err := normalizeAuthenticationJSON(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func expectedAuthenticationCase(key authenticationIdentity) (map[string]any, error) {
	want := map[string]any{"surface": key.surface, "namingPolicy": key.policy, "operation": key.operation, "id": key.id}
	if key.surface == "outgoing" {
		if key.id == "oauth-unavailable" {
			want["evidence"], want["status"], want["reason"] = "public-api-unavailable", "unavailable", "no-public-OAuth-Register-builder"
		} else {
			request := authenticationRequestJSON(key.id)
			want["evidence"] = "public-Register-captured"
			want["result"] = map[string]any{"method": contracts.Webhooks_AddWebhooks_FullMethodName, "requestType": "Cratis.Chronicle.Contracts.Observation.Webhooks.AddWebhooksRequest", "rawJSON": request, "decodedJSON": request, "bytesBase64": "validated-request-bytes"}
		}
		return want, nil
	}
	want["evidence"] = "public-actual-serialization"
	property := func(name string) string {
		if key.policy == "CamelCaseNamingPolicy" {
			return strings.ToLower(name[:1]) + name[1:]
		}
		return name
	}
	arm := key.id
	switch arm {
	case "default-null", "null-authorization":
		arm = "null"
	case "explicit-none", "missing-discriminator", "unknown-discriminator", "case-changed-value", "case-changed-key":
		arm = "none"
	case "basic-then-bearer":
		arm = "bearer"
	case "bearer-then-basic":
		arm = "basic"
	case "basic-then-oauth":
		arm = "oauth"
	}
	var raw, secondary any
	outputType := "Cratis.Chronicle.Concepts.Captures.SourceDefinition"
	switch key.operation {
	case "EventSerializer.Serialize":
		body := map[string]any{property("Type"): float64(1), property("Path"): "/synthetic-capture"}
		if arm != "null" {
			body[property("Authorization")] = authenticationAuthorization(arm)
		}
		raw, secondary = body, body
		want["authorizationArm"] = arm
	case "JsonSerializer.SourceAuthorization":
		raw = authenticationAuthorization(arm)
		secondary = raw
		want["authorizationArm"] = arm
		outputType = "Cratis.Chronicle.Concepts.Captures.SourceAuthorization"
	case "EventSerializer.Deserialize":
		auth, err := authenticationControl(key.id)
		if err != nil {
			return nil, err
		}
		raw = map[string]any{property("Type"): "Webhook", property("Authorization"): auth}
		body := map[string]any{property("Type"): float64(1)}
		if arm != "null" {
			body[property("Authorization")] = authenticationAuthorization(arm)
		}
		secondary = body
	default:
		return nil, authenticationInvalid("operation")
	}
	want["rawJSON"] = raw
	if strings.Contains(key.id, "-missing-") {
		want["read"] = map[string]any{"status": "error", "category": "System.Collections.Generic.KeyNotFoundException", "reserialize": map[string]any{"status": "not-attempted", "reason": "deserialize-failed"}}
	} else {
		want["read"] = map[string]any{"status": "accepted", "outputType": outputType, "authorizationArm": arm, "reserialize": map[string]any{"status": "accepted", "rawJSON": secondary}}
	}
	return want, nil
}

func authenticationAuthorization(arm string) any {
	switch arm {
	case "basic":
		return map[string]any{"type": "basic", "username": "synthetic-user", "password": "synthetic-password"}
	case "bearer":
		return map[string]any{"type": "bearer", "token": "synthetic-token"}
	case "oauth":
		return map[string]any{"type": "oauth", "authority": "https://synthetic.invalid/oauth", "clientId": "synthetic-client", "clientSecret": "synthetic-secret"}
	case "none":
		return map[string]any{"type": "none"}
	default:
		return nil
	}
}

func authenticationControl(id string) (any, error) {
	switch id {
	case "missing-discriminator":
		return map[string]any{}, nil
	case "unknown-discriminator":
		return map[string]any{"type": "unrecognized"}, nil
	case "case-changed-value":
		return map[string]any{"type": "Basic", "username": "synthetic-user", "password": "synthetic-password"}, nil
	case "case-changed-key":
		return map[string]any{"Type": "basic", "username": "synthetic-user", "password": "synthetic-password"}, nil
	case "null-authorization":
		return nil, nil
	case "explicit-none":
		return authenticationAuthorization("none"), nil
	}
	parts := strings.SplitN(id, "-missing-", 2)
	if len(parts) != 2 {
		return nil, authenticationInvalid("control")
	}
	auth, ok := authenticationAuthorization(parts[0]).(map[string]any)
	if !ok {
		return nil, authenticationInvalid("control arm")
	}
	field := map[string]string{"client-id": "clientId", "client-secret": "clientSecret"}[parts[1]]
	if field == "" {
		field = parts[1]
	}
	delete(auth, field)
	return auth, nil
}

func authenticationRequestJSON(id string) map[string]any {
	event := func(name string, generation float64) any {
		return map[string]any{"Id": name, "Generation": generation, "Tombstone": false}
	}
	events := []any{event("capture-first", 2), event("capture-second", 3)}
	headers := map[string]any{}
	if strings.Contains(id, "selected") {
		events = events[1:]
		headers["X-Synthetic"] = "fixed-header"
	}
	var authorization any
	if id == "basic-default" {
		value := map[string]any{"Username": "synthetic-user", "Password": "synthetic-password"}
		authorization = map[string]any{"Value0": value, "Value1": nil, "Value2": nil, "Value": value}
	} else if strings.Contains(id, "bearer") {
		value := map[string]any{"Token": "synthetic-token"}
		authorization = map[string]any{"Value0": nil, "Value1": value, "Value2": nil, "Value": value}
	}
	active := !strings.HasSuffix(id, "false")
	return map[string]any{"EventStore": "synthetic-store", "Webhooks": []any{map[string]any{
		"EventSequenceId": "event-log", "Identifier": "synthetic-webhook", "EventTypes": events,
		"Target":       map[string]any{"Url": "https://synthetic.invalid/receive", "Authorization": authorization, "Headers": headers},
		"IsReplayable": active, "IsActive": active,
	}}}
}

func validateAuthenticationWire(encoded, id string) error {
	data, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(data) == 0 {
		return authenticationInvalid("wire base64")
	}
	var request contracts.AddWebhooksRequest
	if err := proto.Unmarshal(data, &request); err != nil || len(request.GetWebhooks()) != 1 {
		return authenticationInvalid("wire request")
	}
	// Reconstruct the expected request from the independently authored JSON field
	// expectations with the CLR defaults explicitly represented in wire validation.
	expected := &contracts.AddWebhooksRequest{EventStore: "synthetic-store", Webhooks: []*contracts.WebhookDefinition{expectedAuthenticationWebhook(id)}}
	// C# omits true (DefaultValue(true)); proto3 sees false. Presence is checked below.
	expected.Webhooks[0].IsActive, expected.Webhooks[0].IsReplayable = false, false
	if !proto.Equal(&request, expected) {
		return authenticationInvalid("wire semantics")
	}
	fields, err := authenticationWireFields(data)
	if err != nil || len(fields[2]) != 1 {
		return authenticationInvalid("wire outer fields")
	}
	nested, err := authenticationWireFields(fields[2][0].payload)
	if err != nil {
		return authenticationInvalid("wire definition fields")
	}
	for _, number := range []protowire.Number{5, 6} {
		occurrences := nested[number]
		if strings.HasSuffix(id, "false") {
			if len(occurrences) != 1 || occurrences[0].kind != protowire.VarintType || occurrences[0].number != 0 {
				return authenticationInvalid("wire explicit false")
			}
		} else if len(occurrences) != 0 {
			return authenticationInvalid("wire default true omission")
		}
	}
	if strings.HasSuffix(id, "false") {
		expected.Webhooks[0].ProtoReflect().SetUnknown([]byte{0x28, 0, 0x30, 0})
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(expected)
	if err != nil || !bytes.Equal(data, canonical) {
		return authenticationInvalid("wire noncanonical or duplicate fields")
	}
	return nil
}

func expectedAuthenticationWebhook(id string) *contracts.WebhookDefinition {
	value := &contracts.WebhookDefinition{
		EventSequenceId: "event-log", Identifier: "synthetic-webhook",
		EventTypes: []*contracts.EventType{{Id: "capture-first", Generation: 2}, {Id: "capture-second", Generation: 3}},
		Target:     &contracts.WebhookTarget{Url: "https://synthetic.invalid/receive", Headers: map[string]string{}},
		IsActive:   !strings.HasSuffix(id, "false"), IsReplayable: !strings.HasSuffix(id, "false"),
	}
	if strings.Contains(id, "selected") {
		value.EventTypes = value.EventTypes[1:]
		value.Target.Headers["X-Synthetic"] = "fixed-header"
	}
	if id == "basic-default" {
		value.Target.Authorization = &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value0: &contracts.BasicAuthorization{Username: "synthetic-user", Password: "synthetic-password"}}
	} else if strings.Contains(id, "bearer") {
		value.Target.Authorization = &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value1: &contracts.BearerTokenAuthorization{Token: "synthetic-token"}}
	}
	return value
}

type authenticationWireField struct {
	kind    protowire.Type
	number  uint64
	payload []byte
}

func authenticationWireFields(data []byte) (map[protowire.Number][]authenticationWireField, error) {
	result := map[protowire.Number][]authenticationWireField{}
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 || number <= 0 {
			return nil, authenticationInvalid("wire tag")
		}
		data = data[n:]
		field := authenticationWireField{kind: kind}
		switch kind {
		case protowire.BytesType:
			field.payload, n = protowire.ConsumeBytes(data)
		case protowire.VarintType:
			field.number, n = protowire.ConsumeVarint(data)
		default:
			return nil, authenticationInvalid("wire field type")
		}
		if n < 0 {
			return nil, authenticationInvalid("wire field")
		}
		result[number] = append(result[number], field)
		data = data[n:]
	}
	return result, nil
}
