// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestAuthenticationFixtureRejectsMissingAndDuplicateCases(t *testing.T) {
	data, provenance, inventory := authenticationFiles(t)
	for index := range inventory.Count {
		for _, duplicate := range []bool{false, true} {
			t.Run(fmt.Sprintf("case-%d/duplicate-%t", index, duplicate), func(t *testing.T) {
				doc := authenticationDocument(t, data)
				cases := doc["cases"].([]any)
				if duplicate {
					cases[index] = cases[(index+1)%len(cases)]
				} else {
					cases = append(cases[:index], cases[index+1:]...)
				}
				doc["cases"] = cases
				authenticationMustReject(t, doc, provenance, inventory)
			})
		}
	}
}

func TestAuthenticationFixtureRejectsFieldMutations(t *testing.T) {
	data, provenance, inventory := authenticationFiles(t)
	// Exercise each shape/arm and both naming policies, not permutations of every
	// synthetic credential. Whole-case admission is separately checked above.
	for _, index := range []int{0, 1, 2, 3, 4, 8, 11, 12, 16, 22, 24, 26, 28, 36, 40, 48, 49, 50, 51, 52, 53, 54} {
		original := authenticationDocument(t, data)["cases"].([]any)[index].(map[string]any)
		for _, path := range authenticationFieldPaths(original, nil) {
			for _, remove := range []bool{true, false} {
				t.Run(fmt.Sprintf("case-%d/%s/remove-%t", index, strings.Join(path, "/"), remove), func(t *testing.T) {
					doc := authenticationDocument(t, data)
					item := doc["cases"].([]any)[index].(map[string]any)
					parent := item
					for _, key := range path[:len(path)-1] {
						parent = parent[key].(map[string]any)
					}
					key := path[len(path)-1]
					if remove {
						delete(parent, key)
					} else {
						parent[key] = "PRIVATE-MUST-NOT-LEAK"
					}
					authenticationMustReject(t, doc, provenance, inventory)
				})
			}
		}
	}
}

func TestAuthenticationFixtureRejectsProvenanceAndClaims(t *testing.T) {
	data, provenance, inventory := authenticationFiles(t)
	for key := range provenance.Metadata {
		t.Run("missing-"+key, func(t *testing.T) {
			doc := authenticationDocument(t, data)
			delete(doc, key)
			authenticationMustReject(t, doc, provenance, inventory)
		})
	}
	mutations := map[string]func(map[string]any){
		"forbidden-call":      func(d map[string]any) { d["forbiddenCalls"] = map[string]any{"Connect": 1} },
		"dangling-zero-call":  func(d map[string]any) { d["forbiddenCalls"] = map[string]any{"Connect": 0} },
		"wrong-allowed-count": func(d map[string]any) { d["allowedCalls"].(map[string]any)["rpc.AddWebhooks"] = float64(5) },
		"lost-package-hash":   func(d map[string]any) { delete(d["packages"].([]any)[0].(map[string]any), "sha256") },
		"wrong-assembly-hash": func(d map[string]any) {
			d["assemblies"].([]any)[0].(map[string]any)["sha256"] = "PRIVATE-MUST-NOT-LEAK"
		},
		"wrong-architecture": func(d map[string]any) { d["runtime"].(map[string]any)["architecture"] = "X64" },
		"changed-serializer": func(d map[string]any) {
			d["configurations"].([]any)[0].(map[string]any)["PropertyNameCaseInsensitive"] = true
		},
		"encryption-claim":       func(d map[string]any) { d["limits"] = []any{"encryption-proven"} },
		"live-claim":             func(d map[string]any) { d["cases"].([]any)[48].(map[string]any)["evidence"] = "live" },
		"oauth-register-success": func(d map[string]any) { d["cases"].([]any)[54].(map[string]any)["status"] = "accepted" },
		"unexpected-claim":       func(d map[string]any) { d["kernelAuthenticationProven"] = true },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			doc := authenticationDocument(t, data)
			mutate(doc)
			authenticationMustReject(t, doc, provenance, inventory)
		})
	}
}

func TestAuthenticationFixtureRejectsNestedJSONAndWireMutations(t *testing.T) {
	data, provenance, inventory := authenticationFiles(t)
	for _, index := range []int{1, 2, 3, 4, 8, 11, 12, 26, 28, 48, 49, 50, 52, 53} {
		for _, secondary := range []bool{false, true} {
			original := authenticationDocument(t, data)
			item := original["cases"].([]any)[index].(map[string]any)
			parent, key := authenticationRawField(item, secondary)
			parsed := authenticationDocument(t, []byte(parent[key].(string)))
			for _, path := range authenticationFieldPaths(parsed, nil) {
				t.Run(fmt.Sprintf("case-%d/secondary-%t/%s", index, secondary, strings.Join(path, "/")), func(t *testing.T) {
					doc := authenticationDocument(t, data)
					row := doc["cases"].([]any)[index].(map[string]any)
					owner, field := authenticationRawField(row, secondary)
					nested := authenticationDocument(t, []byte(owner[field].(string)))
					object := nested
					for _, name := range path[:len(path)-1] {
						object = object[name].(map[string]any)
					}
					delete(object, path[len(path)-1])
					owner[field] = string(authenticationMarshal(t, nested))
					authenticationMustReject(t, doc, provenance, inventory)
				})
			}
			t.Run(fmt.Sprintf("case-%d/secondary-%t/duplicate", index, secondary), func(t *testing.T) {
				doc := authenticationDocument(t, data)
				owner, field := authenticationRawField(doc["cases"].([]any)[index].(map[string]any), secondary)
				owner[field] = `{"type":"none","type":"none"}`
				authenticationMustReject(t, doc, provenance, inventory)
			})
		}
	}
	for index := 48; index < 54; index++ {
		for _, mutation := range []string{"truncated", "duplicate", "wrong-payload", "wrong-wire-type", "lost-false"} {
			t.Run(fmt.Sprintf("wire-%d/%s", index, mutation), func(t *testing.T) {
				doc := authenticationDocument(t, data)
				result := doc["cases"].([]any)[index].(map[string]any)["result"].(map[string]any)
				wire, err := base64.StdEncoding.DecodeString(result["bytesBase64"].(string))
				if err != nil {
					t.Fatal("wire setup")
				}
				switch mutation {
				case "truncated":
					wire = wire[:len(wire)-1]
				case "duplicate":
					wire = append(wire, wire...)
				case "wrong-payload":
					wire[3] ^= 1
				case "wrong-wire-type":
					wire[0] = 8
				case "lost-false":
					wire = wire[:len(wire)-4]
				}
				result["bytesBase64"] = base64.StdEncoding.EncodeToString(wire)
				authenticationMustReject(t, doc, provenance, inventory)
			})
		}
	}
}

func TestAuthenticationFixtureRejectsDiscriminatorAndUnionConfusion(t *testing.T) {
	data, provenance, inventory := authenticationFiles(t)
	for _, mutation := range []struct {
		name  string
		index int
		old   string
		new   string
	}{
		{"basic-discriminator", 2, `"type":"basic"`, `"type":"Basic"`},
		{"outer-casing", 2, `"Authorization"`, `"authorization"`},
		{"inner-casing", 4, `"clientId"`, `"ClientId"`},
		{"discriminator-type", 3, `"type":"bearer"`, `"type":2`},
		{"typed-source-enum", 2, `"Type":1`, `"Type":"1"`},
		{"credential-type", 3, `"token":"synthetic-token"`, `"token":{}`},
		{"wrong-union-arm", 50, `"Value1":{"Token":"synthetic-token"}`, `"Value1":null`},
		{"wrong-union-value", 49, `"Value":{"Username":"synthetic-user","Password":"synthetic-password"}`, `"Value":null`},
		{"event-generation", 50, `"Generation":3`, `"Generation":1`},
		{"headers-lost", 50, `"Headers":{"X-Synthetic":"fixed-header"}`, `"Headers":{}`},
	} {
		for _, secondary := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/secondary-%t", mutation.name, secondary), func(t *testing.T) {
				doc := authenticationDocument(t, data)
				owner, field := authenticationRawField(doc["cases"].([]any)[mutation.index].(map[string]any), secondary)
				raw := owner[field].(string)
				changed := strings.Replace(raw, mutation.old, mutation.new, 1)
				if raw == changed {
					t.Fatal("mutation did not change a field")
				}
				owner[field] = changed
				authenticationMustReject(t, doc, provenance, inventory)
			})
		}
	}
}

func authenticationRawField(item map[string]any, secondary bool) (map[string]any, string) {
	if item["surface"] == "outgoing" {
		key := "rawJSON"
		if secondary {
			key = "decodedJSON"
		}
		return item["result"].(map[string]any), key
	}
	if secondary {
		return item["read"].(map[string]any)["reserialize"].(map[string]any), "rawJSON"
	}
	return item, "rawJSON"
}

func authenticationFieldPaths(value map[string]any, prefix []string) [][]string {
	var paths [][]string
	for key, item := range value {
		path := append(append([]string(nil), prefix...), key)
		paths = append(paths, path)
		if child, ok := item.(map[string]any); ok {
			paths = append(paths, authenticationFieldPaths(child, path)...)
		}
	}
	return paths
}

func authenticationDocument(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal("fixture setup JSON")
	}
	return value
}

func authenticationMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal("fixture mutation encoding")
	}
	return data
}

func authenticationMustReject(t *testing.T, value any, provenance authenticationProvenance, inventory authenticationInventory) {
	t.Helper()
	err := validateAuthenticationFixture(authenticationMarshal(t, value), provenance, inventory)
	if err == nil {
		t.Fatal("altered evidence accepted")
	}
	if strings.Contains(err.Error(), "PRIVATE-MUST-NOT-LEAK") || strings.Contains(err.Error(), "synthetic-") {
		t.Fatal("diagnostic exposed fixture payload")
	}
}
