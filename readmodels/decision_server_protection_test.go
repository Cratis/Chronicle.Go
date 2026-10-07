// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"errors"
	"strings"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"google.golang.org/protobuf/proto"
)

func TestDecisionServerGenerationAndUnreadableSchemaRefuseBeforeAndAfterFold(t *testing.T) {
	for _, tc := range []struct {
		name, schema string
		generation   uint32
	}{
		{"invalid JSON", `{"properties":PRIVATE`, 1},
		{"generation changed", "", 2},
		{"generation missing", "", 0},
	} {
		for _, afterFold := range []bool{false, true} {
			phase := map[bool]string{false: "before", true: "after"}[afterFold]
			t.Run(tc.name+"/"+phase, func(t *testing.T) {
				f := newDecisionFixture(t)
				folds, cleanups, agreements := 0, 0, 0
				f.handle = func(_ context.Context, request any) (proto.Message, error) {
					switch request.(type) {
					case *contracts.GetDefinitionsRequest:
						agreements++
						response := f.respond(request).(*contracts.GetDefinitionsResponse)
						if !afterFold || agreements == 2 {
							if afterFold && cleanups != 1 {
								t.Error("post-fold agreement ran before cleanup completed")
							}
							response.ReadModels[0].Type.Generation = tc.generation
							if tc.schema != "" {
								response.ReadModels[0].Schema = tc.schema
							}
						}
						return response, nil
					case *contracts.GetInstanceByKeyRequest:
						folds++
					case *contracts.DehydrateSessionRequest:
						cleanups++
					}
					return nil, nil
				}
				read, err := f.reader.GetDetached(t.Context(), "source")
				var refused *DecisionReadRefused
				if !errors.As(err, &refused) || refused.Reason != DecisionDefinitionMismatch || !read.Token.IsZero() || read.Instance.Exists || read.Instance.Value.Name != "" || read.Instance.LastHandled != nil || strings.Contains(err.Error(), "PRIVATE") {
					t.Fatal("unsafe server agreement issued evidence", err)
				}
				wantFolds := map[bool]int{false: 0, true: 1}[afterFold]
				if folds != wantFolds || cleanups != wantFolds || f.acquired != 1 || f.released != 1 || agreements != wantFolds+1 {
					t.Fatalf("folds=%d cleanups=%d leases=%d/%d agreements=%d", folds, cleanups, f.acquired, f.released, agreements)
				}
			})
		}
	}
}

// Chronicle#4561 is fixed in 19.32.2: the kernel releases session folds with
// each value's original subject, so server-side classification, like C#, is not
// an agreement criterion. Malformed schemas that still yield the key are admitted.
func TestDecisionServerProtectionIsNotAnAgreementCriterion(t *testing.T) {
	for _, tc := range []struct{ name, schema string }{
		{"PII", `{"properties":{"id":{"type":"string"},"secret":{"type":"string","compliance":[{"metadataType":"PII"}]}}}`},
		{"subject encryption", `{"properties":{"id":{"type":"string"},"secret":{"security":[{"metadataType":"EncryptedSubject"}]}}}`},
		{"namespace false subject flag", `{"properties":{"id":{"type":"string"},"secret":{"security":[{"metadataType":"EncryptedNamespace"}]}}}`},
		{"global false subject flag", `{"properties":{"id":{"type":"string"},"secret":{"security":[{"metadataType":"EncryptedGlobal"}]}}}`},
		{"unknown protection", `{"properties":{"id":{"type":"string"},"secret":{"security":[{"metadataType":"FutureProtection"}]}}}`},
		{"reference under collection", `{"properties":{"id":{"type":"string"},"items":{"items":{"$ref":"#/definitions/private~1node"}}},"definitions":{"private/node":{"properties":{"secret":{"compliance":[{"metadataType":"PII"}]}},"next":{"$ref":"#/definitions/private~1node"}}}}`},
		{"recursive reference", `{"properties":{"id":{"type":"string"},"nested":{"$ref":"#/definitions/node"}},"definitions":{"node":{"properties":{"next":{"$ref":"#/definitions/node"},"secret":{"security":[{"metadataType":"EncryptedNamespace"}]}}}}`},
		{"unresolved reference", `{"properties":{"id":{"type":"string"},"secret":{"$ref":"#/definitions/PRIVATE"}}}`},
		{"external reference", `{"properties":{"id":{"type":"string"},"secret":{"$ref":"https://PRIVATE"}}}`},
		{"malformed metadata", `{"properties":{"id":{"type":"string"},"secret":{"security":"PRIVATE"}}}`},
		{"unreadable metadata", `{"properties":{"id":{"type":"string"},"secret":{"compliance":[{}]}}}`},
		{"malformed property", `{"properties":{"id":{"type":"string"},"secret":"PRIVATE"}}`},
		{"duplicate metadata hides classification", `{"properties":{"id":{"type":"string"},"secret":{"security":[{"metadataType":"EncryptedSubject"}],"security":[]}}}`},
		{"dependency namespace", `{"properties":{"id":{"type":"string"},"name":{"type":"string"}},"dependencies":{"id":{"properties":{"name":{"security":[{"metadataType":"EncryptedNamespace"}]}}}}}`},
		{"dependency global", `{"properties":{"id":{"type":"string"}},"dependencies":{"id":{"properties":{"name":{"security":[{"metadataType":"EncryptedGlobal"}]}}}}}`},
		{"dependency PII", `{"properties":{"id":{"type":"string"}},"dependencies":{"id":{"properties":{"name":{"compliance":[{"metadataType":"PII"}]}}}}}`},
		{"dependency unknown", `{"properties":{"id":{"type":"string"}},"dependencies":{"id":{"properties":{"name":{"security":[{"metadataType":"FutureProtection"}]}}}}}`},
		{"dependency reference", `{"properties":{"id":{"type":"string"}},"dependencies":{"id":{"$ref":"#/definitions/private"}},"definitions":{"private":{"properties":{"name":{"security":[{"metadataType":"EncryptedNamespace"}]}}}}}`},
		{"nested dependency reference", `{"properties":{"id":{"type":"string"},"nested":{"dependencies":{"trigger":{"$ref":"#/definitions/private"}}}},"definitions":{"private":{"security":[{"metadataType":"EncryptedGlobal"}]}}}`},
		{"dependency malformed metadata", `{"properties":{"id":{"type":"string"}},"dependencies":{"id":{"security":"PRIVATE"}}}`},
		{"dependency duplicate hides metadata", `{"properties":{"id":{"type":"string"}},"dependencies":{"id":{"security":[{"metadataType":"EncryptedNamespace"}]},"id":{}}}`},
		{"unknown container metadata", `{"properties":{"id":{"type":"string"}},"extension":{"properties":{"name":{"security":"PRIVATE"}}}}`},
		{"property names metadata", `{"properties":{"id":{"type":"string"}},"propertyNames":{"security":[{"metadataType":"EncryptedGlobal"}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDecisionFixture(t)
			f.handle = func(_ context.Context, request any) (proto.Message, error) {
				if _, ok := request.(*contracts.GetDefinitionsRequest); ok {
					response := f.respond(request).(*contracts.GetDefinitionsResponse)
					response.ReadModels[0].Schema = tc.schema
					return response, nil
				}
				return nil, nil
			}
			read, err := f.reader.GetDetached(t.Context(), "source")
			var refused *DecisionReadRefused
			if errors.As(err, &refused) && refused.Reason == DecisionDefinitionMismatch {
				// A schema that no longer declares the key is still refused.
				if _, ok := decisionKeySchema(tc.schema); ok {
					t.Fatal("server protection refused agreement", err)
				}
				return
			}
			if err != nil || read.Token.IsZero() || !read.Instance.Exists {
				t.Fatal("classified server schema not admitted", err)
			}
			assertDecisionCleanup(t, f)
		})
	}
}

func TestDecisionAgreementAllowsPlainNonKeySchemaDifferencesAtSelectedGeneration(t *testing.T) {
	f := newDecisionFixture(t, WithGeneration(2))
	f.handle = func(_ context.Context, request any) (proto.Message, error) {
		if _, ok := request.(*contracts.GetDefinitionsRequest); ok {
			response := f.respond(request).(*contracts.GetDefinitionsResponse)
			response.ReadModels[0].Type.Generation = 2
			response.ReadModels[0].Schema = `{"properties":{"id":{"type":"string"},"newServerProperty":{"$ref":"#/definitions/plain~1node"}},"definitions":{"plain/node":{"type":"string"}},"dependencies":{"id":["newServerProperty"],"newServerProperty":{"properties":{"name":{"type":"number","default":{"security":[{"metadataType":"PII"}]}},"annotation":{"enum":[{"security":null}]}}}}}`
			return response, nil
		}
		return nil, nil
	}
	read, err := f.reader.GetDetached(t.Context(), "source")
	if err != nil || read.Token.IsZero() || !read.Instance.Exists {
		t.Fatal("ordinary schema differences rejected", err)
	}
	assertDecisionCleanup(t, f)
}
