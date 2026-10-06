// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"google.golang.org/protobuf/proto"
)

// Chronicle#4561 is fixed in 19.32.2: the kernel releases decision session
// folds with each value's original subject. Classified models are admitted like
// C#, and the SDK never sends the released plaintext through Release again.
func TestClassifiedDecisionsAreAdmittedWithoutSecondRelease(t *testing.T) {
	// String validation cannot distinguish this from ciphertext.
	const plaintext = "Q0VOVgEAAAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxw="
	for _, classification := range []struct {
		name  string
		value compliance.Classification
	}{
		{"pii", compliance.Classification{PII: true}},
		{"subject", compliance.Classification{Encrypted: true}},
		{"namespace", compliance.Classification{Encrypted: true, Scope: compliance.Namespace}},
		{"global", compliance.Classification{Encrypted: true, Scope: compliance.Global}},
	} {
		for _, declaration := range []string{"property", "type", "provider"} {
			t.Run(classification.name+"/"+declaration, func(t *testing.T) {
				option := compliance.Property("name", classification.value)
				switch declaration {
				case "type":
					option = compliance.For[decisionPerson](classification.value)
				case "provider":
					option = compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
						if target.Field == "Name" {
							return classification.value, nil
						}
						return compliance.Classification{}, nil
					})
				}
				f := newDecisionFixture(t, WithProtection(option))
				if a := f.reader.Admit(); !a.IsAdmitted {
					t.Fatalf("classified model refused: %+v", a)
				}
				// Any Release request fails the fixture as an unexpected RPC.
				f.handle = func(_ context.Context, request any) (proto.Message, error) {
					if _, ok := request.(*contracts.GetInstanceByKeyRequest); ok {
						return &contracts.GetInstanceByKeyResponse{ReadModel: `{"id":"source","name":"` + plaintext + `"}`, LastHandledEventSequenceNumber: 5}, nil
					}
					return nil, nil
				}
				read, err := f.reader.GetDetached(t.Context(), "source-not-subject")
				if err != nil || !read.Instance.Exists || read.Instance.Value.Name != plaintext || read.Token.IsZero() {
					t.Fatalf("classified decision read: %+v %v", read, err)
				}
				assertDecisionCleanup(t, f)
			})
		}
	}
}
