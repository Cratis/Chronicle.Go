// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"google.golang.org/protobuf/proto"
)

type decisionProtectedNode struct {
	Value string                 `json:"value"`
	Next  *decisionProtectedNode `json:"next"`
}
type decisionProtectedModel struct {
	ID     string                 `json:"id"`
	Name   string                 `json:"name"`
	Nested *decisionProtectedNode `json:"nested"`
}

func TestClassifiedDecisionsRefuseBeforeLeaseOrRPC(t *testing.T) {
	for _, classification := range []struct {
		name  string
		value compliance.Classification
	}{
		{"pii", compliance.Classification{PII: true}},
		{"subject", compliance.Classification{Encrypted: true}},
		{"namespace", compliance.Classification{Encrypted: true, Scope: compliance.Namespace}},
		{"global", compliance.Classification{Encrypted: true, Scope: compliance.Global}},
	} {
		for _, declaration := range []string{"property", "nested-reference", "type", "provider"} {
			t.Run(classification.name+"/"+declaration, func(t *testing.T) {
				providerCalls := 0
				option := compliance.Property("name", classification.value)
				switch declaration {
				case "nested-reference":
					option = compliance.Property("nested.value", classification.value)
				case "type":
					option = compliance.For[decisionProtectedNode](classification.value)
				case "provider":
					option = compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
						providerCalls++
						if target.Field == "Value" {
							return classification.value, nil
						}
						return compliance.Classification{}, nil
					})
				}
				m, err := Define[decisionProtectedModel](WithIdentifier("person"), WithObserver(Projection, "people"), WithProtection(option))
				if err != nil {
					t.Fatal(err)
				}
				frozenCalls := providerCalls
				models, err := NewCatalog(m.Descriptor())
				if err != nil {
					t.Fatal(err)
				}
				f := newDecisionFixture(t)
				service, err := New("store", "tenant", models, f)
				if err != nil {
					t.Fatal(err)
				}
				reader := DecisionsFor(service, m)
				if a := reader.Admit(); a.IsAdmitted || a.Reason != DecisionProtectedModel {
					t.Fatalf("admitted classified model: %+v", a)
				}
				for _, document := range []string{`{}`, `{"id":"source","name":"","nested":null}`, `{"id":"source","name":"PRIVATE cipher-shaped plaintext"}`} {
					f.handle = func(context.Context, any) (proto.Message, error) {
						return &contracts.GetInstanceByKeyResponse{ReadModel: document, LastHandledEventSequenceNumber: uint64(events.Unavailable)}, nil
					}
					read, err := reader.GetDetached(t.Context(), "source-not-subject")
					var refused *DecisionReadRefused
					if !errors.As(err, &refused) || refused.Reason != DecisionProtectedModel || read.Instance.Exists || !read.Token.IsZero() {
						t.Fatal("classified decision issued evidence", err)
					}
				}
				if f.acquired != 0 || f.released != 0 || len(f.requests) != 0 || providerCalls != frozenCalls {
					t.Fatal("classification admission acquired a lease, called RPC, or reran a provider")
				}
			})
		}
	}
}

func TestDecisionProtectionMetadataErrorsRefuseBeforeLease(t *testing.T) {
	f := newDecisionFixture(t)
	// Public declarations cannot publish malformed schema; exercise the failure
	// branch defensively without treating metadata extraction failure as plain.
	f.model.descriptor.definition.schema = `{"properties":`
	if a := f.reader.Admit(); a.IsAdmitted || a.Reason != DecisionProtectionMetadata {
		t.Fatalf("bad metadata admitted: %+v", a)
	}
	read, err := f.reader.GetDetached(t.Context(), "source")
	if !errors.Is(err, ErrDecisionReadRefused) || read.Instance.Exists || !read.Token.IsZero() || f.acquired != 0 || len(f.requests) != 0 {
		t.Fatal("metadata failure issued work/evidence", err)
	}
}
