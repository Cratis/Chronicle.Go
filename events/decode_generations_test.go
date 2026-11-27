// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"encoding/json"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
)

type DecodeGeneration struct{ Value string }
type PreviousDecodeGeneration DecodeGeneration

// The kernel releases protected values in every generation of GenerationalContent
// (Cratis/Chronicle#4623), so protection never changes which content is selected.
func TestDecodeAlternateGenerationSelectsKernelContentForProtectedEvents(t *testing.T) {
	for _, tc := range []struct {
		name           string
		classification compliance.Classification
	}{
		{"plain", compliance.Classification{}},
		{"pii", compliance.Classification{PII: true}},
		{"encrypted", compliance.Classification{Encrypted: true, Scope: compliance.Namespace}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			option := events.WithProtection(compliance.Property("Value", tc.classification))
			current, err := events.Define[DecodeGeneration](events.WithGeneration(2), option)
			if err != nil {
				t.Fatal(err)
			}
			previous, err := events.DefineGeneration[PreviousDecodeGeneration](current, 1, option)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := events.NewCatalog(current.Descriptor(), previous.Descriptor())
			if err != nil {
				t.Fatal(err)
			}
			appended := events.Appended{
				Context:             events.Context{EventType: current.Ref()},
				Content:             json.RawMessage(`{"Value":"released"}`),
				GenerationalContent: map[events.Generation]json.RawMessage{1: json.RawMessage(`{"Value":"ciphertext-marker"}`)},
			}
			value, err := events.Decode[PreviousDecodeGeneration](catalog, appended)
			if err != nil || value.Value != "ciphertext-marker" {
				t.Fatalf("alternate = %#v, %v", value, err)
			}
			assertDeliveredGeneration(t, catalog, appended)
			// The existing raw-content fallback does not select alternate bytes.
			appended.GenerationalContent = nil
			fallback, err := events.Decode[PreviousDecodeGeneration](catalog, appended)
			if err != nil || fallback.Value != "released" {
				t.Fatalf("fallback = %#v, %v", fallback, err)
			}
		})
	}
}

func assertDeliveredGeneration(t *testing.T, catalog *events.Catalog, appended events.Appended) {
	t.Helper()
	current, err := events.Decode[DecodeGeneration](catalog, appended)
	if err != nil || current.Value != "released" {
		t.Fatalf("current = %#v, %v", current, err)
	}
	pointer, err := appended.Decode(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := pointer.(*DecodeGeneration); !ok || value.Value != "released" {
		t.Fatalf("delivered = %#v", pointer)
	}
}
