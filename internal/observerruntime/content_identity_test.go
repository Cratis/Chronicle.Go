// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/observerruntime"
)

type SiblingGenerationContent struct{ Value string }
type PreviousSiblingGenerationContent SiblingGenerationContent

func TestObserverAlternateSelectsKernelContentWhateverTheIdentityProtection(t *testing.T) {
	for _, tc := range []struct {
		name           string
		classification compliance.Classification
		currentTarget  bool
	}{
		{"plain identity", compliance.Classification{}, false},
		{"PII sibling", compliance.Classification{PII: true}, false},
		{"encrypted sibling", compliance.Classification{Encrypted: true}, false},
		{"current target protected past", compliance.Classification{PII: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, descriptor, appended := identityObserverFixture(t, tc.classification, tc.currentTarget)
			_, value, err := observerruntime.DecodeContent(catalog, descriptor, appended)
			if err != nil {
				t.Fatalf("decode = %v", err)
			}
			var got string
			switch decoded := value.(type) {
			case *PreviousSiblingGenerationContent:
				got = decoded.Value
			case *SiblingGenerationContent:
				got = decoded.Value
			}
			if got != "alternate" {
				t.Fatalf("alternate = %#v", value)
			}
		})
	}
}

func TestObserverGenerationMismatchRequiresCatalog(t *testing.T) {
	_, descriptor, appended := identityObserverFixture(t, compliance.Classification{}, false)
	_, value, err := observerruntime.DecodeContent(nil, descriptor, appended)
	if !errors.Is(err, faults.ErrInvalidConfiguration) || value != nil {
		t.Fatalf("missing catalog = %#v, %v", value, err)
	}
}

func identityObserverFixture(t *testing.T, classification compliance.Classification, currentTarget bool) (*events.Catalog, events.Descriptor, events.Appended) {
	t.Helper()
	currentProtection, previousProtection := classification, compliance.Classification{}
	if currentTarget {
		currentProtection, previousProtection = previousProtection, currentProtection
	}
	current, err := events.Define[SiblingGenerationContent](events.WithGeneration(2), events.WithProtection(compliance.Property("Value", currentProtection)))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[PreviousSiblingGenerationContent](current, 1, events.WithProtection(compliance.Property("Value", previousProtection)))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, delivered := previous.Descriptor(), current.Ref()
	if currentTarget {
		descriptor, delivered = current.Descriptor(), previous.Ref()
	}
	return observerContentCatalog(t, previous.Descriptor(), current.Descriptor()), descriptor, events.Appended{
		Context:             events.Context{EventType: delivered},
		Content:             json.RawMessage(`{"Value":"released"}`),
		GenerationalContent: map[events.Generation]json.RawMessage{descriptor.Ref().Generation: json.RawMessage(`{"Value":"alternate"}`)},
	}
}
