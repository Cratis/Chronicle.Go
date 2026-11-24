// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"encoding/json"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
)

type MixedProtectionCurrent struct{ Value string }
type MixedProtectionPrevious MixedProtectionCurrent

func TestDecodeAlternateSelectsKernelContentWhateverTheIdentityProtection(t *testing.T) {
	for _, tc := range []struct {
		name           string
		classification compliance.Classification
	}{
		{"plain identity", compliance.Classification{}},
		{"PII sibling", compliance.Classification{PII: true}},
		{"encrypted sibling", compliance.Classification{Encrypted: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, appended := mixedProtectionDecodeFixture(t, tc.classification)
			value, err := events.Decode[MixedProtectionPrevious](catalog, appended)
			if err != nil || value.Value != "alternate" {
				t.Fatalf("alternate = %#v, %v", value, err)
			}
		})
	}
}

func mixedProtectionDecodeFixture(t *testing.T, classification compliance.Classification) (*events.Catalog, events.Appended) {
	t.Helper()
	current, err := events.Define[MixedProtectionCurrent](events.WithGeneration(2), events.WithProtection(compliance.Property("Value", classification)))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[MixedProtectionPrevious](current, 1)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(previous.Descriptor(), current.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return catalog, events.Appended{
		Context:             events.Context{EventType: current.Ref()},
		Content:             json.RawMessage(`{"Value":"current"}`),
		GenerationalContent: map[events.Generation]json.RawMessage{1: json.RawMessage(`{"Value":"alternate"}`)},
	}
}
