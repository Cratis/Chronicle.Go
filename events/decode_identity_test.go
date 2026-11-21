// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

type MixedProtectionCurrent struct{ Value string }
type MixedProtectionPrevious MixedProtectionCurrent

func TestDecodeAlternateUsesIdentityWideProtection(t *testing.T) {
	for _, tc := range []struct {
		name           string
		classification compliance.Classification
		wantErr        error
	}{
		{"plain identity", compliance.Classification{}, nil},
		{"PII sibling", compliance.Classification{PII: true}, faults.ErrUnsupported},
		{"encrypted sibling", compliance.Classification{Encrypted: true}, faults.ErrUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, appended := mixedProtectionDecodeFixture(t, tc.classification)
			value, err := events.Decode[MixedProtectionPrevious](catalog, appended)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("decode = %v", err)
			}
			if tc.wantErr != nil {
				if value != (MixedProtectionPrevious{}) {
					t.Fatalf("refused value = %#v", value)
				}
				return
			}
			if value.Value != "alternate" {
				t.Fatalf("plain alternate = %#v", value)
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
