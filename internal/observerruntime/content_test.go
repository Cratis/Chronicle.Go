// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observerruntime"
)

type GenerationContent struct{ Value string }

// The kernel releases protected values in every generation, so protection does not
// change which representation an observer receives (Cratis/Chronicle#4623).
func TestObserverGenerationContentSelectsReleasedProtectedContent(t *testing.T) {
	for _, tc := range []struct {
		name           string
		classification compliance.Classification
		generation     events.Generation
		alternate      bool
		want           string
		wantGeneration events.Generation
		wantErr        error
	}{
		{"pii alternate", compliance.Classification{PII: true}, 2, true, "alternate-marker", 1, nil},
		{"encrypted alternate", compliance.Classification{Encrypted: true}, 2, true, "alternate-marker", 1, nil},
		{"protected fallback", compliance.Classification{PII: true}, 2, false, "released", 2, nil},
		{"pii current", compliance.Classification{PII: true}, 1, true, "released", 1, nil},
		{"encrypted current", compliance.Classification{Encrypted: true}, 1, true, "released", 1, nil},
		{"plain alternate", compliance.Classification{}, 2, true, "alternate-marker", 1, nil},
		{"plain fallback", compliance.Classification{}, 2, false, "released", 2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event, err := events.Define[GenerationContent](events.WithProtection(compliance.Property("Value", tc.classification)))
			if err != nil {
				t.Fatal(err)
			}
			ec := events.Context{EventType: events.TypeRef{ID: event.Ref().ID, Generation: tc.generation}}
			var generations map[events.Generation]json.RawMessage
			if tc.alternate {
				generations = map[events.Generation]json.RawMessage{1: json.RawMessage(`{"Value":"alternate-marker"}`)}
			}
			catalog := observerContentCatalog(t, event.Descriptor())
			actualContext, value, err := observerruntime.DecodeContent(catalog, event.Descriptor(), events.Appended{Context: ec, Content: json.RawMessage(`{"Value":"released"}`), GenerationalContent: generations})
			if !errors.Is(err, tc.wantErr) || actualContext.EventType.Generation != tc.wantGeneration {
				t.Fatalf("context = %#v, error = %v", actualContext, err)
			}
			assertObserverContentValue(t, value, err, tc.want)
		})
	}
}

func observerContentCatalog(t *testing.T, descriptors ...events.Descriptor) *events.Catalog {
	t.Helper()
	catalog, err := events.NewCatalog(descriptors...)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func assertObserverContentValue(t *testing.T, value any, err error, want string) {
	t.Helper()
	if err != nil {
		if value != nil || strings.Contains(err.Error(), "alternate-marker") {
			t.Fatalf("refused content = %#v, %v", value, err)
		}
		return
	}
	if decoded, ok := value.(*GenerationContent); !ok || decoded.Value != want {
		t.Fatalf("content = %#v", value)
	}
}
