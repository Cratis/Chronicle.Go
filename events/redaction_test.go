// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

type redactionOriginal struct{ Value string }

func TestRedactionDecodesOriginalIdentityAndAudit(t *testing.T) {
	original, err := events.Define[redactionOriginal](events.WithID("original"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(original.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	appended := events.Appended{Context: events.Context{EventType: events.TypeRef{ID: events.RedactedTypeID, Generation: 2}}, Content: []byte(`{"reason":"approved","originalEventType":"original","occurred":"2026-01-02T03:04:05+02:00","correlationId":"00112233-4455-6677-8899-aabbccddeeff","causation":[{"type":"request","occurred":"2026-01-02T03:04:05Z"}],"causedBy":["ffeeddcc-bbaa-9988-7766-554433221100"]}`)}
	got, err := events.Decode[events.EventRedacted](catalog, appended)
	if err != nil || got.Reason != "approved" || got.OriginalType(catalog) != reflect.TypeFor[redactionOriginal]() || got.CorrelationID.String() != "00112233-4455-6677-8899-aabbccddeeff" || len(got.CausedBy) != 1 || got.CausedBy[0].String() != "ffeeddcc-bbaa-9988-7766-554433221100" || len(got.Causation) != 1 || got.Causation[0].Type != "request" {
		t.Fatalf("%+v %v", got, err)
	}
	decoded, err := appended.Decode(catalog)
	if err != nil || !reflect.DeepEqual(decoded, &got) {
		t.Fatalf("%+v %v", decoded, err)
	}
	got.OriginalEventType = "unknown"
	if got.OriginalType(catalog) != reflect.TypeFor[any]() || got.OriginalType(nil) != reflect.TypeFor[any]() {
		t.Fatal("unknown original must fall back to any")
	}
	if _, err = events.Decode[redactionOriginal](catalog, appended); !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatalf("marker decoded as original: %v", err)
	}
	// The marker can also be explicitly registered for observer handlers.
	if _, err = events.Define[events.EventRedacted](); err != nil {
		t.Fatal(err)
	}
}

func TestRedactionRejectsNonMarkersAndMalformedContent(t *testing.T) {
	for _, content := range []string{`null`, `{}`, `{"reason":"why"}`, `{"reason":"why","originalEventType":"known","causedBy":["bad-uuid"]}`, `not json`} {
		appended := events.Appended{Context: events.Context{EventType: events.TypeRef{ID: events.RedactedTypeID, Generation: 1}}, Content: []byte(content)}
		if _, err := appended.Redaction(); !errors.Is(err, chronicle.ErrProtocol) {
			t.Fatalf("%s: %v", content, err)
		}
	}
	if _, err := (events.Appended{Content: []byte(`{"reason":"why","originalEventType":"known"}`)}).Redaction(); !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatal(err)
	}
}
