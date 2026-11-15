// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

type descriptorEvent struct{ Value string }
type descriptorPrevious struct{ Value string }
type descriptorOther struct{ Value string }

func TestEventConfigurationValidationOrder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []TypeOption
		message string
	}{
		{"nil option first", []TypeOption{WithSourceStore(" "), nil}, "nil event option"},
		{"source before identity", []TypeOption{WithSourceStore(" "), WithID("")}, "source store must be nonblank"},
		{"blank identity", []TypeOption{WithID(" ")}, "nonblank comma-free type ID and positive generation required"},
		{"comma identity", []TypeOption{WithID("a,b")}, "nonblank comma-free type ID and positive generation required"},
		{"zero generation", []TypeOption{WithGeneration(0)}, "nonblank comma-free type ID and positive generation required"},
		{"nil resolver", []TypeOption{WithSubjectResolver[descriptorEvent](nil)}, "subject resolver must be non-nil and match the declared event type"},
		{"wrong resolver", []TypeOption{WithSubjectResolver(func(descriptorOther) (Subject, bool) { return "", false })}, "subject resolver must be non-nil and match the declared event type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Define[descriptorEvent](tc.options...)
			if !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("error = %v", err)
			}
			if want := faults.ErrInvalidConfiguration.Error() + ": " + tc.message; err.Error() != want {
				t.Fatalf("error = %q, want %q", err, want)
			}
		})
	}
}

func TestEventCatalogDuplicateAndHistoricalValidation(t *testing.T) {
	current, err := Define[descriptorEvent](WithID("event"), WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := DefineGeneration[descriptorPrevious](current, 1)
	if err != nil {
		t.Fatal(err)
	}
	other, err := Define[descriptorOther](WithID("event"), WithGeneration(3))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		descriptors []Descriptor
		message     string
	}{
		{"empty", []Descriptor{{}}, "empty event descriptor"},
		{"duplicate type", []Descriptor{current.Descriptor(), current.Descriptor()}, "duplicate event type event"},
		{"duplicate current ID", []Descriptor{current.Descriptor(), other.Descriptor()}, "duplicate event type event"},
		{"missing current", []Descriptor{previous.Descriptor()}, "historical generation requires its current event"},
		{"different current", []Descriptor{previous.Descriptor(), other.Descriptor()}, "historical generation requires its current event"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewCatalog(tc.descriptors...)
			if !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("error = %v", err)
			}
			if want := faults.ErrInvalidConfiguration.Error() + ": " + tc.message; err.Error() != want {
				t.Fatalf("error = %q, want %q", err, want)
			}
		})
	}
	for _, descriptors := range [][]Descriptor{
		{current.Descriptor(), previous.Descriptor()},
		{previous.Descriptor(), current.Descriptor()},
	} {
		catalog, err := NewCatalog(descriptors...)
		if err != nil {
			t.Fatal(err)
		}
		if got := catalog.Descriptors(); !got[0].SameDeclaration(descriptors[0]) || !got[1].SameDeclaration(descriptors[1]) {
			t.Fatal("catalog changed declaration order")
		}
	}
}
