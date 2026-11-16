// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
)

type uniqueValidationEvent struct{}

func TestValidateUniqueReportsFirstValidationFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []events.TypeOption
		want    string
	}{
		{"count before name", []events.TypeOption{events.WithUnique(events.Unique{Name: " "}), events.WithUnique(events.Unique{})}, "only one type-level unique declaration is allowed"},
		{"name before sequence", []events.TypeOption{events.WithUnique(events.Unique{Name: " ", EventSequences: []events.SequenceID{" "}})}, "constraint name must be nonblank"},
		{"blank sequence", []events.TypeOption{events.WithUnique(events.Unique{EventSequences: []events.SequenceID{"log", " "}})}, "event sequence must be nonblank"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event, err := events.Define[uniqueValidationEvent](tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			err = event.Descriptor().ValidateUnique()
			var declaration *events.DeclarationError
			if !errors.As(err, &declaration) {
				t.Fatalf("error = %v, want DeclarationError", err)
			}
			if declaration.Message != tc.want {
				t.Fatalf("message = %q, want %q", declaration.Message, tc.want)
			}
		})
	}
}
