// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints_test

import (
	"testing"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
)

func TestBuildReportsFirstValidationFailure(t *testing.T) {
	contact := descriptor[Contact](t)
	for _, tc := range []struct {
		name    string
		builder *constraints.Builder
		want    string
	}{
		{"recorded error before header", constraints.UniqueEventTypes().IgnoreCasing(), "IgnoreCasing requires a property constraint"},
		{"header before descriptor", constraints.UniqueValues("").On(events.Descriptor{}), "constraint kind, name and event types are required"},
		{"remover before properties", constraints.UniqueValues("email").On(contact).RemovedWith(events.Descriptor{}), "empty constraint event descriptor"},
		{"property declaration order", constraints.UniqueValues("email").On(contact, "missing", "other"), `property "missing" does not exist on event Contact`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.builder.Build()
			if err == nil {
				t.Fatal("expected validation failure")
			}
			if want := "chronicle: invalid configuration: " + tc.want; err.Error() != want {
				t.Fatalf("error = %q, want %q", err, want)
			}
		})
	}
}
