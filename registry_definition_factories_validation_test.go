// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
)

func TestConstraintFactoryChecksEachOutputIdentityBeforeItsEventsAndCount(t *testing.T) {
	for _, tc := range []struct {
		name       string
		outputName string
		want       string
	}{
		{"foreign event before missing output", "first", "constraint references unregistered event"},
		{"identity before foreign event", "unexpected", "constraint output identity or callback lifetime mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newFactoryDeclarations(t)
			foreign, err := events.Define[factoryCurrent](events.WithID("foreign"))
			if err != nil {
				t.Fatal(err)
			}
			definition, err := constraints.UniqueValues(tc.outputName).On(foreign.Descriptor(), "Name").Build()
			if err != nil {
				t.Fatal(err)
			}
			err = chronicle.RegisterConstraintFactory(d.registry, []string{"first", "second"}, nil, func(context.Context, defaultDefinition) ([]constraints.Definition, error) {
				return []constraints.Definition{definition}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			client, err := chronicle.NewClient(chronicle.WithRegistry(d.registry))
			var preparation *chronicle.PreparationError
			if client != nil || !errors.As(err, &preparation) {
				t.Fatalf("client=%v error=%v", client, err)
			}
			want := "chronicle: invalid configuration: " + tc.want
			if cause := preparation.Unwrap(); cause == nil || cause.Error() != want {
				t.Fatalf("cause=%v, want %q", cause, want)
			}
		})
	}
}
