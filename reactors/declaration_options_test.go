// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

func TestDeclarationOptionValidationOrder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []Option
		want    string
	}{
		{"nil before invalid ID", []Option{WithID(" "), nil}, "nil reactor option"},
		{"invalid ID before blank tag", []Option{WithID(" "), WithTags(" ")}, "invalid reactor options"},
		{"blank sequence", []Option{WithEventSequence(" ")}, "invalid reactor options"},
		{"blank stream type", []Option{func(c *configuration) { c.streamType = " " }}, "invalid reactor options"},
		{"blank source type", []Option{func(c *configuration) { c.sourceType = " " }}, "invalid reactor options"},
		{"source store and sequence", []Option{WithSourceStore("store"), WithEventSequence("log")}, "invalid reactor options"},
		{"invalid option", []Option{WithLogger(nil)}, "invalid reactor options"},
		{"blank tag", []Option{WithTags(" ")}, "blank reactor tag or filter"},
		{"blank filter", []Option{WithEventTagFilter(" ")}, "blank reactor tag or filter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DefineHandlers("reactor", nil, tc.options...)
			if !errors.Is(err, faults.ErrInvalidConfiguration) || err.Error() != "chronicle: invalid configuration: "+tc.want {
				t.Fatalf("error = %v, want %s", err, tc.want)
			}
		})
	}
}

func TestDeclarationAllowsEmptyOptionalSourceTypeAndLastID(t *testing.T) {
	declaration, err := DefineHandlers("initial", nil, WithID(" "), WithID("final"))
	if err != nil {
		t.Fatal(err)
	}
	if declaration.Identifier() != "final" || declaration.GoType() != nil || !declaration.explicit {
		t.Fatal("explicit declaration metadata changed", declaration)
	}
	structDeclaration, err := Define[metadataEvent](nil)
	if err != nil {
		t.Fatal(err)
	}
	if structDeclaration.GoType() == nil || structDeclaration.explicit {
		t.Fatal("struct declaration metadata changed", structDeclaration)
	}
}
