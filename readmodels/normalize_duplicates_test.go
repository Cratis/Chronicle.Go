// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

func TestNormalizeIDDoesNotCollapseDuplicateMembers(t *testing.T) {
	type document struct{ ID string }
	model, err := Define[document]()
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{
		`{"_id":"source","field":1,"field":2}`,
		`{"_id":"source","unknown":[{"field":1,"field":2}]}`,
		`{"Id":"source","field":1,"field":2}`,
	} {
		data, err := normalizeID([]byte(input), model.Descriptor())
		if !errors.Is(err, faults.ErrProtocol) || data != nil {
			t.Fatalf("normalization discarded duplicate evidence: %v", err)
		}
	}
}
