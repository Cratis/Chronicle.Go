// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/enumfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
)

func TestEnumUndeclaredValuesIssueNoAppendRPC(t *testing.T) {
	codecs, err := enumfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[enumfixtures.Scalar[enumfixtures.AllBits]](events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	sequence, calls := parityFixture(t, nil, catalog, eventsequences.ConcurrencyPolicy{})
	for _, value := range []enumfixtures.AllBits{5, 7, 8, 9, -2} {
		if _, err := sequence.Append(t.Context(), "source", enumfixtures.Scalar[enumfixtures.AllBits]{Value: value}); !errors.Is(err, faults.ErrUnsupported) {
			t.Fatalf("unknown enum: %v", err)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("undeclared enum dispatched %d RPCs", got)
	}
}
