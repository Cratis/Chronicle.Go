// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/chronicletest"
)

func TestFidelityCannotBeInferredFromAZeroValueOrMutatedReport(t *testing.T) {
	var empty chronicletest.Fidelity
	if !errors.Is(empty.Require(), chronicletest.ErrFidelityUnavailable) {
		t.Fatal("zero fidelity claimed full coverage")
	}
	s := chronicletest.NewEventScenario(t, chronicletest.Config{Registry: eventRegistry(t)})
	layers := s.Fidelity().Substitutions()
	for i := range layers {
		layers[i] = "invented"
	}
	for _, layer := range []chronicletest.Layer{chronicletest.Constraints, chronicletest.EventHashing, chronicletest.SchemaValidation, chronicletest.Authorization, "invented"} {
		if !errors.Is(s.Fidelity().Require(layer), chronicletest.ErrFidelityUnavailable) {
			t.Fatalf("claimed %s", layer)
		}
	}
}
