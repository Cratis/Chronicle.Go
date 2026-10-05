// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"errors"
	"slices"
	"testing"
)

func TestProjectionScenarioFidelityRefusesUnusedDeliveryAndEffects(t *testing.T) {
	// Fidelity depends on the selected producer, not the availability of a server.
	s := &ReadModelScenario[struct{}]{}
	fidelity := s.Fidelity()
	for _, layer := range []Layer{DeliveryMetadata, EffectAcceptance, ObserverLifecycle, ReadModelStorage, DurableStorage} {
		if !slices.Contains(fidelity.Substitutions(), layer) || !errors.Is(fidelity.Require(layer), ErrFidelityUnavailable) {
			t.Fatalf("projection replay claimed %s", layer)
		}
	}
	if err := fidelity.Require(ProjectionExecution, Constraints); err != nil {
		t.Fatalf("kernel replay boundaries lost: %v", err)
	}
}
